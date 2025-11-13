from math import ceil
from typing import Any, Optional

import pandas as pd
from loguru import logger
from snowflake import connector  # type: ignore
from snowflake.connector.errors import DatabaseError, ForbiddenError  # type: ignore

from app.core.config import settings
from app.schemas import (
    DataChunk,
    FileStatus,
    ObjectContents,
    SnowflakeConfig,
    SnowFlakeInputData,
    SnowflakeUser,
    SupportedServices,
)
from app.send_request import APIEndpoints, HTTPMethods, send_request
from app.services.base_scan_service import BaseScanService


class SnowflakeConnectionError(Exception):
    """
    Raises if was detected DatabaseError
    """

    pass


class SnowflakeService(BaseScanService):
    mapper_name = SupportedServices.SNOWFLAKE

    def __init__(
        self, source: SnowFlakeInputData | str, credentials: Optional[SnowflakeUser] = None, *args: Any, **kwargs: Any
    ):
        """
        Initializes the SnowflakeService data handler with specified credentials.
        This service is responsible for handling Snowflake-specific tasks, leveraging the
        functionalities provided by BaseScanService and extending them for Snowflake's use cases.

        Args:
            credentials: Configuration data for Snowflake authentication(SnowflakeUser). Default is None.
            args: Variable length argument list to be passed to the superclass.
            kwargs: Arbitrary keyword arguments to be passed to the superclass.

        The method initializes the instance with the given credentials, along with any additional arguments.
        """
        super().__init__(source=source, credentials=credentials, *args, **kwargs)

    async def __aenter__(self) -> 'SnowflakeService':
        """
        If the service's credentials are not already set, it fetches them using a
        GET request to a specified API endpoint using aiohttp. It then establishes a session
        with Snowflake using these credentials.

        This method is typically invoked when the SnowflakeService is used within
        an `async with` block, ensuring proper initialization and resource management.

        Returns:
            An instance of SnowflakeService with an established session.
        """
        if not self.credentials:
            self.credentials = await send_request(
                method=HTTPMethods.GET,
                url=APIEndpoints.CLOUD_ACCOUNT.url,
                response_model=SnowflakeConfig,
                account_id=self.account_id,
            )
        self.session = await self._get_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
        """
        Closes the Snowflake session when exiting the context. This method is
        automatically called at the end of an `async with` block.

        Args:
            exc_type: The exception type, if any, that caused the context to be exited.
            exc_val: The exception value, if any, that caused the context to be exited.
            exc_tb: The traceback, if any, for the exception that caused the context to be exited.

        This method ensures that resources are properly released when the context
        is exited, either after normal completion or in case of an exception.
        """
        self.session.close()

    async def create_object_chunks(self, fetch_path: str, size: Optional[int] = 0) -> list[DataChunk]:
        """
        Creation data chunks for Snowflake table based on table size. If size is None or 0 chunks would not appear.

        Args:
            fetch_path: path for retrieving table information
            size: size of current table

        Returns:
            data_chunks: list[DataChunk] or []
        """
        if not size:
            return []
        data_chunks: list[DataChunk] = []
        with self.session.cursor() as cs:
            total_rows = cs.execute(f"SELECT COUNT(*) FROM {fetch_path};").fetchone()[0]
        for i in range(ceil(total_rows / settings.CHUNK_ROWS_CAPACITY)):
            data_chunks.append(
                DataChunk(  # type: ignore
                    object_name=fetch_path.rsplit('.')[-1],
                    fetch_path=fetch_path,
                    offset=str(i * settings.CHUNK_ROWS_CAPACITY),
                    limit=settings.CHUNK_ROWS_CAPACITY,
                    instance_id=settings.SCANNER_ID,
                )
            )

        return data_chunks

    async def _get_session(self) -> connector.SnowflakeConnection:
        """
        Get Snowflake session using credentials.

        Attempts to connect to Snowflake using either an encrypted private key or an
        encrypted password, based on the available credentials. It handles exceptions
        related to database connectivity and access issues, logging appropriate messages.

        Returns:
            A new instance of SnowflakeConnection.

        Raises:
            SnowflakeConnectionError: If there is an issue connecting to Snowflake.
            ForbiddenError: If access to Snowflake is forbidden.
        """
        try:
            if self.credentials.encrypted_private_key:
                return connector.connect(
                    account=self.credentials.account,
                    user=self.credentials.login,
                    private_key=self.credentials.encrypted_private_key,
                )
            elif self.credentials.encrypted_password:
                return connector.connect(
                    account=self.credentials.account,
                    user=self.credentials.login,
                    password=self.credentials.encrypted_password,
                )
        except DatabaseError as e:
            logger.warning(
                f'Unable connect to Snowflake. '
                f'account: {self.credentials.account}, '
                f'user: {self.credentials.login}\n'
                f'ERROR: {e}'
            )
            raise SnowflakeConnectionError
        except ForbiddenError as e:
            logger.error(e)
        else:
            raise ValueError(f'Unable to connect to snowflake account: {self.credentials.account}')

    async def get_database_additional_info(self) -> dict[str, Any]:
        """
        Retrieves additional information about a specific database in Snowflake.

        Executes a Snowflake query to fetch metadata about the database and returns
        specific details like the database owner and creation date. If the query fails
        or the database information is not found, returns a dictionary with empty values.

        Returns:
            A dictionary containing 'database_owner' and 'db_creation_date'. If an error
            occurs or data is unavailable, these fields will be empty.
        """
        try:
            with self.session.cursor() as cs:
                metadata = cs.execute(f"SHOW DATABASES LIKE '{self.source}'").fetchone()
                return {
                    "database_owner": metadata[5] if metadata else '',
                    "db_creation_date": metadata[0] if metadata else '',
                }
        except Exception as e:
            logger.error(f"Error retrieving database information: {e}")
            return {"database_owner": "", "db_creation_date": ""}

    async def get_objects_by_source(self) -> list[ObjectContents]:  # todo: by db_name
        """
        Retrieves a list of ObjectContents for each table in the specified Snowflake database.

        This method executes a query to gather information about all tables in the given
        database. It fetches the region, additional database info, and constructs
        ObjectContents instances for each table, which include details like table name,
        size, owner, creation date, and other metadata.

        Returns:
            A list of ObjectContents instances, each representing metadata for a table
            in the database.
        """
        results: list[ObjectContents] = []
        with self.session.cursor() as cs:
            cs.execute(f'SHOW TABLES IN DATABASE {str(self.source)}')
            db_response = cs.fetchall()
            cs.execute(f"USE DATABASE {str(self.source)};")

            for cur_db in db_response:
                table_last_modified_date = cs.execute(
                    f"""
                     SELECT * FROM INFORMATION_SCHEMA.TABLES
                     WHERE TABLE_SCHEMA='{cur_db[3]}' AND TABLE_NAME='{cur_db[1]}';
                     """
                ).fetchone()[19]
                content = ObjectContents(
                    service=self.mapper_name,
                    source=self.source.source_name,
                    full_path=f'"{cur_db[2]}"/"{cur_db[3]}"/"{cur_db[1]}"',  # TODO: unify
                    fetch_path=f'"{cur_db[2]}"."{cur_db[3]}"."{cur_db[1]}"',
                    object_name=cur_db[1],
                    etag=f'{cur_db[1]}_{cur_db[8]}',
                    size=cur_db[8],
                    owner=cur_db[9],
                    object_creation_date=cur_db[0],
                    resource_id=self.source.source_name,
                    source_region=self.source.source_region,
                    source_owner=self.source.source_owner,
                    last_modified=table_last_modified_date,
                    source_UUID=self.source.source_UUID,
                    data_chunks=await self.create_object_chunks(
                        fetch_path=f'"{cur_db[2]}"."{cur_db[3]}"."{cur_db[1]}"', size=cur_db[8]
                    ),
                )
                if not content.data_chunks:
                    content.status = FileStatus.SCANNED
                results.append(content)
        return results

    async def exclude_redundant_objects(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """
        Not implemented for Snowflake. Method to exclude specific table from retrieved by source.
        Args:
            objects: list of ObjectContents which contains metadata for each table

        Returns:
            all table that were detected for scanning
        """
        return objects  # todo: add exclusion built-in objects

    async def fetch_data(
        self,
        fetch_path: str,
        chunk_path: str,
        limit: int,
        offset: int,
    ) -> Optional[pd.DataFrame]:
        """
        Fetching data from table object by fetch path, limit and offset.
         If no limit and offset, we retrieve data for all rows from table.

        Args:
            fetch_path: path for scanning table in redshift database
            chunk_path: path to scanning chunk
            limit: represents how many rows must be retrieved
            offset: represents start position to retrieve chunk

        Returns:
            dataframe object with data from table by fetch path, limit and offset.
        """
        try:
            query = f'select * from {fetch_path} LIMIT {limit} OFFSET {offset};'
            cursor_result = self.session.cursor().execute(query)
            if not cursor_result:
                raise ValueError('The result cursor is invalid')
            return pd.DataFrame(dtype=str).from_records(
                iter(cursor_result), columns=[current_cur[0] for current_cur in cursor_result.description]
            )
        except Exception as e:
            logger.error(f'Unable to fetch data. Exception: {e}')
            return None

    async def get_source_configuration(self, *args, **kwargs) -> Any:  # type: ignore
        ...
