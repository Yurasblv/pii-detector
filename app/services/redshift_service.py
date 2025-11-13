import asyncio
import datetime
from math import ceil
from typing import Any, Optional

import pandas as pd
from loguru import logger

from app.core.config import settings
from app.schemas import DataChunk, FileStatus, ObjectContents, RedshiftInputData, RedshiftResult, SupportedServices
from app.services.aws_base_service import AwsBaseService, boto3_client


class RedshiftService(AwsBaseService):
    mapper_name = SupportedServices.REDSHIFT

    def __init__(self, source: RedshiftInputData | str, *args, **kwargs):  # type: ignore
        """
        Initializes a new instance of RedshiftService

        This service is responsible for handling Redshift-specific tasks, leveraging the
        functionalities provided by AwsBaseChunkService and extending them for Redshift's use cases.

        Args:
            source: An instance of RedshiftInputData containing details about the Redshift cluster,
                    or a string identifier of the source.
            args: Variable length argument list to be passed to the superclass.
            kwargs: Arbitrary keyword arguments to be passed to the superclass.
        """
        super().__init__(source=source, *args, **kwargs)  # type: ignore

    @boto3_client('redshift-data')
    async def _make_sql_statement(self, sql: str, service_client: Any, db_name: Optional[str] = None) -> dict[str, Any]:
        """
        Executes an SQL statement on an Amazon Redshift database using the boto3 client.
        It first checks if the source attribute is a string and retrieves sources based on the cluster name in source.
        If no sources are found, it returns 'FAILED'. Otherwise, it executes the SQL using the specified cluster,
           database, and user credentials.
        The method continuously checks the execution status and returns the final status and details upon completion.

        Args:
            sql: sql query to call
            service_client: boto3 client
            db_name: name of the database to execute the statement on. If None,
            defaults to the database name specified in the `source` attribute.

        Returns:
            dictionary containing the status and details of the executed SQL statement.

        """
        if isinstance(self.source, str):
            await self.get_source_configuration(cluster_name=self.source)
        statement = await service_client.execute_statement(
            ClusterIdentifier=self.source.cluster,
            Database=db_name or self.source.db_name,
            DbUser=self.source.db_user,
            Sql=sql,
        )
        statement_desc = await service_client.describe_statement(Id=statement['Id'])
        while statement_desc['Status'] in ['PICKED', 'SUBMITTED', 'STARTED']:
            await asyncio.sleep(0.1)
            statement_desc = await service_client.describe_statement(Id=statement['Id'])
        return statement_desc  # type: ignore

    async def create_object_chunks(self, fetch_path: str, db_name: str, size: int = 0) -> list[DataChunk]:
        """
        Creation data chunks for Redshift table.

        Args:
            fetch_path: path for retrieving table information
            db_name: name of selected database
            size: size of current table

        Returns:
            data_chunks: list[DataChunk] or []
        """
        if not size:
            return []
        data_chunks: list[DataChunk] = []
        stmt_result = await self._get_statement_result(
            await self._make_sql_statement(sql=f"SELECT COUNT(*) FROM {fetch_path};", db_name=db_name)
        )
        total_rows = int(stmt_result.records[0])
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

    @staticmethod
    def _get_records_columns(statement_result_column_metadata: list[dict[str, Any]]) -> list[str]:
        """
        Processes statement_results['ColumnMetadata'] from query result to extract and return the names of the columns.

        Args:
            statement_result_column_metadata: ColumnMetadata with information about table from query result

        Returns:
            list of column names extracted from statement_results['ColumnMetadata']).
        """
        return [col['name'] for col in statement_result_column_metadata]

    @staticmethod
    def _get_records_values(statement_result_records: list[list[dict[str, Any]]]) -> list[Any]:
        """
        Processes statement_results['Records'] from query result to extract and return the values of the columns.

        Args:
            statement_result_records: ColumnMetadata with information about values from query result

        Returns:
            list of values extracted from the statement_results['Records'].
        """
        return [value for row in statement_result_records for d in row for value in d.values()]

    @boto3_client('redshift-data')  # type: ignore
    async def _get_statement_result(self, statement: dict[str, Any], service_client) -> RedshiftResult:
        """
        Fetches the result executed Redshift statement.
        This method checks if the statement execution was successful. If failed, it logs an error and returns an
        empty RedshiftResult object. Otherwise, it retrieves the full result of the statement and processes it to
        extract column metadata and record values, returning them in a RedshiftResult object

        Args:
            statement: A dictionary containing details of the executed Redshift statement.
            service_client: boto3 client

        Returns:
            RedshiftResult: An object representing the result of the Redshift statement. It contains columns and
            records extracted from the statement result.
        """
        if statement['Status'] == 'FAILED':
            logger.error(f'Unable to get redshift statement {statement.get("Error")}')
            return RedshiftResult()
        statement_results = await service_client.get_statement_result(Id=statement['Id'])
        return RedshiftResult(
            columns=self._get_records_columns(statement_results['ColumnMetadata']),
            records=self._get_records_values(statement_results['Records']),
        )

    @boto3_client('redshift')  # type: ignore
    async def get_source_configuration(self, service_client, cluster_name: str) -> None:
        """
        Set source as schema by configuring input by cluster name from boto3.

        Args:
            service_client: boto3 client
            cluster_name: collect meta info about cluster

        Returns:
            None
        """
        response = await service_client.describe_clusters(ClusterIdentifier=cluster_name)
        for cluster in response.get('Clusters', []):
            self.source = RedshiftInputData(
                cluster=cluster.get('ClusterIdentifier'),
                db_user=cluster.get('MasterUsername'),
                db_name=cluster.get('DBName'),
            )
        return None

    @boto3_client('redshift-data')  # type: ignore
    async def get_objects_by_source(self, service_client) -> list[ObjectContents]:
        """
        Get all databases from Redshift cluster of AWS account.

        Args:
            service_client: boto3 client
            cluster_name: optional parameter for get all database for specific cluster

        Returns:
            list of RedshiftInputData with information about cluster id , master username, database name and cluster
             creation date
        """
        database_response = await service_client.list_databases(
            ClusterIdentifier=self.source.cluster, Database=self.source.db_name, DbUser=self.source.db_user
        )
        object_lists = [
            await self.get_list_of_objects_by_db(db_name=db_name)
            for db_name in database_response.get('Databases', [])
            if db_name != 'awsdatacatalog'
        ]
        return sum(object_lists, [])  # type: ignore

    async def get_list_of_objects_by_db(self, db_name: str) -> Optional[list[ObjectContents]]:
        """
        Retrieves a list of ObjectContents for database.

        This function executes a SQL statement to fetch details about tables in a given database,
        excluding certain system schemas. It then constructs and returns a list of `ObjectContents` objects,
        each representing a table in the database.

        Args:
            db_name : name of the database

        Returns:
            list of `ObjectContents` objects, each containing details
            about a table in the specified database. Returns an empty list if no tables are found or if
            the query fails.

        """
        results: list[ObjectContents] = []
        statement_result = await self._get_statement_result(
            statement=await self._make_sql_statement(
                sql='''
                        SELECT
                            CAST(d.datname AS text) AS database_name,
                            CAST(n.nspname AS text) AS schema_name,
                            CAST(c.relname AS text) AS table_name,
                            CAST(u.usename AS text) AS table_owner,
                            TO_CHAR(ci.relcreationtime, 'YYYY-MM-DD HH24:MI:SS') AS creation_date,
                            CAST(COALESCE(tinfo.size, 0) AS bigint) * 1024 * 1024 AS table_size_in_bytes
                        FROM
                            pg_class c 
                        JOIN
                            pg_namespace n ON c.relnamespace = n.oid 
                        JOIN
                            pg_user u ON c.relowner = u.usesysid 
                        JOIN
                            pg_class_info ci ON ci.relname = c.relname 
                        JOIN
                            pg_stat_database d ON d.datname = current_database() 
                        LEFT JOIN
                            SVV_TABLE_INFO tinfo ON tinfo.schema = n.nspname AND tinfo.table = c.relname
                        WHERE 
                            n.nspname 
                            NOT IN ('pg_catalog', 'pg_toast', 'information_schema', 'pg_internal', 'pg_automv')
                        GROUP BY
                            c.oid, n.nspname, c.relname, u.usename, d.datname, tinfo.size, ci.relcreationtime;
                ''',
                db_name=db_name,
            )
        )
        if not statement_result or not statement_result.records:
            return []
        split_records = (statement_result.records[x : x + 6] for x in range(0, len(statement_result.records), 6))
        for db in split_records:
            content = ObjectContents(
                service=self.mapper_name,
                full_path=f'{self.source.cluster}/{db[0]}/{db[1]}/{db[2]}',
                fetch_path=f'"{db[0]}"."{db[1]}"."{db[2]}"',
                object_name=f'{db[1]}/{db[2]}',
                etag=f'{db[1]}{db[2]}{db[5]}',
                size=0 if db[5] == 'True' else int(db[5]),
                source=self.source.cluster,
                resource_id=self.source.cluster,
                owner=db[3],
                source_owner=self.source.owner,
                source_region=self.source.region,
                source_UUID=self.source.source_UUID,
                object_creation_date=None
                if db[4] == 'True'
                else datetime.datetime.strptime(db[4], '%Y-%m-%d %H:%M:%S'),
                last_modified=None if db[4] == 'True' else datetime.datetime.strptime(db[4], '%Y-%m-%d %H:%M:%S'),
                data_chunks=await self.create_object_chunks(
                    fetch_path=f'"{db[0]}"."{db[1]}"."{db[2]}"',
                    size=0 if db[5] == 'True' else int(db[5]),
                    db_name=db_name,
                ),
            )
            if not content.data_chunks:
                content.status = FileStatus.SCANNED
            results.append(content)
        return results

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
        sql = f'select * from {fetch_path} LIMIT {limit} OFFSET {offset};'
        result: RedshiftResult = await self._get_statement_result(statement=await self._make_sql_statement(sql=sql))
        if not result.records:
            return None
        logger.success(f'Extracted {len(result.records)} records')
        if len(result.records) > len(result.columns):
            # records and columns has different length, we cut list of records by len columns
            return pd.DataFrame(
                [
                    result.records[x : x + len(result.columns)]
                    for x in range(0, len(result.records), len(result.columns))
                ],
                columns=result.columns,
            )
        elif len(result.records) == len(result.columns):
            return pd.DataFrame([result.records], columns=result.columns)
        else:
            return None

    async def exclude_redundant_objects(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """
        Not implemented for Redshift. Method to exclude specific object from retrieved by source.
        Args:
            objects: list of ObjectContents which contains metadata for each object

        Returns:
            all objects that were detected for scanning
        """
        return objects  # todo: add exclusion built-in objects
