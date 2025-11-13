import os
from contextlib import contextmanager
from math import ceil
from typing import Any, Optional
from urllib import request

import pandas as pd
from loguru import logger
from sqlalchemy import create_engine
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import NullPool

from app.core.config import settings
from app.schemas import DataChunk, FileStatus, ObjectContents, SupportedServices
from app.schemas.rds import RDSInputData, RDSTablesResults
from app.services.aws_base_service import AwsBaseService, boto3_client
from app.services.utils import engine_default_db

CA_NAME = 'global-bundle.pem'  # TODO : create function for downloading global-bundle
CA_PATH = os.path.abspath(__file__ + f"/../../../../{CA_NAME}")


class RDSService(AwsBaseService):
    mapper_name = SupportedServices.RDS

    def __init__(self, source: RDSInputData | str, *args, **kwargs):  # type: ignore
        """
        Initializes a new instance of RDSService

        This service is responsible for handling RDS-specific tasks, leveraging the
        functionalities provided by AwsBaseService and extending them for RDSService's use cases.

        Args:
            source: An instance of RDSInputData containing details about the RDSService source,
                    or a string identifier of the source.
            args: Variable length argument list to be passed to the superclass.
            kwargs: Arbitrary keyword arguments to be passed to the superclass.
        """
        super().__init__(source=source, *args, **kwargs)  # type: ignore
        # ca certificate for mysql engine based databases connection
        self.ca = self.get_ssL_cert_path() if not os.path.exists(CA_PATH) else CA_PATH

    @contextmanager
    def get_session(self, connect_args: dict[str, Any]) -> Session:
        """
        Create inner sync session for RDS connection using sqlalchemy.Using context manager.

        Args:
            connect_args: contains connection arguments with  username, password

        Returns:
            session object
        """
        cluster_engine = create_engine(
            f'{self.source.engine}://{self.source.endpoint}:{self.source.port}/{self.source.name}',
            connect_args=connect_args,
            poolclass=NullPool,
            future=True,
            echo=False,
        )

        Session = sessionmaker(cluster_engine, expire_on_commit=False)
        session = Session()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()

    async def create_object_chunks(self, fetch_path: str, size: int = 0) -> list[DataChunk]:
        """
        Creation data chunks for RDS table.

        Args:
            fetch_path: path for retrieving table information
            size: size of current table

        Returns:
            data_chunks: list[DataChunk] or []
        """
        if not size:
            return []
        data_chunks: list[DataChunk] = []
        connect_args = await self.get_db_connect_args()
        with self.get_session(connect_args) as session:
            match self.source.engine:
                case 'mysql+pymysql':
                    path = f"`{fetch_path.split('.')[1]}`"
                case 'postgresql':
                    path = fetch_path
            total_rows = session.execute(f"SELECT COUNT(*) FROM {path};").fetchone()[0]
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

    @boto3_client('rds')  # type: ignore
    async def get_db_connect_args(self, service_client) -> dict[str, Any]:
        """
        Generating personal token and configuring extra data for db access

        Args:
            service_client: boto3 client

        Returns:
            rds_credentials: dict which contains connection arguments with  username, password
            and ca file for mysql based engine
        """
        token = await service_client.generate_db_auth_token(
            DBHostname=self.source.endpoint,
            Port=self.source.port,
            DBUsername=settings.RDS_DATABASE_USER,
            Region=self.source.region,
        )
        rds_credentials = {'user': settings.RDS_DATABASE_USER, 'password': token}
        match self.source.engine:
            case 'mysql+pymysql':
                rds_credentials.update({'ssl': {'ca': f'{self.ca}'}})
            case _:
                pass
        return rds_credentials

    @staticmethod
    def get_ssL_cert_path() -> str:
        """Download CA file for SSL connection to mysql based dbs"""
        try:
            ca_file = request.urlretrieve(
                'https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem', filename=CA_NAME
            )
        except Exception as e:
            logger.error(f'Unable to load ssL certificate. Details: {e}')
        return os.path.abspath(f"{ca_file[0]}")

    async def get_tables_metadata(self, query: str) -> RDSTablesResults:
        """
        Method for executing completed query to fetch data.

        Args:
            query: raw sql string

        Returns:
            RDSTablesResults with column names and records
        """
        query_results = RDSTablesResults()
        connect_args = await self.get_db_connect_args()
        try:
            with self.get_session(connect_args) as session:
                result = session.execute(query)
                query_results.records = result.fetchall()
                query_results.columns = list(result.keys())
        except Exception as e:
            logger.warning(f"Database connection failed due to {e}")
        return query_results

    async def get_granted_databases(self) -> list[str]:
        """
        Method for retrieving user granted stores for scanning:
            mysql+pymysql engine: list of schemas
            postgresql engine: list of databases
        """
        cluster_dbs_names: list[str] = []
        connect_args = await self.get_db_connect_args()
        try:
            with self.get_session(connect_args) as session:
                match self.source.engine:
                    case 'postgresql':
                        result = session.execute(
                            """
                            SELECT datname 
                            FROM pg_database 
                            WHERE datistemplate = false AND datname not in ('rdsadmin', 'readme_to_recover');
                            """
                        )
                    case 'mysql+pymysql':
                        result = session.execute(
                            """
                            SELECT SCHEMA_NAME AS 'Database'
                            FROM information_schema.SCHEMATA
                            WHERE SCHEMA_NAME NOT IN ('performance_schema', 'sys', 'information_schema', 'mysql');
                            """
                        )
                cluster_dbs_names.extend(result.scalars().all())
        except Exception as e:
            logger.warning(f"Database connection failed: {e}")
        return cluster_dbs_names

    @boto3_client('rds')  # type: ignore
    async def get_source_configuration(self, service_client, db_instance: str) -> None:
        """
        Set source as schema by configuring input by cluster name from boto3.

        Args:
            service_client: boto3 client
            db_instance: str - if db_instance in args it will return meta information across all databases.
            Aurora based databases will be in cluster, so connect must be to instance.

        Returns:
            None
        """
        cluster = await service_client.describe_db_instances(DBInstanceIdentifier=db_instance)
        for instance in cluster.get('DBInstances', []):
            if instance.get('DBInstanceIdentifier') == db_instance:
                if not instance.get('IAMDatabaseAuthenticationEnabled'):
                    logger.warning(f"IAM Authentication is disabled for {instance.get('Endpoint', {}).get('Address')}")
                    return None
                if instance['DBInstanceStatus'] != 'available':
                    logger.warning(f"Cluster is not working {instance.get('Endpoint', {}).get('Address')}")
                    return None
                self.source = RDSInputData(
                    instance=instance.get('DBInstanceIdentifier'),
                    master_name=instance.get('MasterUsername'),
                    endpoint=instance.get('Endpoint', {}).get('Address'),
                    port=instance.get('Endpoint', {}).get('Port'),
                    region=instance.get('AvailabilityZone', ''),
                    engine=instance.get('Engine'),
                    cluster=instance.get('DBClusterIdentifier'),
                )
        return None

    async def get_objects_by_source(self) -> list[ObjectContents]:
        """
        Get all tables by source based on specific engine.
        Source for postgresql based engine is database name and for mysql is schema name.
        """
        source_tables: list[ObjectContents] = []
        self.source.name = engine_default_db.get(self.source.engine)
        cluster_dbs = await self.get_granted_databases()
        if not cluster_dbs:
            return []
        for db in cluster_dbs:
            self.source.name = db
            match self.source.engine:
                case 'postgresql':
                    source_tables.extend(await self.get_postgres_tables(db))
                case 'mysql+pymysql':
                    source_tables.extend(await self.get_mysql_tables(db))
                case _:
                    continue
        return source_tables

    async def get_postgres_tables(self, database: str) -> list[ObjectContents]:
        """
        Collecting information about tables in postgresql engine based database.

        Args:
            database: name of database

        Returns:
            list[ObjectContents] - list with meta information about tables in selected database
        """
        results: list[ObjectContents] = []
        query = f"""
                SELECT DISTINCT j.table_catalog, j.table_schema, j.table_name, r.rolname AS table_owner, table_size
                FROM information_schema.tables AS j
                LEFT JOIN (
                    SELECT schemaname, relname AS table_name, pg_relation_size(schemaname || '.' || relname)
                        AS table_size
                    FROM
                    (
                        SELECT schemaname, relname
                        FROM pg_stat_user_tables
                        ORDER BY relname
                    ) AS t
                ) AS i ON (j.table_name = i.table_name)
                JOIN pg_class c ON c.relname = j.table_name
                JOIN pg_namespace n ON n.oid = c.relnamespace
                JOIN pg_roles r ON r.oid = c.relowner
                WHERE j.table_catalog = '{database}' AND j.table_schema NOT IN ('pg_catalog', 'information_schema');
                 """
        result = await self.get_tables_metadata(query=query)
        for record in result.records:
            content = ObjectContents(
                full_path=self.generate_fullpath(record),
                fetch_path=f'{record[0]}.{record[1]}.{record[2]}',
                object_name=f'{record[2]}',
                etag=f'{record[1]}{record[2]}{record[4]}',
                size=record[4],
                service=self.mapper_name,
                source=str(self.source),
                source_UUID=self.source.source_UUID,
                resource_id=self.source.instance,
                owner=record[3],
                source_owner=self.source.master_name,
                source_region=self.source.region,
                data_chunks=await self.create_object_chunks(
                    fetch_path=f'{record[0]}.{record[1]}.{record[2]}', size=record[4]
                ),
            )
            if not content.data_chunks:
                content.status = FileStatus.SCANNED
            results.append(content)
        return results

    async def get_mysql_tables(self, database: str) -> list[ObjectContents]:
        """
        Collecting information about tables in mysql engine based database.

        Args:
            database: name of database

        Returns:
            list[ObjectContents] - list with meta information about tables in selected database
        """
        results: list[ObjectContents] = []
        query = f"""
                SELECT DISTINCT 
                t.TABLE_CATALOG, t.TABLE_SCHEMA, t.TABLE_NAME, ROUND(t.DATA_LENGTH + t.INDEX_LENGTH), t.CREATE_TIME
                FROM information_schema.TABLES AS t
                WHERE t.TABLE_SCHEMA = '{database}';
                 """
        result = await self.get_tables_metadata(query=query)
        for record in result.records:
            content = ObjectContents(
                full_path=self.generate_fullpath(record),
                fetch_path=f'{record[1]}.{record[2]}',
                object_name=f'{record[2]}',
                etag=f'{record[1]}{record[2]}{record[3]}',
                size=f'{record[3]}',
                service=self.mapper_name,
                source=str(self.source),
                resource_id=self.source.instance,
                owner=f'{record[1]}',
                source_owner=self.source.master_name,
                source_region=self.source.region,
                object_creation_date=None if not record[4] else f'{record[4]}',
                last_modified=None if not record[4] else f'{record[4]}',
                source_UUID=self.source.source_UUID,
                data_chunks=await self.create_object_chunks(fetch_path=f'{record[1]}.{record[2]}', size=record[3]),
            )
            if not content.data_chunks:
                content.status = FileStatus.SCANNED
            results.append(content)
        return results

    def generate_fullpath(self, record: tuple[str]) -> str:
        """
        Concatenating fields to create full path to object.

        Args:
            record: contain the following elements in order: 1. Database host endpoint, 2. Database name,
            3. Schema name, 4. Table name.
            Example - (
            'mydb.cphxmr3m4kmi.us-east-1.rds.amazonaws.com', 'postgres', 'defschema', 'accounts'
            )
        Returns:
            joined string from tuple values.
        """
        elements = (self.source.endpoint, *record[:3])
        return '/'.join(filter(lambda x: x, elements))

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
            fetch_path: path for scanning table in rds database
            chunk_path: path to scanning chunk
            limit: represents how many rows must be retrieved
            offset: represents start position to retrieve chunk

        Returns:
            dataframe object with data from table by fetch path, limit and offset or None if no data present in table
        """
        await self.get_source_configuration(db_instance=self.source)
        self.source.name = fetch_path.split('.', 1)[0]
        match self.source.engine:
            case 'mysql+pymysql':
                path = f"`{fetch_path.split('.')[1]}`"
            case 'postgresql':
                path = fetch_path
        query = f'select * from {path} LIMIT {limit} OFFSET {offset};'
        results: RDSTablesResults = await self.get_tables_metadata(query=query)
        if not results.records:
            return None
        return pd.DataFrame(results.records, columns=results.columns)

    async def exclude_redundant_objects(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """
        Not implemented for RDS. Method to exclude specific table from retrieved by source.
        Args:
            objects: list of ObjectContents which contains metadata for each object

        Returns:
            all objects that were detected for scanning
        """
        return objects  # todo: add exclusion built-in objects
