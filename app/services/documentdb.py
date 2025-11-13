import json
import os
from math import ceil
from typing import Any, AsyncGenerator, Optional
from urllib import request

import pandas as pd
from loguru import logger
from motor import motor_asyncio  # type: ignore[import]

from app.core.config import settings
from app.schemas import DataChunk, DocumentDBInputData, FileStatus, ObjectContents, SupportedServices
from app.services.aws_base_service import AwsBaseService, boto3_client

CA_NAME = "global-bundle.pem"  # TODO : create function for downloading global-bundle
CA_PATH = os.path.abspath(__file__ + f"/../../../../{CA_NAME}")


class DocumentDBService(AwsBaseService):
    mapper_name = SupportedServices.DocumentDB

    def __init__(self, source: DocumentDBInputData | str, *args, **kwargs) -> None:  # type: ignore
        """
        This service is responsible for handling DocumentDB-specific tasks, leveraging the
        functionalities provided by AwsBaseChunkService and extending them for DocumentDB's use cases.

        Args:
            source: An instance of DocumentDBInputData containing details about the DocumentDB cluster,
                or a string identifier of the source.
            args: Variable length argument list to be passed to the superclass.
            kwargs: Arbitrary keyword arguments to be passed to the superclass.
        """
        super().__init__(source=source, *args, **kwargs)  # type: ignore
        # ca certificate for security connection
        self.ca = self.get_ssL_cert_path() if not os.path.exists(CA_PATH) else CA_PATH

    @staticmethod
    def get_ssL_cert_path() -> Optional[str]:
        """
        Download certificate to connect DocumentDB cluster.

        Returns:
            string path to certificate if response from server was successful else None
        """
        try:
            ca_file = request.urlretrieve(
                'https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem', filename=CA_NAME
            )
            return os.path.abspath(f"{ca_file[0]}")
        except Exception as e:
            logger.error(f'Unable to load ssL certificate. Details: {e}')
            return None

    @boto3_client('secretsmanager')  # type: ignore
    async def get_secret(self, service_client, secret_name: str) -> str:
        get_secret_value_response = await service_client.get_secret_value(SecretId=secret_name)
        return get_secret_value_response.get('SecretString', '')  # type: ignore

    async def get_document_db_client(self, cluster: DocumentDBInputData) -> motor_asyncio.AsyncIOMotorClient:
        """
        Create session to connect.

        Args:
            cluster: DocumentDBInputData with args for connection

        Returns:
            AsyncIOMotorClient session
        """
        secrets = json.loads(await self.get_secret(secret_name=str(cluster)))
        return motor_asyncio.AsyncIOMotorClient(
            host=f"mongodb://{cluster.endpoint}:{cluster.port}",
            username=secrets.get('username'),
            password=secrets.get('password'),
            ssl=True,
            tlsCAFile=self.ca,
        )

    @staticmethod
    async def create_object_chunks(
        db: motor_asyncio.AsyncIOMotorClient, fetch_path: str, size: int = 0
    ) -> list[DataChunk]:
        """
        Creation data chunks for DocumentDB table.
        Count of chunks depends on result of dividing total number of documents by the CHUNK CAPACITY.

        Args:
            db: async mongodb session
            fetch_path: path for retrieving table information
            size: size of current table

        Returns:
            data_chunks: list[DataChunk] or []
        """
        if not size:
            return []
        data_chunks: list[DataChunk] = []
        collection_name = fetch_path.split('/')[1]
        collection = db[collection_name]
        total_docs = await collection.count_documents({})
        for i in range(ceil(total_docs / settings.CHUNK_JSON_CAPACITY)):
            data_chunks.append(
                DataChunk(  # type: ignore
                    object_name=fetch_path,
                    fetch_path=fetch_path,
                    offset=str(i * settings.CHUNK_JSON_CAPACITY),
                    limit=settings.CHUNK_JSON_CAPACITY,
                    instance_id=settings.SCANNER_ID,
                )
            )

        return data_chunks

    @boto3_client('docdb')  # type: ignore
    async def get_source_configuration(self, service_client, cluster: str) -> Any:
        """
        Retrieve tables from specified source.

        Args:
            service_client: boto3 client
            cluster: cluster name
        Returns:
            DocumentDBInputData source response with connection arguments
        """
        try:
            response = await service_client.describe_db_clusters(DBClusterIdentifier=cluster)
            cluster = response.get('DBClusters')[0] if response.get('DBClusters') else None
            return DocumentDBInputData(
                cluster_name=cluster.get('DBClusterIdentifier'),
                endpoint=cluster.get('Endpoint'),
                port=str(cluster.get('Port')),
                master_username=cluster.get('MasterUsername'),
            )
        except Exception as e:
            logger.error(f'Unable to get list of sources due to: {e}')

    @staticmethod
    async def get_collection_metadata(db: motor_asyncio.AsyncIOMotorClient, collection_name: str) -> dict[str, Any]:
        """
        Get additional info (etag and size) for collection.

        Args:
            db: AsyncIOMotorClient session
            collection_name: name of database collection

        Returns:
            dict[str, Any] -  collections size and etag
        """
        try:
            collection_stats = await db.command({"collStats": collection_name})
            collection_size = int(collection_stats["size"])
            etag = collection_stats["ns"] + "." + str(collection_size)
            return {"etag": etag, "collection_size": collection_size}
        except Exception as e:
            logger.warning(e)
            return {"etag": "", "collection_size": 0}

    async def get_database_objects(
        self, client: motor_asyncio.AsyncIOMotorClient, database: str
    ) -> AsyncGenerator[ObjectContents, None]:
        """
        Collect collections metadata from specific database.

        Args:
            client: async mongodb client
            database: name of database

        Returns:
             object : ObjectContents
        """
        db = client[database]
        collection_names = await db.list_collection_names()
        for collection_name in collection_names:
            collection_metadata = await self.get_collection_metadata(db, collection_name)
            content = ObjectContents(
                service=self.mapper_name,
                full_path=f"{self.source.cluster_name}/{database}/{collection_name}",
                fetch_path=f"{database}/{collection_name}",
                object_name=f"{database}/{collection_name}",
                size=collection_metadata['collection_size'],
                etag=collection_metadata['etag'],
                source=self.source.cluster_name,
                resource_id=self.source.cluster_name,
                owner=self.source.master_username,
                source_owner=self.source.master_username,
                source_region=self.source.source_region,
                object_creation_date=self.source.created_at,
                source_UUID=self.source.source_UUID,
                last_modified=self.source.created_at,
                data_chunks=await self.create_object_chunks(
                    db=db,
                    fetch_path=f"{database}/{collection_name}",
                    size=collection_metadata['collection_size'],
                ),
            )
            if not content.data_chunks:
                content.status = FileStatus.SCANNED
            yield content

    async def get_objects_by_source(self) -> list[ObjectContents]:
        """
        Retrieve all databases from source which is current cluster.

        Returns:
            objects_list: list with all databases info
        """
        objects_list = []
        try:
            client = await self.get_document_db_client(cluster=self.source)
            database_names = await client.list_database_names()
            for database in database_names:
                objects_list.extend([obj async for obj in self.get_database_objects(client, database)])
        except Exception as e:
            logger.error(f'Unable to get objects by source due to: {e}')
        finally:
            return objects_list

    async def fetch_data(
        self,
        fetch_path: str,
        chunk_path: str,
        limit: int,
        offset: int,
    ) -> Optional[pd.DataFrame]:
        """
        Fetching data from table object by fetch path, limit and offset.

        Args:
            fetch_path: retrieving path
            chunk_path: path to scanning chunk
            limit: how many items must be retrieved
            offset: start position for extract items

        Returns:
            pandas dataframe with data from collection
        """
        db_name = fetch_path.split("/")[0]
        collection_name = fetch_path.split("/")[1]
        try:
            self.source = await self.get_source_configuration(cluster=self.source)
            client = await self.get_document_db_client(cluster=self.source)
            db = client[db_name]
            collection = db.get_collection(collection_name)
            # getting from collection items from start position which in offset and restrict number of records by limit
            cursor = collection.find().skip(int(offset)).limit(limit)
            # get list from cursor which must have limited capacity
            docs = await cursor.to_list(length=limit)
            columns = set(sum([list(item.keys()) for item in docs], []))
            data_fr = pd.DataFrame(docs, columns=list(columns))
            data_fr = data_fr.apply(lambda x: pd.Series(x.dropna().values))
        except Exception as e:
            logger.error(f'Unable to fetch data due to: {e}')
            return None
        return data_fr

    async def exclude_redundant_objects(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """
        Not implemented for DocumentDB. Method to exclude specific collection from retrieved by source.

        Args:
            objects: list of ObjectContents which contains metadata for each collection

        Returns:
            all collections that were detected for scanning
        """
        return objects
