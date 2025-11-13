from math import ceil
from typing import Any, Optional

import pandas as pd
from loguru import logger

from app.core.config import settings
from app.schemas import DataChunk, DynamoDBInputData, FileStatus, ObjectContents, SupportedServices
from app.services.aws_base_service import AwsBaseService, boto3_client


class DynamoDBService(AwsBaseService):
    mapper_name = SupportedServices.DynamoDB

    def __init__(self, source: DynamoDBInputData | str, *args, **kwargs) -> None:  # type: ignore
        """
        This service is responsible for handling DynamoDB-specific tasks, leveraging the
        functionalities provided by AwsBaseChunkService and extending them for DynamoDB's use cases.

        Args:
            args: Variable length argument list to be passed to the superclass.
            kwargs: Arbitrary keyword arguments to be passed to the superclass.
        """
        super().__init__(source=source, *args, **kwargs)  # type: ignore

    async def create_object_chunks(self, size: int, items_number: int) -> list[DataChunk]:
        """
        Creation data chunks for DynamoDB table based on number of items in table.
         Count of chunks depends on result of dividing total number of items by the CHUNK CAPACITY.
         For each chunk offset will be first item from response.

        Args:
            items_number: number of rows
            size: size of current table

        Returns:
            data_chunks: list[DataChunk] or []
        """
        if not size or not items_number:
            return []
        data_chunks: list[DataChunk] = []
        for i in range(ceil(items_number / settings.CHUNK_ROWS_CAPACITY)):
            data_chunks.append(
                DataChunk(  # type: ignore
                    object_name=str(self.source),
                    fetch_path=str(self.source),
                    offset=str(i * settings.CHUNK_ROWS_CAPACITY),
                    limit=settings.CHUNK_ROWS_CAPACITY,
                    instance_id=settings.SCANNER_ID,
                )
            )
        return data_chunks

    @boto3_client('dynamodb')  # type: ignore
    async def get_objects_by_source(self, service_client) -> list[ObjectContents]:
        """
        Collecting information about records by table.

        Args:
            service_client: boto3 client

        Returns:
            List[ObjectContent]: collected information about object
        """
        response = (await service_client.describe_table(TableName=str(self.source))).get('Table', {})
        content = ObjectContents(
            service=self.mapper_name,
            full_path=str(self.source),
            fetch_path=str(self.source),
            object_name=str(self.source),
            etag=response.get('TableId', ''),
            size=response.get('TableSizeBytes', 0),
            source=str(self.source),
            resource_id=str(self.source),
            owner=self.source.source_owner,
            object_creation_date=response.get('CreationDateTime'),
            last_modified=response.get('CreationDateTime'),
            source_owner=self.source.source_owner,
            source_region=self.source.source_region,
            source_UUID=self.source.source_UUID,
            data_chunks=await self.create_object_chunks(
                size=response.get('TableSizeBytes', 0),
                items_number=response.get('ItemCount', 0),
            ),
        )
        if not content.data_chunks:
            content.status = FileStatus.SCANNED
        return [content]

    @boto3_client('dynamodb')  # type: ignore
    async def fetch_table_object_data(
        self, service_client, table_name: str, limit: Optional[int] = None, offset: Optional[int] = None
    ) -> Optional[list[dict[str, Any]]]:
        """
        Retrieve specified chunk by offset. First pre_call need to identify LastEvaluatedKey. Then we use this key to
        insert key from which we want to get items.

        Args:
            service_client: boto3 client,
            table_name: name of target table
            limit: how many items must be retrieved
            offset: start point to take records

        Returns:
            table_names: list of data from table chunk
        """
        scan_params = {'TableName': table_name, 'Limit': offset or 1, 'Select': 'COUNT'}
        try:
            pre_call = await service_client.scan(**scan_params)
            if last_key := pre_call.get('LastEvaluatedKey'):
                scan_params = {'TableName': table_name, 'Limit': limit, "ExclusiveStartKey": last_key}
                items: list[dict[Any, Any]] = (await service_client.scan(**scan_params)).get('Items', None)
                items.insert(0, last_key)
                del items[-1]
                return items
        except IndexError:
            logger.warning('Empty data')
        except Exception as e:
            logger.error(e)
        return None

    async def fetch_data(
        self,
        fetch_path: str,
        chunk_path: str,
        limit: int,
        offset: int,
    ) -> pd.DataFrame | None:
        """
        Collecting data by parameters.

        Args:
            fetch_path: retrieving path
            chunk_path: path to scanning chunk
            limit: how many items must be retrieved
            offset: start position for extract items

        Returns:
            pd.Dataframe with data from table chunks
        """
        logger.info(f'Start extracting data for {fetch_path} source: {self.source=}')
        scanned_items = await self.fetch_table_object_data(table_name=fetch_path, limit=limit, offset=int(offset))
        if not scanned_items:
            return None
        columns = set(sum([list(item.keys()) for item in scanned_items], []))
        for item in scanned_items:
            for column in columns:
                if column in item.keys():
                    continue
                item[column] = {'': None}
        scanned_items = [{column: item[column] for column in columns} for item in scanned_items]
        return pd.DataFrame(
            [[list(value.values())[0] for value in item.values()] for item in scanned_items], columns=list(columns)
        )

    async def exclude_redundant_objects(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """
        Not implemented for DynamoDB. Method to exclude specific table of keys from retrieved by source.

        Args:
            objects: list of ObjectContents which contains metadata for each table

        Returns:
            all tables that were detected for scanning
        """
        return objects

    async def get_source_configuration(self, *args, **kwargs) -> Any:  # type: ignore
        ...
