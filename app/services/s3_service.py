import os
import re
from typing import Any, Optional

import botocore.exceptions  # type:ignore[import]
from loguru import logger

from app.core.config import settings
from app.core.sub_worker import SubWorker
from app.schemas import DataClassificationType, ObjectAcl, ObjectAclType, ObjectContents, S3InputData, SupportedServices
from app.services.aws_base_service import AwsBaseService, boto3_client
from app.services.file_service import ARCHIVE_EXTENSIONS, CONTAINER_TYPES, FileService

MAX_BUCKET_FILES_AMOUNT = 2_000_000


class S3Service(FileService, AwsBaseService):
    mapper_name = SupportedServices.S3

    def __init__(self, source: S3InputData | str, *args, **kwargs):  # type: ignore
        """
        Initializes a new instance of S3Service

        This service is responsible for handling S3-specific tasks, leveraging the
        functionalities provided by AwsBaseService and extending them for S3Service's use cases.

        Args:
            source: An instance of S3SourceMetadata containing details about the S3Service source,
                    or a string identifier of the source.
            args: Variable length argument list to be passed to the superclass.
            kwargs: Arbitrary keyword arguments to be passed to the superclass.
        """
        super().__init__(source=source, *args, **kwargs)  # type: ignore

    @boto3_client('s3')  # type: ignore
    async def get_objects_by_source(self, service_client) -> list[ObjectContents]:
        """
        Get all objects from s3 bucket with using boto3 paginator.

        Args:
            service_client: boto3 client

        Returns:
            list of ObjectContents with objects metadata information
        """
        total_objects: list[ObjectContents] = []
        paginator = service_client.get_paginator('list_objects')
        try:
            async for page in paginator.paginate(
                Bucket=str(self.source), PaginationConfig={'MaxItems': MAX_BUCKET_FILES_AMOUNT}
            ):
                total_objects.extend(await self.read_content(content_list=page.get('Contents', [])))
        except Exception as e:
            logger.error(f'Unable to get list of files for {str(self.source)}= \n ERROR: {e}')
            return []
        return total_objects

    async def get_head_object_info(  # type: ignore
        self, file_object_content: dict[str, Any], service_client
    ) -> dict[str, Any]:
        """
        Retrieve content type of incoming object in s3 bucket. It needs to recognize type, for example is object folder.

        Args:
            file_object_content: contains object name, size, etag, owner, modified date
            service_client: boto3 client

        Returns:
            string representing type ob object
        """
        try:
            return await service_client.head_object(  # type: ignore
                Bucket=str(self.source), Key=file_object_content.get("Key")
            )
        except Exception as e:
            logger.error(f'Unable to get content for {file_object_content.get("Key")}= \n ERROR: {e}')
            return {}

    @boto3_client('s3')  # type: ignore
    async def read_content(self, content_list: list[dict[str, str]], service_client) -> list[ObjectContents]:
        """
        Parse list of dictionaries with metadata about each object in selected bucket into ObjectContents schema.
        Using semaphore to increase speed of processing.

        Args:
            content_list: list of dictionaries with metadata about each object in selected bucket
            service_client: boto3 client

        Returns:
            list of ObjectContents schemas with metadata about each bucket object
        """
        objects_contents = await SubWorker.run(
            100, *[self.parse_content(file_object_content, service_client) for file_object_content in content_list]
        )
        return [content for content in objects_contents if content]

    async def parse_content(  # type: ignore
        self, file_object_content: dict[str, Any], service_client
    ) -> Optional[ObjectContents]:
        """
        Parse metadata for object from dictionary that was prepared by boto3 paginator into ObjectContents schema.
         Also, method add additional information about object like (ACL, region).

        Args:
            file_object_content: contains object name, size, etag, owner, modified date
            service_client: boto3 client

        Returns:
            ObjectContents schema with all information about incoming object
        """
        head_object = await self.get_head_object_info(file_object_content, service_client)
        content = None

        if re.search(r'vpcflowlogs|CloudTrail|-log', str(file_object_content.get("Key")), re.IGNORECASE):
            return None

        if head_object.get('ContentType', '') == 'application/x-directory; charset=UTF-8':
            return content

        object_acl: ObjectAcl = await self.get_object_acl(file_object_content["Key"], service_client)
        content = ObjectContents(
            service=self.mapper_name,
            source=self.source.source_name,
            full_path=f'{self.source.source_name}/{file_object_content.get("Key")}',
            fetch_path=file_object_content.get("Key", ""),
            object_name=file_object_content.get('Key').rsplit('/', maxsplit=1)[-1],  # type: ignore[union-attr]
            etag=file_object_content.get('ETag', ''),
            size=file_object_content.get('Size', 0),
            resource_id=self.source.source_name,
            owner=file_object_content.get('Owner', {}).get('DisplayName'),
            object_creation_date=file_object_content.get('LastModified'),
            last_modified=file_object_content.get('LastModified'),
            is_public=object_acl.is_public,
            source_owner=self.source.source_owner,
            source_region=self.source.source_region,
            object_acl=list(object_acl.permission_types),
            source_UUID=self.source.source_UUID,
        )
        return await self.collect_file_chunks(content)

    async def get_object_acl(self, key: str, service_client) -> ObjectAcl:
        """
        Retrieve the Access Control List information for a specific object in an AWS S3 bucket.
        This method checks if the specified object is publicly accessible and
        fetches its permission types.

        Args:
            key: name of the object in the S3 bucket
            service_client: boto3 client

        Returns:
            An instance of ObjectAcl containing the ACL information. If the object
            does not exist or in case of an error, it returns an ObjectAcl instance
            with default values.
        """
        try:
            response = await service_client.get_object_acl(Bucket=str(self.source), Key=key)
        except botocore.exceptions.ClientError as error:
            if error.response['Error']['Code'] == 'NoSuchKey':
                return ObjectAcl()
            logger.error(error)
            return ObjectAcl()
        result = ObjectAcl()
        for grant in response.get('Grants', []):
            if "URI" in grant.get("Grantee") and grant.get("Grantee", {}).get("URI", "").endswith("AllUsers"):
                result.is_public = True
                result.permission_types.add(ObjectAclType[grant.get("Permission")])
        return result

    @boto3_client('s3')  # type: ignore
    async def read_data_chunk(self, service_client, fetch_path: str, limit: int, offset: int) -> Optional[bytes]:
        """
        Reads data from an S3 bucket by range from offset and limit.

        Args:
            service_client: The Boto3 S3 service client.
            fetch_path: The path of the data to fetch from the S3 bucket.
            limit: The size of the chunk to read.
            offset: The offset from where to start reading the chunk.

        Returns:
            The data as bytes if successful, or None if an error occurs.
        """
        try:
            logger.debug(f'Start extracting data for {fetch_path} source: {str(self.source)=}')
            response = await service_client.get_object(
                Bucket=str(self.source), Key=fetch_path, Range=f'bytes={offset}-{offset+limit}'
            )
            return await response['Body'].read()  # type: ignore
        except Exception as e:
            logger.error(f'Unable to get data chunk from {fetch_path=} {offset=}, {str(self.source)=}, {e}')

    @boto3_client('s3')  # type: ignore
    async def read_data(self, service_client, fetch_path: str) -> Optional[bytes]:
        """
        Reads data from an S3 bucket.

        Args:
            service_client: The Boto3 S3 service client.
            fetch_path: The path of the data to fetch from the S3 bucket.

        Returns:
            The data as bytes if successful, or None if an error occurs.
        """
        try:
            logger.debug(f'Start extracting data for {fetch_path} source: {str(self.source)=}')
            response = await service_client.get_object(Bucket=str(self.source), Key=fetch_path)
            return await response['Body'].read()  # type: ignore
        except Exception as e:
            logger.error(f'Unable to get data from {fetch_path=}, {str(self.source)=}, {e}')

    async def fetch_data(
        self,
        fetch_path: str,
        chunk_path: str,
        limit: int,
        offset: int,
    ) -> Any:
        """
        Fetching data from bucket object by fetch path by parameters

        Args:
            fetch_path: path for scanning object in s3 bucket
            chunk_path: path to scanning chunk
            limit: how many items must be retrieved - not implemented for s3
            offset: start position to retrieve chunk - not implemented for s3

        Returns:
            data from bucket object by fetch path, that was converted from bytes to string or in Dataframe,
             if object has table structure
        """
        limit = limit + settings.OVERLAP_BYTES if offset > 0 else limit
        offset = offset - settings.OVERLAP_BYTES if offset > 0 else offset

        if fetch_path.endswith(ARCHIVE_EXTENSIONS):
            if not os.path.exists(chunk_path):
                # logic for multi scanners,
                # if second scanner will take wait for scan chunks it must have archives locally unpacked
                object_data = await self.read_data(fetch_path=fetch_path)
                if not object_data:
                    return None
                full_path = f'{str(self.source)}/{fetch_path}'
                [i for i in self.unpack_archive_locally(full_path, object_data)]
            return self.read_archive_object_chunk(chunk_path, limit, offset)
        if fetch_path.endswith(CONTAINER_TYPES):
            object_data = await self.read_data(fetch_path=fetch_path)
        else:
            object_data = await self.read_data_chunk(fetch_path=fetch_path, limit=limit, offset=offset)
        return None if not object_data else self.prepare_file(object_data, fetch_path, limit, offset)

    async def exclude_redundant_objects(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """
        Skipping log files by object name filter.

        Args:
            objects: list of objects metadata

        Returns:
            list of objects metadata without files that have part <log> in object name
        """
        return [file for file in objects if not re.search('log', file.object_name, flags=re.IGNORECASE)]

    async def get_source_configuration(self, service_client):  # type: ignore
        ...
