import os
from typing import Any, AsyncGenerator, Optional

from aiohttp import BasicAuth, ClientSession  # type: ignore
from loguru import logger

from app.core.config import settings
from app.schemas.bitbucket import BitbucketConfig, BitBucketInputData, BitBucketResult
from app.schemas.file_data import ObjectContents, SupportedServices
from app.send_request import APIEndpoints, HTTPMethods, send_request
from app.services.base_scan_service import BaseScanService
from app.services.file_service import ARCHIVE_EXTENSIONS, FileService


class BitBucketService(FileService, BaseScanService):
    mapper_name = SupportedServices.BitBucket

    def __init__(  # type: ignore
        self, source: BitBucketInputData | str, credentials: Optional[BitbucketConfig] = None, *args, **kwargs
    ) -> None:
        super().__init__(source=source, credentials=credentials, *args, **kwargs)  # type: ignore
        self.session: Optional[ClientSession] = None
        self.api_version = '/2.0'

    async def _get_session(self) -> ClientSession:
        """
        Creates and returns a new aiohttp ClientSession with Basic Authentication.

        Returns:
            ClientSession: An aiohttp ClientSession object configured with Basic Authentication credentials.
        """
        auth = BasicAuth(self.credentials.username, self.credentials.application_password)
        return ClientSession(auth=auth)

    async def __aenter__(self) -> 'BitBucketService':
        """
        This method is automatically called when the BitBucketService instance
            is entered using an `async with` statement.
        It ensures that the instance is initialized with necessary credentials and an HTTP session for making
        authenticated requests to the BitBucket API.

        Returns:
            The instance itself, prepared with credentials and an HTTP session.

        """
        if not self.credentials:
            self.credentials = await send_request(
                method=HTTPMethods.GET,
                url=APIEndpoints.CLOUD_ACCOUNT.url,
                response_model=BitbucketConfig,
                account_id=self.account_id,
            )
        self.session = await self._get_session()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore
        """
        This method is called when exiting an asynchronous context using the `async with` statement.

        Args:
            exc_type: The exception type, if any exception was raised within the context.
            exc_val: The exception value, if any exception was raised.
            exc_tb: The traceback of the exception, if any was raised.
        """
        await self.session.close()  # type: ignore

    async def send_request(self, url: str, read_mode: bool = False) -> Any:
        """
        This method sends an HTTP GET request to the given URL. It can operate in two modes: general and read mode.

        Args:
            url: The URL endpoint to send the request to, appended to the server's base URL.
            read_mode: If True, the method returns raw response data. If False, it returns JSON data. Default is False.

        Returns:
            The processed response data, either as a JSON object or raw data, based on read_mode.
            Returns an empty list in case of errors.
        """
        async with self.session.get(f"{self.credentials.server_url}{self.api_version}{url}") as session:  # type: ignore
            try:
                if not read_mode:
                    response = await session.json()
                    if response.get('type') == 'error':
                        logger.error(f"{response['error']['message']}: {response['error']['detail']}")
                        return []
                    return response
                return await session.read()
            except Exception as e:
                logger.error(e)
                return []

    async def read_data(self, fetch_path: str, service_client: Optional[Any] = None) -> Optional[bytes]:  # type: ignore
        """
        Reading data from BitBucket branch object by fetch path

        Args:
            fetch_path: retrieving path
            service_client: optional client for service

        Returns:
            data from BitBucket branch object by fetch path
        """
        workspace, fetch_path = fetch_path.split('::')
        repo_branch_path, file_path = fetch_path.split(':')
        repository, branch = repo_branch_path.rsplit('/', maxsplit=1)
        try:
            file_data = await self.send_request(
                f'/repositories/{workspace}/{repository}/src/{branch}/{file_path}',
                read_mode=True,
            )
            return file_data if isinstance(file_data, bytes) else None
        except Exception as e:
            logger.error(f'Unable to fetch data due to: {e}')

    async def get_objects_by_source(self) -> list[ObjectContents]:
        """
        This method retrieves all the objects from a BitBucket source.

        It first retrieves the creation date of the source. Then, it creates a list of ObjectContents
        for each file in the source. Each ObjectContent contains the metadata of a file and a list of data chunks
        that represent the file's content.

        If an error occurs while collecting file chunks, it logs the error and continues with the next file.

        Returns:
            A list of ObjectContents, each representing a file in the source.
        """
        source_creation_date = (
            await self.send_request(f'/repositories/{self.source.workspace}/{self.source.repository}')
        ).get('created_on')
        files_list: list[ObjectContents] = []
        objects_list = [
            ObjectContents(
                service=self.mapper_name,
                source=str(self.source),
                full_path=f'{self.source.workspace}::{self.source.repository}/{self.source.branch}:{obj.file_name}',
                fetch_path=f'{self.source.workspace}::{self.source.repository}/{self.source.branch}:{obj.file_name}',
                object_name=obj.file_name,
                etag=f'{self.source}{obj.size}',
                size=obj.size,
                owner=obj.owner,
                resource_id=str(self.source),
                source_region="SaaS",
                source_owner=obj.owner,
                source_creation_date=source_creation_date,
                source_UUID=self.source.source_UUID,
            )
            for obj in await self.get_files_list()
        ]
        try:
            for file in objects_list:
                if content := await self.collect_file_chunks(file):
                    files_list.append(content)
        except Exception as e:
            logger.error(f'Unable to get objects by source due to: {e}')
        finally:
            logger.info(f'{str(self.source)} objects to scan: {len(files_list)}')
            return files_list

    async def get_nested_folder_files(self, folder: str) -> AsyncGenerator[BitBucketResult, None]:
        """
        This method retrieves all the files in a given folder from a BitBucket repository.

        It sends a request to the BitBucket API to retrieve all files in the folder. Then, for each file,
        it checks if it's a directory or a file. If it's a directory, it recursively calls itself to get
        the files in the nested directory. If it's a file, it yields a BitBucketResult object containing
        the file's metadata.

        Args:
            folder: The path of the folder to retrieve files from.

        Yields:
            An object containing the file's metadata.
        """
        files_url = (
            f'/repositories/{self.source.workspace}/{self.source.repository}/src/{self.source.branch}/' + folder + '/'
        )
        nested_files_list = (await self.send_request(files_url)).get('values', [])
        for obj in nested_files_list:
            if obj.get('type') == 'commit_directory':
                async for nested_file in self.get_nested_folder_files(folder=obj.get('path')):
                    yield nested_file

            if obj.get('type') == 'commit_file':
                yield BitBucketResult(
                    workspace=self.source.workspace,
                    repository=self.source.repository,
                    source=str(self.source),
                    file_name=obj.get('path'),
                    type=obj.get('type'),
                    size=obj.get('size'),
                    branch=self.source.branch,
                    owner=self.source.workspace,
                )

    async def get_files_list(self) -> list[BitBucketResult]:
        """
        This method retrieves a list of all files in a BitBucket branch.

        It sends a request to the BitBucket API to retrieve all objects in the branch. Then, for each object,
        it checks if it's a directory or a file. If it's a directory, it recursively calls itself to get
        the files in the nested directory. If it's a file, it appends a BitBucketResult object containing
        the file's metadata to the files list.

        Returns:
            A list of BitBucketResult objects, each containing the metadata of a file in the branch.
        """
        objs = (
            await self.send_request(
                f'/repositories/{self.source.workspace}/{self.source.repository}/src/{self.source.branch}/'
            )
        ).get('values', [])
        files: list[BitBucketResult] = []
        for obj in objs:
            if obj.get('type') == 'commit_directory':
                files.extend([nested_file async for nested_file in self.get_nested_folder_files(obj.get('path'))])

            if obj.get('type') == 'commit_file':
                files.append(
                    BitBucketResult(
                        workspace=self.source.workspace,
                        repository=self.source.repository,
                        source=str(self.source),
                        file_name=obj.get('path'),
                        type=obj.get('type'),
                        size=obj.get('size'),
                        branch=self.source.branch,
                        owner=self.source.workspace,
                    )
                )
        return files

    async def fetch_data(
        self,
        fetch_path: str,
        chunk_path: str,
        limit: int,
        offset: int,
    ) -> Any:
        """
        Fetching data from BitBucket branch object by fetch path by parameters

        Args:
            fetch_path: retrieving path
            chunk_path: path to scanning chunk
            limit: how many items must be retrieved - not implemented for BitBucket
            offset: start position to retrieve chunk - not implemented for BitBucket

        Returns:
            data from BitBucket branch object by fetch path, that was converted from bytes to string or in Dataframe,
             if object has table structure
        """

        limit = limit + settings.OVERLAP_BYTES if offset > 0 else limit
        offset = offset - settings.OVERLAP_BYTES if offset > 0 else offset

        if chunk_path and fetch_path.endswith(ARCHIVE_EXTENSIONS):
            if not os.path.exists(chunk_path):
                # logic for multi scanners,
                # if second scanner will take wait for scan chunks it must have archives locally unpacked
                object_data = await self.read_data(fetch_path=fetch_path)
                if not object_data:
                    return None
                [i for i in self.unpack_archive_locally(fetch_path, object_data)]
            return self.read_archive_object_chunk(chunk_path, limit, offset)
        file_data = await self.read_data(fetch_path=fetch_path)
        return None if not file_data else self.prepare_file(file_data, fetch_path, limit, offset)

    async def exclude_redundant_objects(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        return objects  # todo: add exclusion built-in objects

    async def get_source_configuration(self, *args, **kwargs) -> Any:  # type: ignore
        ...
