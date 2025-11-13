import base64
import os
import re
from datetime import datetime
from typing import Any, Optional

import aiohttp  # type: ignore
from loguru import logger

from app.core.config import settings
from app.schemas import GithubConfig
from app.schemas.file_data import ObjectContents, SupportedServices
from app.schemas.github import GitHubContentTypes, GitHubInputData
from app.send_request import APIEndpoints, HTTPMethods, send_request
from app.services.base_scan_service import BaseScanService
from app.services.file_service import ARCHIVE_EXTENSIONS, FileService


class GitHubService(FileService, BaseScanService):
    mapper_name = SupportedServices.GitHub

    def __init__(  # type: ignore
        self, source: GitHubInputData, credentials: Optional[GithubConfig] = None, *args, **kwargs
    ) -> None:
        super().__init__(source=source, credentials=credentials, *args, **kwargs)  # type: ignore
        self.headers: dict[str, str] = {}

    async def __aenter__(self) -> 'GitHubService':
        """
        This method is called when entering a context using the `async with` statement.
        It sets up the necessary credentials for GitHub API access.
        If credentials are not already present, it fetches them using a GET request.

        Returns:
            The instance itself, set up with the necessary headers for GitHub API authentication.
        """
        if not self.credentials:
            self.credentials = await send_request(
                method=HTTPMethods.GET,
                url=APIEndpoints.CLOUD_ACCOUNT.url,
                response_model=GithubConfig,
                account_id=self.account_id,
            )
        self.headers = {
            'Authorization': f'token {self.credentials.access_token}',
        }
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
        pass

    async def send_request(
        self, url: str, params: Optional[dict[str, Any]] = None, content_type: Optional[str] = None
    ) -> Optional[dict[str, Any]]:
        headers = self.headers
        if content_type:
            headers['Content-type'] = content_type

        try:
            async with aiohttp.request(
                method='GET',
                url=url,
                headers=headers,
                params=params,
            ) as response:
                result = await response.json()
                if extension := re.search(r'(\.)(tar(\.gz|\.bz2)?|zip)($)', url):
                    return await self.send_request(
                        url=result.get('git_url'),
                        content_type=GitHubContentTypes(extension.group(2)).content_type,  # type: ignore
                    )
                if isinstance(result, dict) and result.get('message', '') == 'Bad credentials':
                    logger.error('Bad credentials')
                    return None
                return result  # type: ignore
        except Exception as e:
            logger.error(e)
            return None

    async def read_data(self, fetch_path: str, service_client: Optional[Any] = None) -> Any:
        fetch_path = fetch_path.split('::')[-1]
        repo_branch_path, file_path = fetch_path.split(':')
        repository, branch = repo_branch_path.rsplit('/', maxsplit=1)
        try:
            response = await self.send_request(
                url=f'{self.credentials.server_url}/repos/{repository}/contents/{file_path}',
                params={'ref': branch},
            )
            if response and response.get('content'):
                content_bytes = base64.b64decode(response['content'])
                return content_bytes
        except Exception as e:
            logger.warning(f'Unable to fetch data due to: {e}')
            return None

    async def get_folder_files(self, path: str) -> list[ObjectContents]:
        """
        Retrieve a list of files and directories within a specified directory in branch.
        Recursively launch when directory was detected in directory.

        Args:
            path : path to the folder

        Returns:
            list of ObjectContents objects representing files
        """

        url = f'{self.credentials.server_url}/repos/{self.source.repo_name}/contents/'
        files_response = await self.send_request(url + '/' + path, params={'ref': self.source.branch_name})
        for file in files_response or []:
            if not file.get('path'):
                continue
            if file.get('type') == 'file':  # type: ignore
                yield ObjectContents(
                    service=self.mapper_name,
                    source=str(self.source),
                    full_path=(
                        f'{self.source.repo_owner}::{self.source.repo_name}/{self.source.branch_name}:'
                        f'{file.get("path")}'  # type: ignore
                    ),
                    fetch_path=(
                        f'{self.source.repo_owner}::{self.source.repo_name}/{self.source.branch_name}:'
                        f'{file.get("path")}'  # type: ignore
                    ),
                    object_name=file.get('name', ''),
                    etag=file.get('sha', ''),
                    size=file.get('size', 0),
                    resource_id=str(self.source),
                    source_region="SaaS",
                    source_owner=self.source.repo_owner,
                    owner=self.source.repo_owner,
                    source_UUID=self.source.source_UUID,
                )
            if file.get('type') == 'dir':  # type: ignore
                async for result in self.get_folder_files(path=file.get('path')):  # type: ignore
                    yield result

    async def get_all_files_by_branch(self) -> list[ObjectContents]:
        """
        Retrieve all files in the repository by a specific branch.

        Returns:
            list of ObjectContents objects representing files in the branch.
        """
        url = f'{self.credentials.server_url}/repos/{self.source.repo_name}/contents/'
        files_list: list[ObjectContents] = []
        files_response = await self.send_request(url, params={'ref': self.source.branch_name})
        for file in files_response or []:
            if not file.get('path'):
                continue
            if file.get('type') == 'file':  # type: ignore
                files_list.append(
                    ObjectContents(
                        service=self.mapper_name,
                        source=str(self.source),
                        full_path=(
                            f'{self.source.repo_owner}::{self.source.repo_name}/{self.source.branch_name}/'
                            f'{file.get("path")}'  # type: ignore
                        ),
                        fetch_path=(
                            f'{self.source.repo_owner}::{self.source.repo_name}/{self.source.branch_name}/'
                            f'{file.get("path")}'  # type: ignore
                        ),
                        object_name=file.get('name', ''),
                        etag=file.get('sha', ''),
                        size=file.get('size', 0),
                        resource_id=str(self.source),
                        source_region="SaaS",
                        source_owner=self.source.repo_owner,
                        owner=self.source.repo_owner,
                        source_UUID=self.source.source_UUID,
                    )
                )
            if file.get('type') == 'dir':  # type: ignore
                async for result in self.get_folder_files(path=file.get('path')):  # type: ignore
                    files_list.append(result)
        return files_list

    async def get_objects_by_source(self) -> list[ObjectContents]:
        """
        Retrieve files from the source repository.

        Returns:
            list of ObjectContents objects representing files retrieved from the source.
        """
        files_list: list[ObjectContents] = []
        try:
            for file in await self.get_all_files_by_branch():
                if content := await self.collect_file_chunks(file):
                    files_list.append(content)
        except Exception as e:
            logger.error(f'Unable to get objects by source due to: {e}')
        finally:
            logger.info(f'{str(self.source)} objects to scan: {len(files_list)}')
            return files_list

    async def fetch_data(
        self,
        fetch_path: str,
        chunk_path: str,
        limit: int,
        offset: int,
    ) -> Any:
        """
        Fetching data from GitHub branch object by fetch path by parameters

        Args:
            fetch_path: retrieving path
            chunk_path: path to scanning chunk
            limit: how many items must be retrieved - not implemented for GitHub
            offset: start position to retrieve chunk - not implemented for GitHub

        Returns:
            data from GitHub branch object by fetch path, that was converted from bytes to string or in Dataframe,
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
        object_data = await self.read_data(fetch_path=fetch_path)
        return None if not object_data else self.prepare_file(object_data, fetch_path, limit, offset)

    async def exclude_redundant_objects(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        return objects

    async def get_source_configuration(self, *args, **kwargs) -> Any:  # type: ignore
        ...
