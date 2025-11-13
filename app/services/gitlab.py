import base64
import os
from typing import Any, Optional

from gitlab import Gitlab  # type: ignore
from loguru import logger

from app.core.config import settings
from app.schemas import GitlabConfig, GitLabInputData, ObjectContents, SupportedServices
from app.send_request import APIEndpoints, HTTPMethods, send_request
from app.services.base_scan_service import BaseScanService
from app.services.file_service import ARCHIVE_EXTENSIONS, FileService


class GitLabService(FileService, BaseScanService):
    mapper_name = SupportedServices.GitLab

    def __init__(self, source: GitLabInputData | str, *args, **kwargs) -> None:  # type: ignore
        """
        Initializes an instance of the class with the specified GitLab input data.

        Args:
            source: The source data from GitLab(GitLabInputData). In case of GitLab source is branch.
            args: Variable length argument list to be passed to the superclass.
            kwargs: Arbitrary keyword arguments to be passed to the superclass.

        This method calls the superclass initializer and sets up initial attributes for the GitLab-related operations.
        """
        super().__init__(source=source, *args, **kwargs)
        self.git_lab = None

    async def __aenter__(self) -> 'GitLabService':
        """
        This method is called when entering a context using the `async with` statement.

        This method is invoked upon entering an `async with` block.
        It ensures that the GitLabService is correctly initialized with necessary credentials.
        If credentials are not already set, it fetches them using an asynchronous request.

        Returns:
            The instance itself, configured with GitLab credentials for API access.
        """
        if not self.credentials:
            self.credentials = await send_request(
                method=HTTPMethods.GET,
                url=APIEndpoints.CLOUD_ACCOUNT.url,
                response_model=GitlabConfig,
                account_id=self.account_id,
            )
        self.git_lab = Gitlab(self.credentials.server_url, private_token=self.credentials.access_token)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:  # type: ignore
        """
        This method is called when exiting an asynchronous context using the `async with` statement.

        Args:
            exc_type: The exception type, if any exception was raised within the context.
            exc_val: The exception value, if any exception was raised.
            exc_tb: The traceback of the exception, if any was raised.
        """
        self.git_lab.session.close()

    async def read_data(self, fetch_path: str, service_client: Optional[Any] = None) -> Optional[bytes]:  # type: ignore
        """
        Reading data from GitLab branch object by fetch path

        Args:
            fetch_path: retrieving path
            service_client: GitLab client

        Returns:
            data from GitLab branch object by fetch path
        """
        branch_name = str(self.source).split(':')[1]
        repo_id = fetch_path.split('::')[0]
        file_path = fetch_path.split(':')[-1]
        try:
            repository = self.git_lab.projects.get(int(repo_id))
            content = repository.files.get(file_path=file_path, ref=branch_name).content
            if not content:
                return None
            file_bytes = base64.b64decode(content)
            return file_bytes
        except Exception as e:
            logger.error(f'Unable to read data due to: {e}')

    async def get_all_files_by_branch(self, directory: Optional[str] = None) -> list[ObjectContents]:
        files_list: list[ObjectContents] = []
        repository = self.git_lab.projects.get(self.source.repo_id)
        response = repository.repository_tree(ref=self.source.branch_name, path=directory, get_all=True)
        for file in response:
            if file.get("type") == "tree":
                files_list.extend(await self.get_all_files_by_branch(directory=file.get("path")))
            if file.get("type") == "blob":
                blob = repository.files.get(file_path=file.get("path"), ref=self.source.branch_name)
                files_list.append(
                    ObjectContents(
                        service=self.mapper_name,
                        source=str(self.source),
                        full_path=f'{self.source.repo_id}::{self.source.repo_name}/{self.source.branch_name}:'
                        f'{file.get("path")}',
                        fetch_path=f'{self.source.repo_id}::{self.source.repo_name}/{self.source.branch_name}:'
                        f'{file.get("path")}',
                        object_name=file.get("name"),
                        etag=file.get("id"),
                        size=blob.attributes.get("size"),
                        resource_id=str(self.source),
                        source_region="SaaS",
                        source_owner=self.source.repo_owner,
                        owner=self.source.repo_owner,
                        source_UUID=self.source.source_UUID,
                    )
                )
        return files_list

    async def get_objects_by_source(self) -> list[ObjectContents]:
        """
        Collecting files for source (branch for selected repository)
         Args:
            needed when using the method recursively(when you need to get a list of files from a folder)
        Returns:
            files_list: the list of file for specific branch in selected repository
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
        Fetching data from GitLab branch object by fetch path by parameters

        Args:
            fetch_path: retrieving path
            chunk_path: path to scanning chunk
            limit: how many items must be retrieved - not implemented for GitLab
            offset: start position to retrieve chunk - not implemented for GitLab

        Returns:
            data from GitLab branch object by fetch path, that was converted from bytes to string or in Dataframe,
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
        """
        Not implemented for GitLab. Method to exclude specific object from retrieved by source.
        Args:
            objects: list of ObjectContents which contains metadata for each object

        Returns:
            all objects that were detected for scanning
        """
        return objects

    async def get_source_configuration(self, *args, **kwargs) -> Any:  # type: ignore
        ...
