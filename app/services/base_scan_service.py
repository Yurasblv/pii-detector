import asyncio
import hashlib
import os
import random
import re
from abc import ABC
from datetime import datetime, timezone
from types import NoneType
from typing import Any, Optional
from uuid import UUID

from loguru import logger
from pandas import DataFrame

from app.core.config import settings
from app.schemas import (
    AnalyzerAttributes,
    Category,
    DataChunkBatchCreate,
    DataChunkBatchUpdate,
    DataChunkFilter,
    DataChunkUpdate,
    DataClassification,
    DataClassificationType,
    DataClassifierFilters,
    DataClassifiers,
    DataClassifiersEngine,
    DataClassifierType,
    FileData,
    FileMetadata,
    FileMetadataCreate,
    FileMetadataFilter,
    FileMetadataRead,
    FileMetadataUpdate,
    FileStatus,
    ObjectContents,
    PatternRecognizer,
    ServiceType,
    SupportedServices,
    UpdateDataClassification,
)
from app.send_request import APIEndpoints, HTTPMethods, send_request
from app.services.base_service import BaseService
from app.services.data_analysis_service import DataAnalysisService


class BaseScanService(BaseService, ABC):
    mapper_name: SupportedServices
    LIMIT_SIZE_PER_EXTENSION = 300_000_000
    AMOUNT_OF_RANDOM_OBJECTS = 20

    def __init__(
        self,
        account_id: str,
        source: Any,
        credentials: Any = None,
        analysis_service: Optional[DataAnalysisService] = None,
    ) -> None:
        self.source = source
        self.account_id = account_id
        self.credentials = credentials
        self.stored_source_metadata: list[FileMetadata] = []
        self.analysis_service = analysis_service or DataAnalysisService()
        self.random_sampling = False

    def __repr__(self):  # type: ignore
        pass

    def __str__(self):  # type: ignore
        pass

    async def load_stored_metadata(self) -> None:
        """
        Method to load all metadata by account id and source from database.

        Returns:
            stored_source_metadata: A list of FileMetadata objects from database
        """
        try:
            metadata = await send_request(
                method=HTTPMethods.GET,
                url=APIEndpoints.FILE_METADATA_FILTER.url,
                response_model=list[FileMetadataRead],
                filters=FileMetadataFilter(account_ids=[self.account_id], sources=[str(self.source)]),
            )
        except Exception as e:
            logger.error(f'Unable to load db metadata for {self.source=}: {e}')
            metadata = []
        self.stored_source_metadata = metadata
        return None

    @staticmethod
    async def set_recognizers(data_types: Optional[list[DataClassifiers]] = None) -> Optional[AnalyzerAttributes]:
        """
        This method initializes the analyzers by setting default regex patterns and additional secret patterns.
        If the fetched patterns list is empty or if certain conditions are met,
         those patterns are not added to the analyzer.

        Returns:
            An instance of AnalyzerAttributes or None if no data was fetched.
        """
        if not data_types:
            while True:
                data_types = await send_request(
                    method=HTTPMethods.GET,
                    url=APIEndpoints.CLASSIFIERS_FILTER.url,
                    response_model=list[DataClassifiers],
                    filters=DataClassifierFilters(
                        type=DataClassifierType.REGEX, category=Category.INCLUDE, is_enabled=True
                    ),
                )
                if data_types:
                    break
                sleep_between_attempts_time = 5
                logger.error(f'Unable to fetch data types. Will retry in {sleep_between_attempts_time} secs')
                await asyncio.sleep(sleep_between_attempts_time)

        analyzer_attrs = AnalyzerAttributes()

        last_created_at, last_updated_at = datetime.min, datetime.min

        for _id, data_type in enumerate(data_types, 1):
            analyzer_attrs.labels_mapper[data_type.name.upper()] = data_type.labels
            # Check conditions to determine whether to include this data type's patterns in the analyzer
            if data_type.engine == DataClassifiersEngine.MITIE.value or not data_type.patterns:
                continue

            if data_type.created_at and last_created_at < data_type.created_at:
                last_created_at = data_type.created_at

            if data_type.last_updated_at and last_updated_at < data_type.last_updated_at:
                last_updated_at = data_type.last_updated_at
            # Depending on the engine of the data type, add it to the appropriate list
            if data_type.engine == DataClassifiersEngine.HYPERSCAN:
                analyzer_attrs.hyperscan_recognizers.append(
                    PatternRecognizer(id=_id, name=data_type.name, patterns=data_type.patterns)
                )
            elif data_type.engine == DataClassifiersEngine.RE2:
                analyzer_attrs.re2_recognizers.append(
                    PatternRecognizer(id=_id, name=data_type.name, patterns=data_type.patterns)
                )
            elif data_type.engine == DataClassifiersEngine.RE:
                analyzer_attrs.re_recognizers.append(
                    PatternRecognizer(id=_id, name=data_type.name, patterns=data_type.patterns)
                )
        # Create the id_name_mapper dictionary based on the recognizers
        analyzer_attrs.create_id_name_mapper()
        analyzer_attrs.latest_data_type = max(filter(None, [last_created_at, last_updated_at]))
        return analyzer_attrs

    @staticmethod
    async def delete_file_metadata(obj_in: FileMetadataFilter) -> None:
        """
        Remove metadata record from db by id.

        Args:
            obj_in: schema with filtering fields

        Returns:
            None
        """
        await send_request(
            method=HTTPMethods.DELETE,
            url=APIEndpoints.DELETE_BATCH_METADATA.url,
            response_model=FileMetadataRead,
            obj_in=obj_in,
        )
        return None

    async def save_batch_metadata(self, obj_in: list[FileMetadataCreate]) -> Optional[list[FileMetadataCreate]]:
        """
        Create multiple records of metadata in db.

        Args:
            obj_in: list of objects to be saved.

        Returns:
            saved_metadata: list of saved objects or None if the saving process fails.
        """
        try:
            saved_metadata = await send_request(
                method=HTTPMethods.POST,
                url=APIEndpoints.FILE_METADATA_BATCH.url,
                obj_in=obj_in,
                response_model=list[FileMetadataRead],
            )
            logger.success(f'Saved batch of metadata for source: {str(self.source)}')
            return saved_metadata  # type: ignore[no-any-return]
        except Exception:
            logger.warning(f'Unable to save batch of metadata for source: {str(self.source)}')
        return None

    def _get_random_objects(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """
        Deprecated

        Randomly selects a specified number(constant `AMOUNT_OF_RANDOM_OBJECTS`) of objects from a given list.
        Works only in cases when service use databases as source for scan.

        Args:
            objects: The list of objects to select from.

        Returns:
            objects: A list containing a random subset of the original objects, limited to `AMOUNT_OF_RANDOM_OBJECTS`.
        """
        random.shuffle(objects)
        return objects[: self.AMOUNT_OF_RANDOM_OBJECTS]

    def _get_random_files(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """
        Deprecated

        Randomly selects a specified files from a given list.

        Args:
            objects: The list of objects to select from.

        Returns:
            objects: A list containing a random subset of the original objects, limited to `AMOUNT_OF_RANDOM_OBJECTS`.
        """
        random.shuffle(objects)
        dict_of_objects: dict[str, list[ObjectContents]] = {}
        for obj in objects:
            extension = os.path.splitext(obj.object_name)[1]
            if extension not in dict_of_objects:
                dict_of_objects[extension] = [obj]
            if extension in dict_of_objects and obj.size <= (
                self.LIMIT_SIZE_PER_EXTENSION - sum((content.size for content in dict_of_objects[extension]))
            ):
                dict_of_objects[extension].append(obj)
        return sum(dict_of_objects.values(), [])

    @staticmethod
    def hash_data_chunk(chunk: str | DataFrame, is_object: bool = False) -> str:
        """
        Hashing data with hashlib.md5 .

        Args:
            chunk: data from object. could be either string or pd.Dataframe
            is_object: temporary arg to distinguish chunk or complete file, by default use chunk

        Returns:
            hash format string
        """
        if isinstance(chunk, DataFrame):
            chunk = chunk.to_string(index=False)
        return hashlib.md5(chunk.encode('utf-8'), usedforsecurity=False).hexdigest()

    @staticmethod
    def is_new_ignored_files(db_paths: set[str], objects: list[ObjectContents]) -> bool:
        """
        Iterate through source objects and search for specific which are not presents in db.

        Args:
            db_paths: database paths
            objects: objects that were founded in source

        Returns:
            boolean value
        """
        return False if not any(content for content in objects if content.full_path not in db_paths) else True

    async def set_ignore_status(
        self, objects: list[ObjectContents]
    ) -> list[FileMetadataCreate] | list[FileMetadataUpdate] | None:
        """
        Updating status of object to ignore in situations when object must be excluded from scanning.

        This method processes a given list of objects and determines which ones should be marked
        as ignored based on their current status in the database. It handles three primary tasks:
        - Deleting metadata for objects no longer ignored.
        - Saving new metadata for newly ignored objects.
        - Updating existing metadata to mark objects as ignored.

        Args:
            objects: objects that were founded in source

        Returns:
            None.
        """
        # due to multi scanner support we need to renew list of objects from database to have actual state
        await self.load_stored_metadata()

        # configure additional paths set from database metadata
        db_paths: set[str] = {obj.file_full_path for obj in self.stored_source_metadata}  # type: ignore
        # configure paths set for objects from source
        ignored_paths: set[str] = {obj.full_path for obj in objects}
        # define a list with objects that must be removed if object was removed from ignored
        metadata_ids_delete = [
            obj.id
            for obj in self.stored_source_metadata
            if obj.status == FileStatus.IGNORED and obj.file_full_path not in ignored_paths
        ]
        if metadata_ids_delete:
            await self.delete_file_metadata(obj_in=FileMetadataFilter(ids=metadata_ids_delete))

        if self.is_new_ignored_files(db_paths, objects):
            ignored_create_list: list[FileMetadataCreate] = [
                FileMetadataCreate(
                    file_name=obj_in.object_name,
                    file_etag=obj_in.etag,
                    file_size=obj_in.size,
                    file_full_path=obj_in.full_path,
                    fetch_path=obj_in.fetch_path,
                    source=str(self.source),
                    service=obj_in.service,
                    account_id=self.account_id,
                    random_sampling=self.random_sampling,
                    status=FileStatus.IGNORED,
                    scanned_at=datetime.utcnow(),
                    resource_id=str(self.source),
                    owner=obj_in.owner,
                    object_creation_date=obj_in.object_creation_date,
                    source_owner=obj_in.source_owner,
                    source_region=obj_in.source_region,
                    source_UUID=obj_in.source_UUID,
                    object_acl=obj_in.object_acl,
                    instance_id=settings.SCANNER_ID,
                    labels=[],
                )
                for obj_in in objects
                if obj_in.full_path not in db_paths
            ]
            await self.save_batch_metadata(ignored_create_list)

        # renew status of objects from scanned to ignore status if they must be excluded from scanning procedure
        ignored_update_list: list[FileMetadata] = [
            obj
            for obj in self.stored_source_metadata
            if not obj.status == FileStatus.IGNORED and obj.file_full_path in ignored_paths
        ]

        if ignored_update_list:
            try:
                await send_request(
                    method=HTTPMethods.PATCH,
                    url=APIEndpoints.UPDATE_WITH_IGNORE.url,
                    ids=[str(obj.id) for obj in ignored_update_list],
                )
            except Exception as e:
                logger.error(
                    f'Unable to set IGNORE status for paths: {[obj.file_full_path for obj in ignored_update_list]}.'
                    f'Details: {e}'
                )
        return None

    async def scanner_preparation(self) -> list[FileMetadataCreate]:
        """
        Base method to prepare arguments for source objects. Here are multiple stages which includes:
            configuring patterns,
            filtering names, full paths, etag, sizes etc.,
            saving newly uploaded objects to database,
            enabling randomization to select files for scanning.

        Returns:
            wait_for_scan_objects: complete list of ObjectContents for objects which must be processed later
        """
        await self.load_stored_metadata()
        all_src_objects = await self.get_objects_by_source()
        await self.remove_deleted_files_from_db(all_src_objects)
        filtered_objects = await self.filter_objects(all_src_objects)
        files_to_scan = await self.save_newly_added(filtered_objects=filtered_objects)
        logger.debug(f'Found {len(files_to_scan)} files to scan for source: {str(self.source)}')
        return files_to_scan

    async def save_newly_added(self, filtered_objects: list[ObjectContents]) -> list[FileMetadataCreate]:
        """
        Checkout for a new object that were added to source and create records in database.

        Args:
            filtered_objects: ObjectContents with detected objects metadata from source and filtered by previous steps

        Returns:
            None
        """
        # due to multi scanner support we need to renew list of objects from database to have actual state
        await self.load_stored_metadata()
        # set of tuples with paths and etags from database for current source
        db_path_with_etag = {(obj.file_full_path, obj.file_etag) for obj in self.stored_source_metadata}
        # find out new objects and create them in database
        new_meta = [
            FileMetadataCreate(
                file_name=obj.object_name,
                file_etag=obj.etag,
                file_size=obj.size,
                file_full_path=obj.full_path,
                fetch_path=obj.fetch_path,
                source=obj.source,
                service=obj.service,
                account_id=self.account_id,
                random_sampling=self.random_sampling,
                status=obj.status,
                resource_id=obj.resource_id,
                owner=obj.owner,
                object_creation_date=obj.object_creation_date,
                source_owner=obj.source_owner,
                source_region=obj.source_region,
                source_UUID=obj.source_UUID,
                object_acl=obj.object_acl,
                last_modified=obj.last_modified,
                source_creation_date=obj.source_creation_date,
                instance_id=settings.SCANNER_ID,
                scanned_at=None if obj.data_chunks else datetime.utcnow(),
                labels=None if obj.data_chunks else [],
                chunks=obj.data_chunks,
            )
            for obj in filtered_objects
            if (obj.full_path, obj.etag) not in db_path_with_etag
        ]
        if new_meta:
            await self.save_batch_metadata(new_meta)
        return new_meta

    async def filter_objects(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """
        Method to extract object for scanning basing on objects in classification, which objects are redundant,
        which objects were skipped.

        Args:
            objects: all objects metadata detected in source

        Returns:
            list of objects for further processing
        """
        filtered_objects = await self.exclude_redundant_objects(objects)
        filtered_objects = await self.filter_objects_by_classifications(objects=filtered_objects)
        ignored_objs = list(set(objects) - (set(filtered_objects)))
        await self.set_ignore_status(objects=ignored_objs)
        return await self.filter_scanned(filtered_objects)

    async def filter_scanned(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """
        Method filter incoming objects from source and check if those objects have <SCANNED> status.
        If they exist, they will be removed from object list to prevent scanning already scanned objects.

        Args:
            objects: objects metadata detected in source after setting ignore statuses and filtering by classification

        Returns:
            list of objects for further processing
        """
        db_data: set[tuple[str, str]] = {
            (obj.file_full_path, obj.file_etag)
            for obj in self.stored_source_metadata
            if obj.status == FileStatus.SCANNED
        }
        return [obj for obj in objects if (obj.full_path, obj.etag) not in db_data]

    async def get_randomized(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """Deprecated"""
        if self.mapper_name.type == ServiceType.FILE:
            return self._get_random_files(objects)
        else:
            return self._get_random_objects(objects)

    @staticmethod
    def is_not_valid_chunk(chunk: Optional[str | DataFrame]) -> bool:
        """
        Check out if chunk in not empty. For string and Dataframe there are differences between how to check emptiness.
        Also, it detects is chunk is NoneType.

        Args:
            chunk: data incoming after fetching from object

        Returns:
            boolean result
        """
        return any(
            (
                isinstance(chunk, str) and not chunk,
                isinstance(chunk, DataFrame) and not chunk.bool,
                isinstance(chunk, NoneType),
            )
        )

    @staticmethod
    async def get_classification_sources(
        account_id: str,
        classification_id: str,
        response_model: Any,
    ) -> Optional[list[dict[str, Any]]]:  # todo: create data_classification service
        """
        Get from database classification by UUID.

        Args:
            account_id: account from classification group
            service: service from classification group
            classification_id: id of classification

        Returns:
            list of dicts with sources filtered from inventory
        """
        return await send_request(  # type: ignore[no-any-return]
            method=HTTPMethods.GET,
            url=APIEndpoints.CLASSIFICATION_SOURCES.url,
            account_id=account_id,
            classification_id=classification_id,
            response_model=response_model,
        )

    async def get_all_classifications(
        self,
        category: Optional[Category] = None,
    ) -> list[DataClassification]:
        """
        Get from database all classification basing on account_id(service_id) and service name.
         Also support filtering by category.

        Args:
            category: type of classification
        Returns:
            DataClassifications list if they exist else []
        """
        classifications: list[DataClassification] = await send_request(
            method=HTTPMethods.GET,
            url=APIEndpoints.CLASSIFICATION_FILTER.url,
            response_model=list[DataClassification],
            service_id=self.account_id,
            service=self.mapper_name.value,
            category=category,
        )
        return classifications

    async def update_classification_group_with_last_scanned(self, last_scanned: Optional[datetime]) -> None:
        """
        Update of classification scanned time after scanning procedure.

        Args:
            last_scanned: time of procedure

        Returns:
            None
        """
        await send_request(
            method=HTTPMethods.PUT,
            url=APIEndpoints.UPDATE_CLASSIFICATION_LAST_SCANNED.url,
            parameters=UpdateDataClassification(account_id=self.account_id, last_scanned=last_scanned),
        )
        return None

    async def get_classification_includes(self, data_type: DataClassificationType) -> set[str]:
        """
        Filter objects or sources by classification, basing on its data type.

        Args:
            data_type: classification type

        Returns:
            list of included sources or objects for scanning
        """
        classifications = await self.get_all_classifications()
        included: set[str] = set()
        for cl in classifications:
            # if classification hasn't any objects or sources return empty set
            if (not cl.data_sources and data_type == DataClassificationType.SOURCE) or (
                data_type == DataClassificationType.OBJECT and not cl.data_objects
            ):
                return set()
            # otherwise add only those sources which mentioned in classification
            included.update(
                cl.data_sources if data_type == DataClassificationType.SOURCE else cl.data_objects  # type: ignore
            )
        return included

    @staticmethod
    async def get_include_exclude_filenames() -> tuple[dict[tuple[str], list[str]], dict[tuple[str], list[str]]]:
        """
        Filter objects by name basing on classifier's type <Filename>.

        Returns:
            tuple of dicts with classifiers patterns and labels for include and for exclude
        """
        filenames = await send_request(
            method=HTTPMethods.GET,
            url=APIEndpoints.CLASSIFIERS_FILTER.url,
            response_model=list[DataClassifiers],
            filters=DataClassifierFilters(type=DataClassifierType.FILENAME, is_enabled=True),
        )
        included: dict[tuple[str], list[str]] = {}
        excluded: dict[tuple[str], list[str]] = {}
        for filename in filenames:
            if not filename.patterns:
                continue
            if filename.category == Category.INCLUDE:
                included[tuple(filename.patterns)] = filename.labels
            else:
                excluded[tuple(filename.patterns)] = filename.labels
        return included, excluded

    @staticmethod
    async def is_supported_filename(
        obj: ObjectContents,
        included_filenames: dict[tuple[str], list[str]],
        excluded_filenames: dict[tuple[str], list[str]],
    ) -> bool:
        """
        The method first compiles the exclusion patterns into a regular expression and checks if the object's filename
        matches any of these patterns. If a match is found, the method returns False. If no exclusion pattern matches,
        the method then checks against the inclusion patterns, updating the object's labels and returning True upon
        finding a match. If no included patterns are specified, the method defaults to returning True.

        Args:
            obj: object meta information in source
            included_filenames: patterns and labels for include
            excluded_filenames:  patterns and labels for exclude

        Returns:
            boolean result of checking name by patterns
        """
        excluded_filenames = '|'.join(sum([list(key) for key in excluded_filenames.keys()], []))
        if excluded_filenames and re.search(excluded_filenames, obj.object_name, flags=re.IGNORECASE):
            return False
        if not included_filenames:
            return True
        for patterns, labels in included_filenames.items():
            if re.search('|'.join(patterns), obj.object_name, flags=re.IGNORECASE):
                obj.labels = labels  # type: ignore
                return True
        return False

    async def filter_objects_by_classifications(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """
        Filter incoming objects for source by classification.
         Also, here is filter with classifiers where type is <Filename>.
          It will choose which files could be processed or not basing on their names.

        Args:
            objects: list of objects for source

        Returns:
            configured list of objects to further processing
        """
        included = await self.get_classification_includes(DataClassificationType.OBJECT)
        included_filenames, excluded_filenames = await self.get_include_exclude_filenames()
        object_list = []
        for obj in objects:
            if (not included or obj.object_name in included) and await self.is_supported_filename(
                obj, included_filenames, excluded_filenames
            ):
                object_list.append(obj)
        return object_list

    @staticmethod
    async def create_newly_added_chunks(
        source_objects_chunks: list[str],
        db_chunks: dict[str, str],
        metadata: FileMetadata,
        obj_value: ObjectContents,
        updated_chunks: Optional[set[str]],
    ) -> None:
        """
        Create newly added chunks after filtration. Checks offsets which must be created.
         Created chunks depends on which were updated due to both of them has changed offset.

        Args:
            source_objects_chunks: list of source object chunks in string format that contains
             full_path, offset:hash for each
            db_chunks: dict of all chunks related to current db metadata in format
                key: file_full_path, offset, hash
                value: chunk_id, metadata_id
            metadata: database metadata record
            obj_value: object with meta information
            updated_chunks: list of updated chunks strings in format file_full_path, offset, hash

        Returns:
            None
        """
        # get chunks that could be created of updated
        filtered_chunks = set(source_objects_chunks) - set(db_chunks.keys())
        # list of offsets which must be updated
        offsets_to_update: list[str] = [chunk.split(',')[1] for chunk in updated_chunks]  # type:ignore
        # list of offsets which must be created
        offsets_to_create: list[str] = [
            chunk.split(',')[1] for chunk in filtered_chunks if chunk.split(',')[1] not in offsets_to_update
        ]
        chunks_to_create_list = [chunk for chunk in obj_value.data_chunks if chunk.offset in offsets_to_create]
        if chunks_to_create_list:
            await send_request(
                method=HTTPMethods.POST,
                url=APIEndpoints.CHUNKS_BATCH.url,
                obj_in=DataChunkBatchCreate(
                    metadata_id=metadata.id,
                    metadata_size=metadata.file_size,
                    metadata_status=FileStatus.WAIT_FOR_SCAN,
                    chunks=chunks_to_create_list,
                ),
            )
        return None

    @staticmethod
    async def update_metadata_existing_chunks(
        object_chunks: list[str], metadata: FileMetadata, metadata_chunks: dict[str, str]
    ) -> set[str]:
        """
        Update changed chunks after comparing source object chunks and metadata chunks.

        Args:
            metadata: db metadata object
            object_chunks: list of object chunks in full_path, offset, hash format
            metadata_chunks: db metadata in format key: full_path,offset,hash
                                                                    value: chunk_id,metadata_id

        Returns:
            chunks_to_update: set[str] or empty set with
        """
        filter_params: DataChunkBatchUpdate = DataChunkBatchUpdate(metadata_id=metadata.id, chunks=[])
        chunks_to_update = set(metadata_chunks.keys()) - set(object_chunks)

        for key, value in metadata_chunks.items():
            if key not in chunks_to_update:
                continue
            filter_params.chunks.append(value.split(',')[0])

        if filter_params.chunks:
            update_params = {
                "object_size": metadata.file_size,
                "scanned_at": None,
                "instance_id": settings.SCANNER_ID,
                "labels": None,
                "hash": None,
                "status": FileStatus.WAIT_FOR_SCAN,
            }
            await send_request(
                method=HTTPMethods.PATCH,
                url=APIEndpoints.CHUNKS_BATCH.url,
                filter_params=filter_params,
                update_params=update_params,
            )

            logger.info(f'Updated data chunks: {chunks_to_update}')
        return chunks_to_update

    async def get_wait_for_scan_objects(self) -> list[ObjectContents]:
        chunk_objects = await send_request(
            method=HTTPMethods.GET,
            url=APIEndpoints.CHUNKS_FILTER.url,
            response_model=list[ObjectContents],
            filter_params=DataChunkFilter(
                service=self.mapper_name.value,
                account_ids=[self.account_id],
                status=FileStatus.WAIT_FOR_SCAN,
            ),
        )
        return chunk_objects

    async def remove_deleted_metadata(self, fetched_objects: list[ObjectContents]) -> None:
        """
        Check if object was deleted from source and clean redundant records from database.

        Args:
            fetched_objects: meta information about objects from source

        Returns:
            None
        """

        source_objects_paths = {obj.full_path for obj in fetched_objects}
        db_objects_dict = {metadata.file_full_path: metadata.id for metadata in self.stored_source_metadata}
        db_object_paths = set(db_objects_dict.keys())
        files_to_delete = db_object_paths - source_objects_paths

        if not files_to_delete:
            return None

        meta_ids = [db_objects_dict.get(key) for key in db_objects_dict if key in files_to_delete]

        if meta_ids:
            await self.delete_file_metadata(obj_in=FileMetadataFilter(ids=meta_ids))
            logger.info(f'Deleted unused file data: {files_to_delete}')
            await self.load_stored_metadata()
        return None

    async def remove_deleted_chunks(self, fetched_objects: list[ObjectContents]) -> set[str]:
        """
        Remove chunks if total number of object chunks was decreased.
        Compare objects chunks and existing in db by file_full_path and chunk offset. If in database were chunks that
            not exists in source, we remove these chunks.

        Args:
            fetched_objects: meta information about objects from source

        Returns:
            chunks_to_delete: array of string in file_full_path offset format for chunks that were removed
        """
        db_object_chunks_dict = {}
        source_objects_chunks: set[str] = set()

        # fill source_objects_chunks with full path of object and offset of each founded chunks
        for obj in fetched_objects:
            source_objects_chunks.update(f'{obj.full_path}{chunk.offset}' for chunk in obj.data_chunks)

        # configure dict with database object's chunks in same format like in source_objects_chunks with id value
        # of chunk from database
        for meta in self.stored_source_metadata:
            for chunk in meta.chunks:  # type:ignore
                db_object_chunks_dict[f'{meta.file_full_path}{chunk.get("offset")}'] = chunk.get('id')
        # chunks_to_delete is difference between full_path offset in db_object_chunks_dict keys
        # and source_objects_chunks
        chunks_to_delete = set(db_object_chunks_dict.keys()) - source_objects_chunks
        chunk_ids = [db_object_chunks_dict.get(chunk) for chunk in chunks_to_delete]
        if chunk_ids:
            await send_request(method=HTTPMethods.DELETE, url=APIEndpoints.CHUNKS_BATCH.url, ids=chunk_ids)
            logger.info(f'Deleted unused data chunks: {chunks_to_delete}')
            await self.load_stored_metadata()
        # return chunks to delete to continue processing chunks
        return chunks_to_delete

    async def remove_deleted_files_from_db(self, fetched_objects: list[ObjectContents]) -> None:
        """
        Remove db records about objects that were removed from source. Update chunks where was changes data and
         created new if new data was added.

        Args:
            fetched_objects: meta information about objects from source

        Returns:
            None
        """
        # skip if database empty
        if not self.stored_source_metadata:
            logger.info(f'No stored metadata found for the {str(self.source)}')
            return None

        await self.remove_deleted_metadata(fetched_objects=fetched_objects)

        deleted_chunks: set[str] = await self.remove_deleted_chunks(fetched_objects=fetched_objects)

        metadata_paths_sizes = {meta.file_full_path: meta for meta in self.stored_source_metadata}

        objects_paths_sizes = {(obj.full_path, obj.size): obj for obj in fetched_objects}

        # iterate over source objects in objects_paths_sizes dict where
        #  key is full path and size and value is ObjectContents schema with meta information about object
        for key, obj in objects_paths_sizes.items():
            object_chunks: list[str] = []
            metadata_chunks: dict[str, str] = {}

            # get record with this file full path from database
            meta_obj = metadata_paths_sizes.get(key[0])  # obj_key[0] - full_path

            if not meta_obj or meta_obj.file_size == key[1]:  # obj_key[1] - size
                continue

            # if object size was changed we change it for record that presents in db
            meta_obj.file_size = key[1]

            # create strings that contains object's full path, chunk's offset and chunk's
            # hash for existing in db records and for items at source

            for chunk in obj.data_chunks:
                if f'{obj.full_path}{chunk.offset}' in deleted_chunks:
                    continue
                data = await self.fetch_data(
                    obj.fetch_path, chunk.fetch_path, chunk.limit, int(chunk.offset)  # type:ignore
                )
                object_chunks.append(
                    f'{obj.full_path},{chunk.offset},{self.analysis_service.hash_data(data)}'  # type: ignore
                )

            for chunk in meta_obj.chunks:  # type:ignore
                if f'{meta_obj.file_full_path}{chunk.get("offset")}' in deleted_chunks:
                    continue
                key = f'{meta_obj.file_full_path},{chunk.get("offset")},{chunk.get("hash")}'  # type:ignore
                value = f'{chunk.get("id")},{meta_obj.id}'
                metadata_chunks[key] = value  # type:ignore

            # update chunks
            updated_chunks = await self.update_metadata_existing_chunks(object_chunks, meta_obj, metadata_chunks)
            # creation chunks depends on which chunks were updated because offsets could be changed for updated chunks
            await self.create_newly_added_chunks(object_chunks, metadata_chunks, meta_obj, obj, updated_chunks)

        return None

    @staticmethod
    async def scanning_update_status(
        object_content: ObjectContents,
        status: FileStatus,
        scanner_id: str,
        filter_params: Optional[dict[str, str]] = None,
    ) -> Any:
        """
        Update status for currently scanning chunks.

        Args:
            object_content: object's metadata from source
            scanner_id: id of instance
            status: arg for setting new value
            filter_params: additional field in dict format with values to update

        Returns:
            request result
        """
        if filter_params is None:
            filter_params = {}
        return await send_request(
            method=HTTPMethods.PATCH,
            url=APIEndpoints.CHUNKS.url,
            update_params=DataChunkUpdate(status=status, instance_id=scanner_id),
            filter_params=DataChunkUpdate(id=object_content.current_chunk.id, **filter_params),  # type: ignore
        )

    async def analyze_content_data(self, content: ObjectContents) -> None:
        """
        Function for processing data for chunk of object.
        It gets data by chunk offset and limit.
         Processed with presidio after validation fetched data from object chunk.

        Args:
            content: object metadata in ObjectContents format

        Returns:
            None
        """
        chunk = content.current_chunk
        offset = int(content.current_chunk.offset)  # type:ignore
        logger.info(f'Processing {content.full_path}, chunk: {chunk.offset}')  # type: ignore
        content.data = await self.fetch_data(
            content.fetch_path, chunk.fetch_path, chunk.limit, offset  # type:ignore
        )
        if self.is_not_valid_chunk(content.data):
            return await self.save_chunk(content)

        if self.analysis_service.rescan_mode:  # type:ignore
            content.current_chunk.hash = None  # type:ignore

        content.current_chunk.is_phi = self.analysis_service.is_phi_in_file_data(  # type: ignore
            content.object_name, content.data
        )

        for sensitive_data in self.analysis_service.scan_file_object(content=content):  # type: ignore
            if not sensitive_data:
                continue
            await send_request(
                method=HTTPMethods.POST,
                url=APIEndpoints.SENSITIVE_DATA.url,
                obj_in={
                    'metadata_id': content.current_chunk.metadata_id,  # type: ignore
                    'chunk_id': content.current_chunk.id,  # type: ignore
                    'sensitive_data': sensitive_data,
                },
            )

        return await self.save_chunk(content=content)

    @staticmethod
    async def save_chunk(content: ObjectContents) -> None:
        """
        Function for saving result of scanning chunk. Set status and scanned date for it.
        Also, add labels for each chunk and add for its metadata if they were not exist.

        Args:
            content: object metadata in ObjectContents format

        Returns:
            None
        """
        chunk: DataChunkUpdate = DataChunkUpdate(
            scanned_at=datetime.utcnow(),
            status=FileStatus.SCANNED,
            **content.current_chunk.dict(  # type: ignore
                # this method also uses for update chunk so we exclude 'status' and 'scanned_at' because we want to
                # change status from wait_for_scan to scanned and time when chunk was scanne sensitive_data saved
                # separately
                exclude_none=True,
                exclude={'status', 'scanned_at', 'sensitive_data'},
            ),
        )
        try:
            await send_request(method=HTTPMethods.PUT, url=APIEndpoints.CHUNKS.url, obj_in=chunk)
            logger.success(f'Saved chunk: {content.full_path}, chunk: {chunk.offset}')
        except Exception as e:
            logger.error(f"{e}")
