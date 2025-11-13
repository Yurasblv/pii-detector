from typing import Any, AsyncGenerator
from unittest.mock import AsyncMock, MagicMock, Mock

import pytest

from app.schemas import FileData, FileMetadata, ObjectContents
from app.services.base_service import BaseService


@pytest.fixture(scope='function')
async def base_service() -> BaseService:
    BaseService.__abstractmethods__ = set()
    return BaseService(source='test_source', account_id='92b47dcb-61fb-406b-8a9f-e2ab9c598a55')


@pytest.mark.skip()
@pytest.mark.asyncio
async def test_delete_file_metadata(base_service: BaseService, mock_metadata: FileMetadata) -> None:
    deleted_metadata = await base_service.delete_file_metadata(metadata_id=mock_metadata.id)
    assert deleted_metadata
    assert deleted_metadata == mock_metadata


@pytest.mark.skip()
@pytest.mark.asyncio
async def test_save_file_metadata(save_record: Any, base_service: BaseService, mock_metadata: FileMetadata) -> None:
    saved_metadata = await base_service.update_scanned_metadata(obj_in=mock_metadata)  # type: ignore
    assert saved_metadata
    assert saved_metadata.dict(exclude={'id'}) == mock_metadata.dict(exclude={'id'})


@pytest.mark.skip()
@pytest.mark.asyncio
async def test_remove_unused_files(
    base_service: BaseService,
    mock_metadata: FileMetadata,
    pii_file_object: ObjectContents,
) -> None:
    deleted_file = mock_metadata.copy(update={'file_full_path': 'deleted_file.txt'})
    base_service.stored_source_metadata = [mock_metadata, deleted_file]
    deleted_files = await base_service.remove_deleted_files_from_db([pii_file_object])
    assert len(deleted_files) == 1
    assert deleted_files.pop()[0] == "deleted_file.txt"


@pytest.mark.skip()
@pytest.mark.asyncio
async def test_copy_existing_metadata(
    base_service: BaseService,
    mock_metadata: FileMetadata,
    metadata_copy: FileMetadata,
    pii_file_object: ObjectContents,
) -> None:
    copied_metadata = await base_service.copy_existing_metadata(
        metadata=mock_metadata, file_full_path=pii_file_object.full_path, etag=pii_file_object.etag
    )
    assert copied_metadata
    assert copied_metadata.dict(exclude={'id'}) == mock_metadata.dict(exclude={'id'})


@pytest.mark.skip('mock async generator (fetch_data)')
@pytest.mark.asyncio
async def test_fetch_scan_and_save_file(
    base_service: BaseService,
    mock_file_data: FileData,
    mock_metadata: FileMetadata,
    pii_file_object: ObjectContents,
    fetch_data: AsyncGenerator,
) -> None:
    mock = MagicMock()
    mock.__aiter__.side_effect = fetch_data
    base_service.fetch_data = AsyncMock(return_value=mock)
    base_service.presidio_service.scan_file_object = Mock(return_value=[mock_file_data, mock_file_data])
    base_service.save_file_metadata = AsyncMock(return_value=[mock_file_data, mock_file_data])
    base_service.presidio_service.get_pii_types = AsyncMock(return_value=['PII'])
    base_service.presidio_service.check_threshold = Mock(return_value=None)
    base_service.presidio_service.cut_records_by_threshold = Mock(return_value=[mock_file_data, mock_file_data])
    metadata = await base_service.fetch_scan_and_save_file(file_object=pii_file_object)
    assert metadata.files == [mock_file_data, mock_file_data]
    assert metadata.labels == ['PII']
    assert metadata.source == base_service.source
