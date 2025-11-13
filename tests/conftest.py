import asyncio
import datetime
import os
from asyncio import AbstractEventLoop
from typing import Any, Generator
from unittest.mock import MagicMock

import pandas as pd
import pytest
from loguru import logger

from app.schemas.common import LoggedInUser
from app.services import base_service

from app.schemas import (
    Category,
    DataClassification,
    DataClassificationGroup,
    DataClassifiers,
    DataClassifiersCreate,
    DataClassifiersFilter,
    DataClassifiersUpdate,
    DataClassifierType,
    FileData,
    FileMetadata,
    FileStatus,
    ObjectContents,
    SensitivityCategory,
    SensitivityLevel,
    SupportedServices,
)

# Environment


@pytest.fixture(scope='session')
def event_loop() -> Generator[AbstractEventLoop, None, None]:
    yield asyncio.get_event_loop()


# Common


@pytest.fixture(scope='session', autouse=True)
def user() -> LoggedInUser:
    return LoggedInUser.get_test_user()


@pytest.fixture(scope='function', autouse=True)
def mock_boto3_client():
    logger.debug('boto3')
    base_service.boto3_client = MagicMock(return_value=lambda func: func)  #


# PII


@pytest.fixture(scope='session')
def source() -> str:
    return 'test_account_id'


@pytest.fixture(scope='session')
def file_etag() -> str:
    return 'test_etag'


@pytest.fixture(scope='session')
def pii_file_name() -> str:
    return 'pii_sample_data.csv'


@pytest.fixture(scope='session')
def pii_file_path(pii_file_name: str) -> str:
    return os.path.abspath(f'./tests/test_data/pii_file_samples/{pii_file_name}')


@pytest.fixture(scope='session')
def phi_file_name() -> str:
    return 'phi_sample_data.csv'


@pytest.fixture(scope='session')
def phi_file_path(phi_file_name: str) -> str:
    return os.path.abspath(f'./tests/test_data/pii_file_samples/{phi_file_name}')


@pytest.fixture(scope='function')
def file_object(source: str, file_etag: str) -> Any:
    def _get_file_object(file_path: str, file_name: str) -> ObjectContents:
        with open(file_path, 'rb') as file:
            return ObjectContents(
                service='S3',
                source=source,
                full_path=file_path,
                fetch_path=file_path,
                object_name=file_name,
                etag=file_etag,
                size=os.stat(file_path).st_size,
                data=file.read(),
                status=FileStatus.WAIT_FOR_SCAN.value,
                owner='test_user',
                resource_id=source,
                object_creation_date=datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc),
                source_owner='test_user',
                source_region='us-west-1',
            )

    return _get_file_object


@pytest.fixture(scope='function')
async def fetch_data(pii_file_object):
    yield pii_file_object.data


@pytest.fixture(scope='function')
def pii_file_object(file_object: Any, pii_file_path: str, pii_file_name: str) -> ObjectContents:
    return file_object(pii_file_path, pii_file_name)


@pytest.fixture(scope='function')
def phi_file_object(file_object: Any, phi_file_path: str, phi_file_name: str) -> ObjectContents:
    return file_object(phi_file_path, phi_file_name)


@pytest.fixture(scope="function")
def mock_metadata(pii_file_name: str, pii_file_path: str, file_etag: str) -> FileMetadata:
    return FileMetadata(
        account_id='92b47dcb-61fb-406b-8a9f-e2ab9c598a55',
        source='test_source',
        file_name=pii_file_name,
        file_etag=file_etag,
        file_size=32,
        service=SupportedServices.S3,
        labels=['PII'],
        file_full_path=pii_file_path,
        random_sampling=False,
        is_updated=False,
        owner='test_user',
        object_creation_date=datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc),
        status=FileStatus.WAIT_FOR_SCAN.value,
        scanned_at=datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc),
        files=[],
        resource_id='test_source',
        source_owner='test_user',
        source_region='us-west-1',
        source_UUID='cc8eb0e9-3b1e-4325-97d2-d6c9e30afa37',
        object_hash='9b5b679cbf6ecbd82ac93bf7b3cb0921',
    )


@pytest.fixture(scope="function")
def mock_data_classification() -> DataClassification:
    return DataClassification(
        data_sources=["source_1", "source_2"], category="include", type="source", service="S3", scanning_period_hours=3
    )


@pytest.fixture(scope="function")
def mock_data_classification_group(mock_data_classification) -> DataClassificationGroup:
    return DataClassificationGroup(
        name="group_1",
        services_ids=["1", "66f4f68b-5771-41fc-9be6-1b4ea1495480"],
        data_classifications=[mock_data_classification],
    )


@pytest.fixture(scope='function')
def metadata_copy(mock_metadata) -> FileMetadata:
    return FileMetadata.parse_obj(mock_metadata.dict(exclude={'id'}))


@pytest.fixture(scope='function')
def mock_file_data(mock_metadata: FileMetadata, pii_file_name: str) -> FileData:
    return FileData(
        metadata_id=mock_metadata.id,
        file_name=pii_file_name,
        pii_data='pii_data',
        pii_type='pii_type',
        pii_region='test_pii_region',
        score=23.1,
        row=12,
        col=1,
        start_position=12,
        end_position=13,
    )


@pytest.fixture(scope='function')
def file_data_copy(mock_file_data) -> FileData:
    return FileData.parse_obj(mock_file_data.dict(exclude={'id'}))


@pytest.fixture(scope='module')
def mock_dataframe():
    return pd.DataFrame([[3, 4], [5, 6]], [1, 2], ['col1', 'col2'], dtype=str)


@pytest.fixture(scope='function')
def mock_data_classifier() -> DataClassifiers:
    return DataClassifiers(
        name='pii_type',
        pattern='test_pattern',
        description='test_description',
        category=Category.INCLUDE,
        is_enabled=True,
        type=DataClassifierType.REGEX,
        service_ids=['66f4f68b-5771-41fc-9be6-1b4ea1495480'],
        labels=['PII'],
        sensitivity_category=SensitivityCategory.INTERNAL.value,
        sensitivity_level=SensitivityLevel.LOW.value,
    )


@pytest.fixture(scope='function')
def mock_data_classifier_create() -> DataClassifiersCreate:
    return DataClassifiersCreate(
        name='pii_type',
        pattern='test_pattern',
        description='test_description',
        category=Category.INCLUDE,
        is_enabled=True,
        type=DataClassifierType.REGEX,
        labels=['PII'],
        sensitivity_category=SensitivityCategory.INTERNAL.value,
        sensitivity_level=SensitivityLevel.LOW.value,
    )


@pytest.fixture(scope='function')
def mock_data_classifier_update() -> DataClassifiersUpdate:
    return DataClassifiersUpdate(
        name='test_update_name',
        pattern='test_update_pattern',
        description='test_update_description',
        category=Category.INCLUDE,
        is_enabled=False,
        type=DataClassifierType.REGEX,
        labels=None,
    )


@pytest.fixture(scope='function')
def mock_filter_fields() -> DataClassifiersFilter:
    return DataClassifiersFilter(
        type=DataClassifierType.REGEX, service_id="66f4f68b-5771-41fc-9be6-1b4ea1495480", service=SupportedServices.S3
    )
