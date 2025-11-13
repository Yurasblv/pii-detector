from unittest.mock import Mock

import pytest

from app.schemas import LoggedInUser, ObjectContents, ObjectRead, SnowflakeUser
from app.services.snowflake_service import SnowflakeService


@pytest.fixture(scope='session')
def scan_input(source: str, file_etag: str) -> dict[str, str]:
    return {
        'table_name': 'test_table',
        'table_size': 32,
        'source': source,
        'file_etag': file_etag,
    }


@pytest.fixture(scope='session')
def table_object() -> ObjectRead:
    return ObjectRead(
        db_name='test_db',
        db_schema='test_schema',
        table_name='test_table',
        name='"test_db""test_schema""test_table"',
        size=32,
    )


@pytest.fixture(scope='function')
def snowflake_user() -> SnowflakeUser:
    return SnowflakeUser(
        login='test_login',
        account='test_account',
        account_id='test_account_id',
        name='test_name',
        type='test_type',
        snowflake_entry='test_snowflake_entry',
        encrypted_password='test_encrypted_password',
        encrypted_private_key=None,
    )


@pytest.fixture(scope='function')
async def snowflake_service(user: LoggedInUser, snowflake_user: SnowflakeUser) -> SnowflakeService:
    SnowflakeService._get_session = Mock(return_value=None)
    return SnowflakeService(account_id=user.user_id, credentials=snowflake_user, source=user.user_id)


@pytest.mark.skip()
@pytest.mark.asyncio
async def test_snowflake_filter_objects(
    snowflake_service: SnowflakeService,
    pii_file_object: ObjectContents,
    phi_file_object: ObjectContents,
) -> None:
    objects = [pii_file_object, phi_file_object]
    metadata = await snowflake_service.filter_objects(objects=objects)
    assert metadata
    assert metadata[0] in objects
    assert metadata[1] in objects
