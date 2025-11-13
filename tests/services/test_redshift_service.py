import pytest

from app.schemas import LoggedInUser, ObjectContents
from app.schemas.redshift import RedshiftInputData
from app.services.redshift_service import RedshiftService


@pytest.fixture(scope='module')
def redshift() -> dict[str, str]:
    return {
        'cluster': 'test-redshift-cluster',
        'db_name': 'test-db',
        'db_user': 'test-user',
        'db_schema': 'test-schema',
    }


@pytest.fixture(scope='module')
def redshift_table(redshift) -> dict[str, str]:
    return {
        'database': redshift.get('db_name'),
        'schema': redshift.get('db_schema'),
        'table_id': 'test_table_id',
        'table': 'test_table',
        'size': 32,
    }


@pytest.fixture(scope='module')
def source(redshift: dict[str, str]) -> str:
    return f'{redshift["cluster"]}_{redshift["db_name"]}_{redshift["db_user"]}'


@pytest.fixture(scope='module')
def redshift_input_data() -> RedshiftInputData:
    return RedshiftInputData(
        cluster='test_cluster',
        db_user='test_db_user',
        db_name='test_db_name',
    )


@pytest.fixture(scope='function')
async def redshift_service(user: LoggedInUser, redshift_input_data: RedshiftInputData) -> RedshiftService:
    return RedshiftService(org_id=user.org_id, source=str(redshift_input_data), account_id=user.user_id)


@pytest.mark.skip()
@pytest.mark.asyncio
async def test_redshift_filter_objects(
    redshift_service: RedshiftService, pii_file_object: ObjectContents, phi_file_object: ObjectContents
) -> None:
    objects = [pii_file_object, phi_file_object]
    metadata = await redshift_service.filter_objects(objects=objects)
    assert metadata
    assert metadata == objects
