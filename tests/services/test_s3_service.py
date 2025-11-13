import pytest

from app.schemas import ObjectContents
from app.services.s3_service import S3Service


@pytest.fixture(scope='function')
def bucket_name() -> str:
    return 'test_bucket'


@pytest.fixture(scope='function')
async def s3_service(bucket_name: str) -> S3Service:
    return S3Service(
        source=bucket_name,
        account_id='test_account_id',
    )


@pytest.mark.skip()
@pytest.mark.asyncio
async def test_s3_filter_objects(
    s3_service: S3Service,
    pii_file_object: ObjectContents,
    phi_file_object: ObjectContents,
    default_extensions: set[str],
) -> None:
    objects = [pii_file_object, phi_file_object]
    metadata = await s3_service.filter_objects(objects=objects)
    assert metadata
    assert metadata == objects
