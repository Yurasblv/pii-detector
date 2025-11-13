from abc import ABC, abstractmethod
from functools import wraps
from typing import Optional, no_type_check

from aioboto3 import Session
from aiobotocore.config import AioConfig  # type: ignore[import]
from botocore.exceptions import ClientError  # type: ignore[import]
from loguru import logger

from app.core.config import settings
from app.services.base_scan_service import BaseScanService

AIO_CONFIG = AioConfig(
    {'keepalive_timeout': 75},
    connect_timeout=50,
    read_timeout=70,
    max_pool_connections=10,
    retries={'max_attempts': 10},
    region_name=settings.AWS_DEFAULT_REGION,
)


@no_type_check
def boto3_client(resource_name: str):
    def func_wrapper(func):
        @wraps(func)
        async def client_wrapper(self, *args, **kwargs):
            try:
                async with self.session.client(service_name=resource_name, config=AIO_CONFIG) as service_client:
                    return await func(self, service_client=service_client, *args, **kwargs)
            except ClientError as error:
                if error.response['Error']['Code'] == 'ExpiredToken':
                    self.session = Session(region_name=settings.AWS_DEFAULT_REGION)
                    async with self.session.client(service_name=resource_name, config=AIO_CONFIG) as service_client:
                        return await func(self, service_client=service_client, *args, **kwargs)
                else:
                    logger.error(str(error))

        return client_wrapper

    return func_wrapper


class AwsBaseService(BaseScanService, ABC):
    """
    A base service class for AWS services, extending the functionality of BaseService.

    This class provides a foundational structure for AWS services, handling AWS session management
    and defining an abstract method for fetching a list of sources.

    Attributes:
        session: An optional boto3 Session object to manage AWS service sessions.

    Methods:
        __aenter__: Asynchronously initializes the AWS session upon entering the context.
        __aexit__: Asynchronously closes the AWS session upon exiting the context.
        get_list_of_sources: An abstract method, to be implemented by subclasses,
         for retrieving a list of sources from AWS.
    """

    def __init__(self, *args, **kwargs):  # type: ignore
        super().__init__(*args, **kwargs)  # type: ignore
        self.session: Optional[Session] = None

    async def __aenter__(self) -> 'AwsBaseService':
        self.session = Session(region_name=settings.AWS_DEFAULT_REGION)
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:  # type: ignore
        del self.session

    @abstractmethod
    async def get_source_configuration(self, service_client):  # type: ignore
        pass
