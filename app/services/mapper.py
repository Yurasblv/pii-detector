import enum
from typing import Any

from app.services.bitbucket import BitBucketService
from app.services.documentdb import DocumentDBService
from app.services.dynamodb import DynamoDBService
from app.services.github import GitHubService
from app.services.gitlab import GitLabService
from app.services.rds_service import RDSService
from app.services.redshift_service import RedshiftService
from app.services.s3_service import S3Service
from app.services.snowflake_service import SnowflakeService


class ServicesMapper(str, enum.Enum):
    """
    This class extends `str` and `enum.Enum` to create an enumeration where each member represents a specific service
    like S3, Redshift, Snowflake, etc. Each enum member is associated with the service's mapper name,
    its service handler class, and the native resource type name.

    Members:
    - S3: Amazon Simple Storage Service (S3).
    - REDSHIFT: Amazon Redshift service.
    - SNOWFLAKE: Snowflake service.
    - RDS: Amazon Relational Database Service (RDS).
    - DynamoDB: Amazon DynamoDB service.
    - DocumentDB: Amazon DocumentDB service.
    - GitHub: GitHub service.
    - BitBucket: BitBucket service.
    - GitLab: GitLab service.

    Each member is a tuple of three elements: the service's mapper name, the service handler class, and a string
    representing the native resource type associated with the service.

    Methods:
        __new__: Customizes the creation of enumeration members.

    Args for __new__:
        value (Any): The mapper name of the service.
        service (Any): The service handler class.
        native_resource (str): The native resource type name associated with the service.

    Returns:
        An instance of the ServicesMapper enumeration.
    """

    S3 = (S3Service.mapper_name.value, S3Service, 'SimpleStorageService')
    REDSHIFT = (RedshiftService.mapper_name.value, RedshiftService, 'RedshiftCluster')
    SNOWFLAKE = (SnowflakeService.mapper_name.value, SnowflakeService, 'SnowflakeDatabases')
    RDS = (RDSService.mapper_name.value, RDSService, 'RelationalDatabaseService')
    DynamoDB = (DynamoDBService.mapper_name.value, DynamoDBService, 'DynamoDB')
    DocumentDB = (DocumentDBService.mapper_name.value, DocumentDBService, 'DocumentDBCluster')
    GitHub = (GitHubService.mapper_name.value, GitHubService, 'GitHubBranch')
    BitBucket = (BitBucketService.mapper_name.value, BitBucketService, 'BitBucketBranch')
    GitLab = (GitLabService.mapper_name.value, GitLabService, 'GitLabBranch')

    def __new__(cls, value: Any, service: Any, native_resource: str) -> 'ServicesMapper':
        obj = str.__new__(cls)
        obj._value_ = value
        obj.service = service
        obj.native_resource = native_resource
        return obj
