import enum
import uuid
from datetime import datetime
from typing import Optional
from pydantic import BaseModel
from sqlmodel import Field


class Base(BaseModel):
    id: uuid.UUID

    def __init__(self, **data):  # type: ignore
        """
        This method overrides the default initialization behavior.
        It ensures that if an 'id' is not explicitly provided in the initialization data, a new unique UUID is generated
        and assigned as the 'id'.

        Args:
            data: Variable length keyword arguments. Expected to contain initialization data for the Base class and
            its fields.
        """
        if 'id' not in data:
            data['id'] = uuid.uuid4()
        super().__init__(**data)


class AuditInfo(BaseModel):
    """
    Base model for basic audit in table.
    AuditBase scheme is inherited from it
    """

    created_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: Optional[str]
    last_updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    last_updated_by: Optional[str]


class AuditBase(Base, AuditInfo):
    """Model for basic audit in table."""


class Category(str, enum.Enum):
    """
    Model that describe data_type or data_classification category
    """

    EXCLUDE = 'exclude'
    INCLUDE = 'include'

    def __str__(self) -> str:
        """
        Returns the string representation of the enumeration option.

        Returns:
            str: The string value of the enum option.
        """
        return self.value


class LoggedInUser(BaseModel):
    """
    A model representing a logged-in user's profile and permissions.

    This class encapsulates key information about a user who is currently logged into a system,
    including his username, email, full_name, permissions. Used for authorization

    Attributes:
        username: The username of the logged-in user.
        email: The email address of the user. Default is None.
        full_name: The full name of the user. Default is None.
        disabled: A flag indicating whether the user's account is disabled.
        user_id: A unique identifier for the user.
        org_id: The identifier of the organization to which the user belongs.
        org_name: The name of the organization to which the user belongs.
        permissions: A list of permissions or roles assigned to the user.
        accounts: A list of account identifiers associated with the user.
    """

    username: str
    email: Optional[str] = None
    full_name: Optional[str] = None
    disabled: Optional[bool] = None
    user_id: str
    org_id: str
    org_name: str
    permissions: list[str] = []
    accounts: list[str] = []

    @classmethod
    def get_test_user(cls) -> 'LoggedInUser':
        """
        Creates and returns an instance of LoggedInUser with predefined data.
        Returns:
            An instance of LoggedInUser with predefined data.
        """
        return cls(
            user_id='1',
            org_id='test_org',
            org_name='test_org',
            username='test_user',
        )


class ServiceType(str, enum.Enum):
    """
    This enum describes type of object in processing system
    Options:
        FILE: for services with main type of objects is file(Amazon S3, GitHub, GitLab, BitBucket)
        DATABASE: for services with main type of objects is database(Snowflake,Amazon: Redshift, RDS, DocumentDB,
        DynamoDB)
    """

    FILE = 'file'
    DATABASE = 'database'


class SupportedServices(str, enum.Enum):
    """
    This enum class categorizes different services each associated with a specific service type.
    It helps in identifying and processing data across various services.

    Options:
        S3: Represents Amazon Simple Storage Service (S3).
        REDSHIFT: Represents Amazon Redshift.
        SNOWFLAKE: Denotes Snowflake's cloud data platform.
        RDS: Represents Amazon Relational Database Service (RDS).
        DynamoDB: Denotes Amazon DynamoDB, a NoSQL database service.
        DocumentDB: Represents Amazon DocumentDB.
        GitHub: Represents GitHub repository's branch.
        BitBucket: Denotes BitBucket repository's branch.
        GitLab: Represents GitLab repository's branch.

    Each option is a tuple containing the native name of the resource and its service type.
    """

    S3 = ('SimpleStorageService', ServiceType.FILE)
    REDSHIFT = ('RedshiftCluster', ServiceType.DATABASE)
    SNOWFLAKE = ('SnowflakeDatabases', ServiceType.DATABASE)
    RDS = ('RelationalDatabaseService', ServiceType.DATABASE)
    DynamoDB = ('DynamoDB', ServiceType.DATABASE)
    DocumentDB = ('DocumentDBCluster', ServiceType.DATABASE)
    GitHub = ('GitHubBranch', ServiceType.FILE)
    BitBucket = ("BitBucketBranch", ServiceType.FILE)
    GitLab = ("GitLabBranch", ServiceType.FILE)

    def __new__(cls, native_resource: str, type: ServiceType) -> 'SupportedServices':
        """
        Creates a new instance of the SupportedServices enum.

        Overrides the standard enum instance creation to include additional attributes.
        Each enum instance will have a 'type' attribute indicating the type of the service's objects.

        Args:
            native_resource: The native name or identifier of the service.
            type: The type of the service (e.g., FILE, DATABASE).

        Returns:
            obj: A new instance of the SupportedServices enum.
        """
        obj = str.__new__(cls, native_resource)
        obj._value_ = native_resource
        obj.type = type
        return obj

    def is_aws(self) -> bool:
        """
        This method checks if the service is one of the AWS-specific services like S3, Redshift, RDS, etc.

        Returns:
            bool: True if the service is an AWS service, False otherwise.
        """
        return self.value in (
            self.S3.value,
            self.REDSHIFT.value,
            self.RDS.value,
            self.DynamoDB.value,
            self.DocumentDB.value,
        )
