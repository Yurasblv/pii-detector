import enum
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, validator

from app.schemas.common import AuditBase, Base, Category, SupportedServices
from app.services.utils.mappings import repositories_mapper


class DataClassificationType(str, enum.Enum):
    """
    This enum class provides a way to categorize data based on its origin or nature, particularly distinguishing between
    'source' data and 'object' data. They have different processing ways.
    Options:
        SOURCE: Represents data that originates from a specific source(e.g, GitLab branch, S3 bucket).
        OBJECT: Denotes data that represents an object(e.g, file, table)
    """

    SOURCE = 'source'
    OBJECT = 'object'

# Data Classification


class DataClassification(Base):
    """
    A model representing the classification of data in a data processing or management system.

    This class describe source or object that should be scanned

    Attributes:
        data_sources: A list of identifiers for the data sources to be scanned.
        data_objects: A list of identifiers for specific data objects within the data sources. Optional parameter, if it
        None all objects for specific sources will be scanned
        account_email: The email address associated with the account responsible for the data.
        category: The category of data classification,
                  indicating whether the data is to be included or excluded in certain operations.
        service: The service associated with the data, indicating where the data is stored or managed.
        scanning_period_minutes: The frequency, in minutes, at which the data should be scanned again.
        Defaults to 15 minutes.
        data_classification_group_id: An optional UUID that uniquely identifies a group of data classifications.
    """

    data_sources: Optional[list[str]]
    data_objects: Optional[list[str]]
    account_email: Optional[str]
    category: Category
    service: SupportedServices
    scanning_period_minutes: int = 15
    data_classification_group_id: Optional[UUID]

    def __hash__(self) -> int:
        """
        Returns a hash value for the DataClassification id.
        """
        return self.id.__hash__()

    class Config:
        """
        This class defines how enum fields are handled during serialization and parsing.

        Attributes:
            use_enum_values: When set to True, enum fields in the model are serialized and parsed using their values
            instead of their names.
        """

        use_enum_values = True


# Data Classification Group


class DataClassificationGroupBase(BaseModel):
    """
    This class describes group of classifications. It contains general information related to services, environments,
    and scanners.

    Attributes:
        name: The name of the classification group.
        service_ids: List of account IDs related to the service. Defaults to an empty list.
        environment_id: The UUID of the environment to which this group belongs.
        last_scanned: The timestamp of the last scan performed on this group.
        scanner_ids: List of scanner IDs associated with this group.
        scanner_environment_id: The UUID of the environment in which the scanners are located.
        scanner_account_id: The UUID of the account under which the scanners operate.
    """

    name: str
    service_ids: list[str] = []
    environment_id: Optional[UUID]
    last_scanned: Optional[datetime]
    scanner_ids: Optional[list[str]]
    scanner_environment_id: Optional[UUID]
    scanner_account_id: Optional[UUID]


class DataClassificationGroupRead(AuditBase, DataClassificationGroupBase):
    """
    This class extends both AuditBase, which provides audit-related fields (like creation and update timestamps),
    and DataClassificationGroupBase, which provides the base structure for a data classification group.
    It is specifically used for reading and processing groups of data classifications.

    Attributes(it extends attributes of DataClassificationGroupBase and AuditBase classes):
        data_classifications: A list of DataClassification objects representing the classifications within the group.
    """

    data_classifications: list[DataClassification]

    @validator('data_classifications', pre=True)
    def validate_service(cls, classifications: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Validates and updates the 'service' field in data classification dictionaries.
        If service belong to repositories(GitHub, GitLab, BitBucket services), repository source transforms into branch
        source otherwise the service itself

        Args:
            classifications: data classifications to validate.

        Returns:
            Updated classifications with validated 'service' fields.
        """
        for classification in classifications:
            service = classification['service']
            classification['service'] = repositories_mapper.get(service, service)
        return classifications


class DataClassificationGroup(AuditBase, DataClassificationGroupBase):
    """
    This class extends both AuditBase, which provides audit-related fields (like creation and update timestamps),
    and DataClassificationGroupBase, which provides the base structure for a data classification group.
    It is specifically used for reading and processing groups of data classifications.

    Attributes(it extends attributes of DataClassificationGroupBase and AuditBase classes):
        data_classifications: A list of DataClassification objects representing the classifications within the group.
    """

    data_classifications: list[DataClassification]


class UpdateDataClassification(BaseModel):
    """
    Model for updating the scanning schedule and status of a data classification.

    Attributes:
        account_id: The unique identifier of the account associated with the data classification.
        next_scan: The scheduled timestamp for the next scan. Default is None.
        last_scanned: The timestamp of the last completed scan. Default is None.
    """

    account_id: str
    next_scan: Optional[datetime] = None
    last_scanned: Optional[datetime] = None


class DataClassificationSourcesResponse(BaseModel):
    scanning_period_minutes: int = 15
    sources: list[dict[str, Any]] = []
