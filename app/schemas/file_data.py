import enum
import json
import uuid
from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, validator

from app.schemas.common import Base, SupportedServices
from app.schemas.data_classifiers import DataClassifiers


class FileStatus(str, enum.Enum):
    """
    Enumeration for representing the various statuses of a file in a data processing system.
    This enumeration is typically used to track and manage the state of files within the system, allowing for
    organized processing workflows and status-based actions.

    Statuses:
        IGNORED: Indicates that the object is ignored and not considered for further processing.
        WAIT_FOR_SCAN: The object is queued and waiting to be scanned.
        WAIT_FOR_RESCAN: The object is queued for rescanning, usually after an update or change.
        SCANNED: Indicates that the object has been successfully scanned.
        IN_PROGRESS: The object is currently being processed or scanned.
        SKIPPED: If object cannot be processed due to big size we skip it
        FAILED: If file cannot be processed due to error
    """

    IGNORED = 'Ignored'
    WAIT_FOR_SCAN = 'Wait for scan'
    RESCAN_IN_PROGRESS = 'Rescan in progress'
    SCANNED = 'Scanned'
    IN_PROGRESS = 'In progress'
    SKIPPED = 'Skipped'
    FAILED = 'Failed'


class ObjectAclType(int, enum.Enum):
    """
    Enumeration defining access control levels for an object in a data processing or storage system.

    This enumeration is typically used in systems where access control needs to be explicitly managed and controlled,
    and is often part of security and privacy management policies.

    Types:
        READ: Grants permission to read the object's data.
        WRITE: Grants permission to modify or write data to the object.
        READ_ACP: Grants permission to read the object's access control policies.
        WRITE_ACP: Grants permission to modify the object's access control policies.
    """

    READ = 0
    WRITE = 1
    READ_ACP = 2
    WRITE_ACP = 3


class ObjectAcl(BaseModel):
    """
    A model representing the access control list (ACL) settings of an object in a data storage or processing system.

    This class defines the access control configurations for an object, specifying whether it is publicly accessible,
    identifying the bucket owner, and detailing the specific types of permissions that are granted.

    Attributes:
        is_public: A bool flag indicating whether the object is publicly accessible. Defaults to False.
        bucket_owner: The identifier (such as a username or account ID) of the owner of the bucket in which the
        object is stored. Defaults to an empty string.
        permission_types: A set of permission types associated with the object, indicating what operations
        (read, write, etc.) are allowed. Defaults to an empty set.
    """

    is_public: bool = False
    permission_types: set[ObjectAclType] = set()


class ObjectRead(BaseModel):
    """
    A model representing basic information about an object, typically used in data processing and storage system.

    This class provides a simplified view of an object, focusing on its essential attributes like the full path, object
    name, and size. It's commonly used for initial read operations where basic details of the object are required.

    Attributes:
        full_path: The full path of the object, which typically includes the directory path and the object name.
        object_name: The name of the object.
        size: The size of the object in bytes.
    """

    full_path: str
    object_name: str
    size: int


class FileData(Base):
    """
    A model representing a specific instance of personally identifiable information (PII) found within a file.

    This class is used to store detailed information about a piece of PII detected in the object.

    Attributes:
        file_name: The name of the file where the PII was found.
        pii_type: The type of PII detected ('email', 'phone number', 'medical license').
        pii_data: The actual PII content detected in the file.
        pii_region: The specific region where the PII was found.
        score: A confidence score indicating the likelihood that the detected data is PII.
        is_custom_regex: Indicates if a custom regular expression was used to detect this PII.
        column_name: The name of the column in the file where the PII was found, applicable for tables.
        metadata_id: A reference to the metadata record associated with this instance of PII.
    """

    file_name: str
    pii_type: str
    pii_data: str
    pii_region: Optional[str]
    score: float
    is_custom_regex: Optional[bool]
    column_name: Optional[str]
    metadata_id: Optional[UUID]
    pii_hash: Optional[str]
    chunk_id: Optional[UUID]


class DataChunk(Base):
    """
    A model representing a chunk of data associated with a file or object in a data processing system.

    This class is used to define and store information about a specific part of a larger data object, facilitating
    the handling of large files by breaking them down into chunks for processing.

    Attributes:
        object_name: The name of the object or file to which this data chunk belongs.
        fetch_path: The specific path used to fetch or locate this chunk of data.
        offset: The starting point in the data object from which this chunk begins.
        limit: The maximum size or limit of this data chunk, often defining how much data is contained in the chunk.
        scanned_at: The datetime when this data chunk was last scanned.
        status: The current processing status of this data chunk. Defaults to WAIT_FOR_SCAN.
        hash: A unique hash value representing content of this specific chunk of data.
        instance_id: An identifier for the particular instance or processing job associated with this data chunk.
    """

    object_name: Optional[str]
    fetch_path: Optional[str]
    offset: Optional[str]
    limit: Optional[int]
    scanned_at: Optional[datetime]
    status: FileStatus = FileStatus.WAIT_FOR_SCAN
    hash: Optional[str]
    instance_id: Optional[str]
    labels: Optional[list[str]]
    latest_data_type: Optional[datetime]


class DataChunkBatchCreate(BaseModel):
    """
    metadata_id: id of record in file metadata table
    metadata_size: recalculated size of object when new chunks were added
    metadata_status: object status changes on wait for scan if were newly added chunks
    chunks: array of newly configured chunks for existing object
    """

    metadata_id: UUID
    metadata_size: int
    metadata_status: FileStatus
    chunks: list[DataChunk]

    @validator('metadata_id', pre=True)
    def validate_uuid_to_str(cls, value: UUID) -> str:
        """
        Validator to transform the 'metadata_id' attribute is a string.
        Args:
            value: id of metadata to which this data chunk belongs
        Returns:
            value transformed to string if value exists
        """
        return str(value)

    @validator('chunks')
    def validate_chunks(cls, value: list[DataChunk]) -> list[dict[str, Any]]:
        """
        Validator to convert 'chunks' attribute to a list of dictionaries.
        Args:
            value: list of data_chunks related to file metadata
        Returns:
            list of dict(converted chunks(DataChunk) into dict)
        """
        return [v.dict() for v in value]


class DataChunkBatchUpdate(BaseModel):
    """
    A model for updating a batch of data chunks associated with a specific metadata id.

    This class allows batch processing of data chunk updates.

    Attributes:
        metadata_id: The unique identifier of the metadata record associated with these data chunks.
        chunks: A list of chunk identifiers(object full_path, chunk offset, hash data chunk content)
        representing the data chunks to be updated.
    """

    metadata_id: UUID
    chunks: list[str]

    @validator('metadata_id', pre=True)
    def validate_to_str(cls, value: UUID) -> str:
        """
        Validator to transform the 'metadata_id' attribute is a string.
        Args:
            value: id of metadata to which this data chunk belongs
        Returns:
            value transformed to string if value exists
        """
        return str(value)


class DataChunkUpdate(BaseModel):
    """
    A model for updating the specific data chunk in a data processing system.

    This class is typically used for updating the attributes of a data chunk record in a database or data processing
    system.
    This model includes similar fields to DataChunk but also has additional fields like: id, sensitive_data, labels,
    metadata_id(fields that were added in process of scanning this chunk)
    """

    id: Optional[str] = None
    object_name: Optional[str] = None
    fetch_path: Optional[str] = None
    offset: Optional[str] = None
    limit: Optional[int] = None
    scanned_at: Optional[datetime] = None
    is_phi: Optional[bool] = None
    status: Optional[FileStatus] = None
    hash: Optional[str] = None
    sensitive_data: Optional[list[FileData]] = []
    metadata_id: Optional[str] = None
    instance_id: Optional[str] = None
    latest_data_type: Optional[datetime] = None

    @validator('id', pre=True)
    def validate_uuid_to_str(cls, value: UUID) -> str:
        """
        Validator to transform the 'id' to the string.
        Args:
            value: id of chunk that to be updated
        Returns:
            value transformed to string if value exists
        """
        return str(value)


class FileMetadataCommon(BaseModel):
    """
    This class stores various attributes related to a file, including its path, name, size, status,
    and other metadata.

    Attributes:
        file_full_path: The full path of the object.
        fetch_path: The path used for fetching the object.
        file_name: The name of the object.
        file_etag: The unique identifier of the object.
        file_size: The size of the object in bytes.
        labels: A list of labels associated with the object.
        source: The source from where the object originates(bucket, database, GitHub branch).
        account_id: The account ID of NDA
        service: The service that should be scanned.
        random_sampling: bool flag to indicate if random sampling is applied.
        scanned_at: The datetime when the object was last scanned.
        status: The current status of the object(Scanned, Wait for scan).
        is_public: bool flag indicating if the object is public. Defaults to False.
        resource_id: The resource identifier of the source(name of source).
        owner: The owner of the object.
        source_creation_date: The creation date of the object's source.
        object_creation_date: The creation date of the object.
        last_modified: The last modified date of the object.
        source_owner: The owner of the object's source.
        source_region: The region of the object's source.
        source_UUID: The UUID of the object's source.
        object_hash: The hash of the object.
        object_acl: The access control list of the object.
        scanner_id: The identifier of the scanner used.
        chunks: A list of data chunks related to the object.
    """

    file_full_path: Optional[str]
    fetch_path: Optional[str]
    file_name: str
    file_etag: str
    file_size: int
    labels: Optional[list[str]]
    source: str
    account_id: str
    service: str
    random_sampling: bool = False
    scanned_at: Optional[datetime]
    status: FileStatus
    is_public: bool = False
    resource_id: Optional[str]
    owner: Optional[str]
    source_creation_date: Optional[datetime]
    object_creation_date: Optional[datetime]
    last_modified: Optional[datetime]
    source_owner: Optional[str]
    source_region: Optional[str]
    source_UUID: Optional[str]
    object_hash: Optional[str]
    object_acl: Optional[list[ObjectAclType]]
    instance_id: Optional[str]
    chunks: Optional[list[DataChunk]]


class FileMetadata(Base, FileMetadataCommon):
    """A model representing metadata of a file in the data processing system."""

    @validator('source', pre=True)  # type: ignore
    def validate_source(cls, value: Any) -> str | Any:
        """
        Validator to transform the 'source' attribute is a string.
        Args:
            value: source that should be scanned
        Returns:
            value transformed to string if value exists
        """
        return str(value) if value else value

    @validator('chunks')
    def validate_chunks(cls, value: list[DataChunk]) -> list[dict[str, Any]]:
        """
        Validator to convert 'chunks' attribute to a list of dictionaries.
        Args:
            value: list of data_chunks related to file metadata
        Returns:
            list of dict(converted chunks(DataChunk) into dict)
        """
        return [v.dict() for v in value]


class FileMetadataCreate(FileMetadataCommon):
    """
    A subclass of FileMetadata designed for creating new file metadata entries.

    This class inherits from FileMetadata all attributes and is tailored for scenarios where new file metadata is being
    created and stored.
    """

    id: str

    def __init__(self, **data: Any) -> None:
        if 'id' not in data:
            data['id'] = str(uuid.uuid4())
        super().__init__(**data)


class FileMetadataRead(FileMetadata):
    """
    A subclass of FileMetadata specifically designed for reading file metadata.

    This class inherits from FileMetadata and is tailored for scenarios where file metadata is being read or
    retrieved, particularly from a data store or an API.
    The class inherits all attributes and methods from the FileMetadata class.
    """


class FileMetadataUpdate(BaseModel):
    """
    A model for updating existing file metadata entries.

    This class designed to represent the structure of a file metadata record for update operations.
    It includes various optional fields from FileMetadata that can be updated.
    also it contains id of metadata as identifier and chunk for update(separate specific chunk)
    """

    id: Optional[UUID] = None
    file_full_path: Optional[str] = None
    fetch_path: Optional[str] = None
    file_name: Optional[str] = None
    file_etag: Optional[str] = None
    file_size: Optional[int] = None
    source: Optional[str] = None
    account_id: Optional[str] = None
    service: Optional[SupportedServices] = None
    random_sampling: Optional[bool] = None
    labels: Optional[list[str]] = None
    scanned_at: Optional[datetime] = None
    status: Optional[FileStatus] = None
    owner: Optional[str] = None
    source_owner: Optional[str] = None
    source_region: Optional[str] = None
    source_UUID: Optional[str] = None
    object_hash: Optional[str] = None
    object_acl: Optional[list[ObjectAclType]] = None
    object_creation_date: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    instance_id: Optional[str] = None
    chunk: Optional[DataChunkUpdate] = None

    class Config:
        """
        orm_mode: Enables ORM mode for compatibility with SQL databases.
        """

        orm_mode = True

    @validator('source', pre=True)
    def validate_source(cls, value: str | UUID | None) -> Optional[str]:
        """
        Validator to transform the 'source' attribute is a string.
        Args:
            value: source that should be scanned
        Returns:
            value transformed to string if value exists
        """
        return str(value) if value else None

    @validator('chunk')
    def validate_chunks(cls, value: DataChunkUpdate) -> dict[str, Any]:
        """
        Validator to convert 'chunks' attribute to a list of dictionaries.
        Args:
            value: data_chunk related to file metadata that should be updated
        Returns:
            converted chunk(DataChunkUpdate) that should be updated into dict
        """
        return value.dict()  # type: ignore


class FileMetadataFilter(BaseModel):
    """
    A model for filtering file metadata based on various criteria.

    This class is used to define a set of parameters for filtering file metadata.
    It includes a range of optional fields that can be used as a search criteria for file metadata in a
    database or a data processing system.
    """

    ids: Optional[list[str | UUID]] = None
    limit: Optional[int] = None
    status: Optional[FileStatus] = None
    account_ids: Optional[list[str]] = None
    sources: Optional[list[str]] = None
    services: Optional[list[SupportedServices]] = None
    file_full_path: Optional[str] = None
    fetch_path: Optional[str] = None
    file_name: Optional[str] = None
    file_etag: Optional[str] = None
    file_size: Optional[int] = None
    not_ignored: Optional[bool] = None
    labels: Optional[list[str]] = None
    owner: Optional[str] = None
    source_owner: Optional[str] = None
    source_region: Optional[str] = None
    source_UUID: Optional[str] = None
    object_hash: Optional[str] = None
    chunk_status: Optional[FileStatus] = None


class DataChunkFilter(BaseModel):
    ids: Optional[list[str | UUID]] = None
    service: Optional[SupportedServices] = None
    account_ids: Optional[list[str]] = None
    instance_id: Optional[str] = None
    status: Optional[FileStatus] = None


class RescannedMetadataUpdate(BaseModel):
    """
    A model for updating metadata associated with a file that has been rescanned.

    Attributes:
        obj_in: An instance of FileMetadataUpdate containing the updated metadata values for the file.
        obj_id: The unique identifier of the file metadata object to be updated.
    """

    obj_in: FileMetadataUpdate
    obj_id: str


class ObjectContents(ObjectRead):
    """
    A model extending ObjectRead to represent detailed contents of an object in a data processing system.

    This class includes its service information, source, fetch path, and various metadata attributes.
    It is used to store and manage the data associated with an object during processing tasks.
    """

    account_id: Optional[str] = None
    service: str
    source: Optional[str]
    source_UUID: Optional[str]
    fetch_path: str
    etag: str
    labels: Optional[set[str]] = None
    status: Optional[FileStatus] = FileStatus.WAIT_FOR_SCAN
    is_public: Optional[bool] = False
    source_owner: Optional[str] = None
    chunk_last_data_type: Optional[datetime] = None  # chunk_last_data_type needs for rescan job
    owner: Optional[str] = None
    resource_id: str
    source_region: Optional[str] = None
    object_hash: Optional[str] = None
    object_acl: list[ObjectAclType] = []
    object_creation_date: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    source_creation_date: Optional[datetime] = None
    data: Optional[Any] = None
    data_chunks: list[DataChunk] = []
    current_chunk: Optional[DataChunkUpdate] = None

    @validator('source', pre=True)  # type: ignore
    def validate_source(cls, value: Any) -> str | Any:
        """
        Validator to transform the 'source' attribute is a string.
        Args:
            value: source that should be scanned
        Returns:
            value transformed to string if value exists
        """
        return str(value) if value else value

    @validator('object_creation_date', 'last_modified', 'source_creation_date', pre=True)
    def remove_timezone(cls, value: str | datetime) -> str | datetime:
        """Removing timezones which set by default by datetime library"""
        if isinstance(value, datetime):
            # Remove timezone information
            value = value.replace(tzinfo=None)
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace('Z', '+00:00')).replace(tzinfo=None)
        return value

    def __str__(self) -> str:
        """
        Returns a string representation of the object (often represented by its full path).
        """
        return self.full_path

    def __hash__(self) -> Any:  # make hashable BaseModel subclass
        """
        Returns a hash value for the object.

        This method enables instances of ObjectContents to be used in data structures that require hashable items,
        such as sets or dictionary keys.

        Returns:
            The hash value of the object, calculated based on the object's type and its full path.
        """
        return hash((type(self),) + tuple(self.full_path))

    class Config:
        """
        Configuration settings for the ObjectContents model.

        This configuration enables validation on assignment.
        It means when an attribute value is assigned, it will be validated according to the field's type and any
        defined validators.
        """

        validate_assignment = True


class RescanObjectResponse(BaseModel):
    data_types: list[DataClassifiers] = []
    rescan_object: ObjectContents
