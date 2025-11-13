import enum
from typing import Any, Optional

from pydantic import BaseModel, root_validator, validator

from app.schemas.common import AuditBase, AuditInfo, Category


class SensitivityCategory(str, enum.Enum):
    """
    Enumeration defining categories of data sensitivity, each associated with a specific weight.

    The weight value is used to quantify the sensitivity level, aiding in decision-making processes like data handling
    and access control.

    Methods:
        __new__: Custom constructor for creating enum instances with additional attributes.
    """

    RESTRICTED = ('Restricted', 1.0)
    CONFIDENTIAL = ('Confidential', 0.75)
    PUBLIC = ('Public', 0.5)
    INTERNAL = ('Internal', 0.25)

    def __new__(cls, category: str, weight: float) -> 'SensitivityCategory':
        """
        Custom constructor for creating enum instances with additional attributes.
        Args:
            category: sensitivity category
            weight: sensitivity level in numerical equivalent
        Returns:
            obj: instance of SensitivityCategory
        """
        obj = str.__new__(cls, category)
        obj._value_ = category
        obj.weight = weight
        return obj


class SensitivityLevel(str, enum.Enum):
    """
    This enum class categorizes data based on the degree of sensitivity, which is crucial for determining the level of
    security and handling required. The sensitivity levels range from low to critical.
    """

    LOW = 'Low'
    MEDIUM = 'Medium'
    HIGH = 'High'
    CRITICAL = 'Critical'


class DataClassifierType(str, enum.Enum):
    """
    This enum class represents type of classifier
    """

    FILENAME = 'Filename'
    REGEX = 'Data'

    def __str__(self) -> str:
        """
        Returns a string representation of the type of data_classifier.
        """
        return self.value


class DataClassifiersFilter(BaseModel):
    """
    This class is used to specify filtering parameters for querying or organizing data classifiers, based on the
    classifier type.

    Attributes:
        type: The type of data classifier to filter by. This can be either 'Filename'
        or 'Data'.
    """

    type: Optional[DataClassifierType]

    class Config:
        """
        This class defines how enum fields are handled during serialization and parsing.

        Attributes:
            use_enum_values: When set to True, enum fields in the model are serialized and parsed using their values
            instead of their names.
        """

        use_enum_values = True


class DataClassifiersEngine(str, enum.Enum):
    MITIE = 'MITIE'
    HYPERSCAN = 'HYPERSCAN'
    RE2 = 'RE2'
    RE = 'RE'


class DataClassifiersCreate(BaseModel):
    """
    This class extends ConfigMetadata to include specific properties required for defining a new data classifier.
    It is used to specify the characteristics and behavior of the classifier being created.

    Attributes:
        description: An optional description of the data classifier.
        category: The category of data classification, such as 'include' or 'exclude'.
        is_enabled: Indicates whether the classifier is active.
        type: The type of the classifier, such as 'Filename' or 'Data' (using regex).
        labels: An optional list of labels associated with the classifier.
        sensitivity_category: The category of data sensitivity, with a default value corresponding to 'Internal'.
        sensitivity_level: The level of data sensitivity, defaulting to 'Low'.
    """

    read_name: Optional[str]
    engine: Optional[str]
    name: str
    patterns: Optional[list[str]]
    description: Optional[str]
    category: Category
    is_enabled: bool = True
    type: DataClassifierType
    labels: Optional[list[str]]
    sensitivity_category: SensitivityCategory = SensitivityCategory.INTERNAL.value
    sensitivity_level: SensitivityLevel = SensitivityLevel.LOW.value  # type: ignore


class DataClassifiers(AuditInfo, DataClassifiersCreate):
    """
    This class inherits from AuditBase and DataClassifiersCreate, combining audit details (like creation and update
    timestamps) with the specific properties required for data classifiers.

    Attributes:
        service_ids: An optional list of account IDs associated with the data classifier.
    """

    class Config:
        """
        This class defines how enum fields are handled during serialization and parsing.

        Attributes:
            use_enum_values: When set to True, enum fields in the model are serialized and parsed using their values
            instead of their names.
        """

        use_enum_values = True

    @root_validator  # type: ignore
    def validate_type_category(cls, model_values: Any) -> Any:
        """
        Root validator method for validating the combination of 'type' and 'category' fields in the model.

        This method checks if the combination of 'type' and 'category' fields in the model is valid.
        Specifically, it ensures that the 'Category.EXCLUDE' is not used with 'DataClassifierType.REGEX'(data).

        Args:
            model_value: A dictionary containing the values of the model fields(In most cases type and category).

        Returns:
            The original model_values if the validation passes.
        """
        type = model_values.get('type')
        category = model_values.get('category')
        if type == DataClassifierType.REGEX.value and category == Category.EXCLUDE.value:
            raise ValueError('Category Exclude is not allowed for type Data')
        return model_values


class DataClassifiersUpdate(BaseModel):
    """
    This class is needed for update data_classifier.
    It has the same fields as data_classifiers except for: 'service_id' is string(not list) and 'type' field does
    not exist
    """

    name: Optional[str]
    is_enabled: Optional[bool]
    patterns: Optional[list[str]]
    labels: Optional[list[str]]
    service_id: Optional[str]
    sensitivity_category: Optional[SensitivityCategory]
    sensitivity_level: Optional[SensitivityLevel]


class DataClassifierFilters(BaseModel):
    """
    A model representing filter criteria for querying data classifiers.

    This class is used to specify various filter parameters for the search data classifiers based on
    different attributes.
    Some attributes similar to data_classifiers but some of them differ:
    1) pattern_names instead of patterns
    2) 'service_id' is string(not list) and 'type' field does
    not exist
    3) 'description' does not exist
    4) 'labels' does not exist

    """

    type: Optional[DataClassifierType] = None
    pattern_names: Optional[list[str]] = None
    category: Optional[Category] = None
    is_enabled: Optional[bool] = None
    sensitivity_category: Optional[SensitivityCategory] = None
    sensitivity_level: Optional[SensitivityLevel] = None
