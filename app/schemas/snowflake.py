from enum import Enum
from typing import Optional

from pydantic import BaseModel, validator

from app.schemas.common import AuditInfo, Base


class SnowflakeConfig(BaseModel):
    """
    A configuration model for connecting to a Snowflake.

    This class holds the connection details for establishing a connection to a Snowflake
    account.

    Attributes:
        login: The username used for logging into the Snowflake account.
        account: The Snowflake account identifier, typically consisting of an organization name and possibly a region or
        cloud platform identifier.
        account_id: An optional, more specific identifier for the Snowflake account.
        region: The geographic region or cloud region where the Snowflake account is hosted.
        encrypted_password: An optional encrypted password used for authentication.
        encrypted_private_key: An optional encrypted private key used for key-pair authentication, providing an
        additional security layer.
    """

    login: str
    account: str
    account_id: Optional[str]
    region: Optional[str]
    encrypted_password: Optional[str]
    encrypted_private_key: Optional[str]


class AccountState(Enum):
    """
    Enumeration defining the possible states of an account in a system.

    This enum class categorizes the status of an account, indicating whether it is active, has authentication issues,
    or is disabled.
    """

    ACTIVE = 'Active'
    AUTHENTICATION_FAILED = 'Authenticated Failed'
    DISABLED = 'Disabled'


class UserCommonFields(BaseModel):
    """
    A base model representing common fields associated with a user entity for Snowflake.

    Attributes:
        login: The login identifier for the user.
        account: The account name or identifier associated with the user.
        account_id: An optional, more specific identifier for the user's account. Defaults to None.
        name: The name of the user.
        type: The type of account, with a default value of 'Snowflake'.
        account_state: The current state of the account, represented using the AccountState enum.
        Defaults to AccountState.ACTIVE.
    """

    login: str
    account: str
    account_id: Optional[str] = None
    name: Optional[str] = None
    type: str = 'Snowflake'
    account_state: Optional[AccountState] = AccountState.ACTIVE

    class Config:
        """
        Configures the model to use the values of enum members rather than their names.
        """

        use_enum_values = True

    @validator('account_state')
    def convert_state(cls, state: str) -> str:
        return AccountState(state).name


class SnowflakeUser(Base, UserCommonFields, AuditInfo):
    """
    A model with basic credentials for connecting to a Snowflake.

    This class holds the necessary credentials for establishing a connection to a Snowflake account.

    Attributes:
        encrypted_password: An optional encrypted password used for authentication.
        encrypted_private_key: An optional encrypted private key used for key-pair authentication, providing an
        additional security layer.
    """

    encrypted_password: Optional[str] = None
    encrypted_private_key: Optional[str] = None


class SnowFlakeInputData(BaseModel):
    """
    A model representing general information of specific to a Snowflake table.

    This class is designed to store and manage general information about Snowflake table.

    Attributes:
       source_name: The name of the Snowflake source (Snowflake table name).
       source_region: The region where the Snowflake table is located
       source_owner: Creator of the Snowflake table
       source_UUID: The UUID(individual identifier) of the Snowflake source
    """

    source_name: str
    source_region: Optional[str]
    source_owner: Optional[str]
    source_UUID: Optional[str]

    def __str__(self) -> str:
        """
        Returns a string representation of the source(Snowflake table).

        This method provides a human-readable format of the source(Snowflake table) metadata.

        Returns:
            A string representation of the Snowflake source (table).
        """
        return self.source_name
