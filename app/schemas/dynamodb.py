from typing import Optional

from pydantic import BaseModel


class DynamoDBInputData(BaseModel):
    """
    A model representing general information of specific to an Amazon DynamoDB source.

    This class is designed to store and manage general information about Amazon DynamoDB source.

    Attributes:
       source_name: The name of the DynamoDB source (DynamoDB table name).
       source_region: The region where the DynamoDB table is located
       source_owner: Creator of the DynamoDB table
       source_UUID: The UUID(individual identifier) of the DynamoDB
    """

    source_name: str
    source_region: Optional[str]
    source_owner: Optional[str]
    source_UUID: Optional[str]

    def __str__(self) -> str:
        """
        Returns a string representation of the source(DynamoDB table).

        This method provides a human-readable format of the source(DynamoDB table) metadata.

        Returns:
            A string representation of the DynamoDB source (table).
        """
        return self.source_name
