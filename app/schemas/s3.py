from typing import Optional

from pydantic import BaseModel


class S3InputData(BaseModel):
    """
    A model representing general information of specific to an Amazon S3 source.

    This class is designed to store and manage general information about Amazon S3 bucket, such as its name and
    creation date.

    Attributes:
        source_name: The name of the Amazon S3 source (S3 bucket name).
        source_region: The region where the S3 bucket is located
        source_owner: Creator of the Amazon S3 source
        source_UUID: The UUID(individual identifier) of the Amazon S3 source
    """

    source_name: str
    source_region: Optional[str]
    source_owner: Optional[str]
    source_UUID: Optional[str]

    def __str__(self) -> str:
        """
        Returns a string representation of the source(S3 bucket) name.

        This method provides a human-readable format of the source(S3 bucket) metadata, primarily focusing on
        the bucket name.

        Returns:
            A string representation of the S3 source (bucket).
        """
        return self.source_name
