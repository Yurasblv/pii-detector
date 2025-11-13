from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class DocumentDBInputData(BaseModel):
    """
    A model representing input data for a DocumentDB instance.

    This class is designed to hold general metadata details of specific database in Amazon DocumentDB instance.

    Attributes:
        cluster_name: The name of the DocumentDB cluster. Useful for identification and reference within an application.
        endpoint: The connection endpoint for the DocumentDB cluster.
        port: The port number used to connect to the DocumentDB cluster.
        created_at (Optional[datetime]): The date and time when the DocumentDB instance was created.
        master_username (Optional[str]): The username for the master user of the DocumentDB cluster.
    """

    cluster_name: str
    endpoint: Optional[str]
    port: Optional[str]
    master_username: Optional[str]
    source_region: Optional[str]
    created_at: Optional[datetime]
    source_UUID: Optional[str]

    def __str__(self) -> str:
        """
        Returns the string representation of the DocumentDBInputData instance.

        Overrides the default string representation to return the cluster name of the DocumentDB instance.

        Returns:
            The cluster name of the DocumentDB instance
        """
        return self.cluster_name  # type: ignore
