from datetime import datetime
from typing import Optional

from pydantic import BaseModel, validator


class RedshiftInputData(BaseModel):
    """
    A model representing general metadata specific to Redshift source(database).
    This class is designed to store and manage general information about Redshift table, such as cluster, db_user,
    db_name, cluster_creation_date
    """

    cluster: str
    db_user: str
    db_name: str
    owner: Optional[str]
    region: Optional[str]
    source_UUID: Optional[str]

    def __str__(self) -> str:
        """
        Returns a string representation of the source(Redshift database).

        This method provides a human-readable format of the source(Redshift database) metadata, primarily focusing on
        the cluster.

        Returns:
            A string representation of the Redshift source (cluster).
        """
        return self.cluster


class RedshiftResult(BaseModel):
    """
    This class is designed to hold and structure the results from a database query, representing columns and records of
    the Redshift table.

    Attributes:
        columns: A list of column names in the Redshift table. These names correspond to the fields returned in the
        query.
        records: A list of string which represents a record from the Redshift table.
    """

    columns: list[str] = []
    records: list[str] = []

    @validator('records', pre=True)
    def strip_backspaces(cls, record: list[str]) -> list[str]:
        """
        This method iterates through the list of records and trims any trailing spaces from string elements.

        Args:
            record: A list containing elements (records) to be validated and cleaned.

        Returns:
             he cleaned list with trailing spaces removed from string elements.
        """
        return [rec.strip() if isinstance(rec, str) else rec for rec in record]
