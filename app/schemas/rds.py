import re
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, validator

rds_provider_mapper = {
    'postgres': 'postgresql',
    'aurora-postgresql': 'postgresql',
    'mariadb': 'mysql+pymysql',
    'mysql': 'mysql+pymysql',
    'aurora-mysql': 'mysql+pymysql',
}

class RDSInputData(BaseModel):
    """
    A model representing general metadata specific to RDS source(database).
    This class is designed to store and manage general information about RDS table, such as cluster, master_name,
    endpoint, port, creation_date
    """

    instance: Optional[str]
    cluster: Optional[str]
    master_name: Optional[str]
    name: Optional[str]
    endpoint: Optional[str]
    port: Optional[int]
    region: str
    engine: Optional[str]
    source_UUID: Optional[str]

    def __str__(self) -> str:
        """
        Returns a string representation of the source(RDS database).

        This method provides a human-readable format of the source(RDS database) metadata, primarily focusing on the
        instance.

        Returns:
            A string representation of the RDS source (instance).
        """
        return self.instance  # type: ignore

    @validator('region')
    def validate_region(cls, v: str) -> str:
        """
        This method performs validation on the 'region' field by removing any non-digit characters at the end of the
        string.
        This validator is automatically applied to the 'region' field of an InstancesUpdate instance during its
        creation or modification, enforcing the required format for the region value.

        Args:
            v (str): The value of the 'region' field to be validated and processed.

        Returns:
            str: The processed 'region' string with non-digit characters removed from its end.
        """
        return re.sub(r'\D$', '', v)

    @validator('engine')
    def validate_engine(cls, v: str) -> str:
        """
        This method checks and potentially maps the 'engine' field value to a corresponding value defined in the
        rds_provider_mapper. If the provided engine value exists in the mapper, it is replaced with the mapped value;
        otherwise, the original value is retained.

        Args:
            v: The engine value to be validated and mapped.

        Returns:
            The mapped engine value if it exists in the rds_provider_mapper, or the original value if it does not.
        """
        return rds_provider_mapper.get(v, v)


class RDSTablesResults(BaseModel):
    """
    This class is designed to hold and structure the results from a database query, representing columns and records of
    the RDS table.

    Attributes:
        columns: A list of column names in the RDS table. These names correspond to the fields returned in the query.
        records: A list of tuples where each tuple represents a record from the RDS table.
    """

    columns: list[str] = []
    records: list[tuple[str]] = []
