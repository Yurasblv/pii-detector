from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel


class ActivityStatus(str, Enum):
    """
    This class describes status of scanner
    """

    ACTIVE = "Active"
    INACTIVE = "Inactive"


class Instances(BaseModel):
    """
    This class is designed to store key information about instance(scanner)

    Attributes:
        instance_id: The unique identifier of the instance.
        account_id: The identifier of the account under which the instance is running.
        region: The geographic or cloud region where the instance is located. Default is None.
        first_seen: The timestamp when the instance was first detected. Default is None.
        last_seen : The timestamp of the last observation of the instance. Default is None.
        next_scan : The scheduled timestamp for the next scan by the instance. Default is None.
        status: The current operational status of the instance, e.g., active or inactive.
        Defaults to ActivityStatus.ACTIVE.
    """

    instance_id: str
    account_id: str
    region: Optional[str]
    first_seen: Optional[datetime]
    last_seen: Optional[datetime]
    next_scan: Optional[datetime]
    status: Optional[ActivityStatus] = ActivityStatus.ACTIVE


class InstancesUpdate(BaseModel):
    """
    A model for updating the information about instance(scanner).
    This class has similar attributes to Instances but all of them are optional
    """

    account_id: Optional[str]
    region: Optional[str]
    first_seen: Optional[datetime]
    last_seen: Optional[datetime]
    instance_id: Optional[str]
    next_scan: Optional[datetime]
    status: Optional[ActivityStatus]
