from datetime import datetime

from pydantic import BaseModel

from app.schemas import SupportedServices


class InfoBase(BaseModel):
    """
    Base model of trend that represents time when trend was created
    """

    created_at: datetime


class DataSources(InfoBase):
    """
    Model extends InfoBase and represents the amount of data sources
    """

    source_count: int


class RegionsNumber(InfoBase):
    """
    Model extends InfoBase and represents the amount of regions
    """

    region_count: int


class SensitiveData(InfoBase):
    """
    Model extends InfoBase and represents the amount of sensitive data
    """

    data_count: int


class TrendDataInfo(BaseModel):
    """
    Prefabricated model which includes amount of data sources, amount of regions, amount of sensitive data
    """

    data_sources_trend: list[DataSources]
    regions_trend: list[RegionsNumber]
    sensitive_data_trend: list[SensitiveData]


class ServiceSources(BaseModel):
    """
    Model that represents the amount of records with selected service
    """

    service: SupportedServices
    source_count: int


class DataSourcesWithData(BaseModel):
    """
    Model that represents the list of ServiceSources and it's total
    """

    service_sources: list[ServiceSources]
    total: int
