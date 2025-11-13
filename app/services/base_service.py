from abc import ABC, abstractmethod
from typing import Any

from app.schemas import ObjectContents


class BaseService(ABC):

    @abstractmethod
    async def fetch_data(
        self,
        fetch_path: str,
        chunk_path: str,
        limit: int,
        offset: int,
    ) -> Any:
        """
        Fetching data from table object by fetch path, limit and offset.
         If no limit and offset, we retrieve data for all rows from table.

        Args:
            fetch_path: path for scanning object
            chunk_path: path to scanning chunk
            limit: value which contain number of data which must be retrieved from object
            offset: value which contain start position to retrieve data from object

        Returns:
            Any of type which can include data or return None instead
        """
        ...

    @abstractmethod
    async def exclude_redundant_objects(self, objects: list[ObjectContents]) -> list[ObjectContents]:
        """
        Method for excluding specific objects.

        Args:
            objects: objects: objects that were founded in source in ObjectContents format

        Returns:
            list with objects that satisfy conditions
        """
        ...

    @abstractmethod
    async def get_objects_by_source(self) -> list[ObjectContents]:
        """
        Method for retrieve all objects that contains in self.source.

        Returns:
            list with objects that exists in self.source.
        """
        ...

    @abstractmethod
    async def get_source_configuration(self, *args, **kwargs) -> Any:  # type: ignore[no-untyped-def]
        """
        Set input schema for connecting specific source.
        """
        ...
