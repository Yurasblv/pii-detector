from datetime import datetime
from typing import Any, Optional

from pydantic import BaseModel


class PatternRecognizer(BaseModel):
    """
    Defines patterns for classifier and classifier name
    """

    id: int
    name: str
    patterns: Optional[list[str]] = []


class AnalyzerAttributes(BaseModel):
    hyperscan_recognizers: list[PatternRecognizer] = []
    re2_recognizers: list[PatternRecognizer] = []
    re_recognizers: list[PatternRecognizer] = []
    id_name_mapper: dict[int, str] = {}
    labels_mapper: dict[int, list[str]] = {}
    latest_data_type: Optional[datetime] = None

    def create_id_name_mapper(self) -> None:
        recognizers = self.hyperscan_recognizers + self.re2_recognizers + self.re_recognizers
        self.id_name_mapper = {**{0: 'PERSON'}, **{r.id: r.name for r in recognizers}}  # noqa
        return None

    def dict(self, *args, **kwargs) -> dict[str, Any]:  # type: ignore
        return {
            'hyperscan_recognizers': self.hyperscan_recognizers,
            're2_recognizers': self.re2_recognizers,
            're_recognizers': self.re_recognizers,
            'id_name_mapper': self.id_name_mapper,
            'labels_mapper': self.labels_mapper,
        }
