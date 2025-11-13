from typing import Optional

import re2  # type: ignore

from app.schemas import PatternRecognizer


class Re2Service:
    def __init__(self, recognizers: Optional[list[PatternRecognizer]] = None) -> None:
        self.recognizers = recognizers

    @staticmethod
    def extract_entity(text: str, recognizer: PatternRecognizer) -> list[tuple[int, str]]:
        """
        Take recognizer and matches of this pattern in the text
        Args:
            text: the text that should be analyzed
            recognizer: PatternRecognizer object that contains data_type name, creator and pattern
        Returns:
            a list of tuples - matches that were found in the text per recognizer
        """
        analyzer_results: list[tuple[int, str]] = []
        for match in re2.finditer(recognizer.patterns[0], text):  # type: ignore
            value = match.group()
            analyzer_results.append((recognizer.id, value))
        return analyzer_results

    def extract_entities(self, text: str) -> list[tuple[int, str]]:
        """
        Take list of recognizers and run extract_entity with each recognizer separately
        Args:
            text: the text that should be analyzed
        Returns:
            list of tuples - matches that were found in the text for all recognizers
        """
        analyzer_results: list[tuple[int, str]] = []
        for recognizer in self.recognizers:  # type: ignore
            analyzer_result = self.extract_entity(text=text, recognizer=recognizer)
            analyzer_results.extend(analyzer_result)
        return analyzer_results
