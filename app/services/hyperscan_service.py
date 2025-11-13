import re
from typing import Optional

import hyperscan  # type:ignore
from loguru import logger

from app.core.regex_patterns import regex
from app.schemas import PatternRecognizer


class HyperScanService:
    def __init__(self, recognizers: Optional[list[PatternRecognizer]] = None) -> None:
        self.recognizers = recognizers
        self.db = None

    def compile_hyperscan_patterns(self) -> None:
        """
        Compiles regex patterns into a Hyperscan database for efficient matching.
        """
        try:
            if self.recognizers:
                db = hyperscan.Database()
                all_expressions = {r.id: r.patterns[0].encode('utf-8') for r in self.recognizers}  # type:ignore
                db.compile(
                    expressions=[exp for exp in all_expressions.values()],
                    ids=[key for key in all_expressions.keys()],
                    flags=[hyperscan.HS_FLAG_SOM_LEFTMOST] * len(all_expressions.keys()),
                )

                self.db = db
        except Exception as e:
            logger.info(f'Error compiling hyperscan db: {e}')

    def extract_entities(self, text: str, id_mapper_name: dict[int, str]) -> list[tuple[int, str]]:
        """
        Extracts entities from the provided text based on the compiled Hyperscan patterns.

        Args:
            text: text to be analyzed
            id_mapper_name: mapper to get name of founded pattern
        Returns:
            list of tuples containing the ID of the matched pattern and the extracted entity.
        """
        results: dict[tuple[int, str], tuple[int, str]] = {}

        def __match_event_handler(_id, start, end, flags, context):  # type: ignore
            """
            Internal callback function for Hyperscan to handle match events.

            Args:
                _id: ID of the matching pattern.
                start: Start index of the match in the text.
                end: End index of the match in the text.
                flags: Hyperscan flags for the match.
                context: Contextual information for the match (not used here).
            """
            if id_mapper_name.get(_id, '') in regex.credentials_patterns.keys() and re.search(
                regex.SECRET_EXCLUDE, text[start:end], flags=re.IGNORECASE
            ):
                return None
            # get the biggest string which can hyperscan recognize
            results[(_id, start)] = (_id, text[start:end])

        try:
            self.db.scan(text.encode('utf-8'), __match_event_handler)
        except Exception as e:
            logger.warning(f"{e}")
        return list(results.values())
