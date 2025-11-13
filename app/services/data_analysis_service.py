import hashlib
import json
import re
from typing import Any, Generator, Optional, Union
from uuid import uuid4

import hyperscan  # type: ignore
import pandas as pd
from loguru import logger

from app.core.regex_patterns import regex
from app.schemas import FileData, ObjectContents, PatternRecognizer
from app.services.hyperscan_service import HyperScanService
from app.services.mitie_service import mitie_service
from app.services.re2_service import Re2Service
from app.services.re_service import ReService

# it's need to define amount of file_data that may be sent big number need to decrease amount of requests
SENSITIVE_DATA_CHUNK = 100_000


class DataAnalysisService:
    def __repr__(self):  # type: ignore
        pass

    def __str__(self):  # type: ignore
        pass

    def __init__(
        self,
        id_name_mapper: Optional[dict[int, Any]] = None,
        labels_mapper: Optional[dict[str, list[str]]] = None,
        hyperscan_recognizers: Optional[list[PatternRecognizer]] = None,
        re2_recognizers: Optional[list[PatternRecognizer]] = None,
        re_recognizers: Optional[list[PatternRecognizer]] = None,
        rescan_mode: Optional[bool] = False,
    ) -> None:
        self.phi_pattern = (
            r'(\b|_)(health)(\b|_)|medical|immun|pharmacy|disease|patient|insura|(\b|_)(Rh)(\b|_)|'
            r'MRN|(\b|_)(phi)(\b|_)'
        )
        self.id_name_mapper: dict[int, str] = id_name_mapper or {}
        self.labels_mapper: dict[str, list[str]] = labels_mapper or {}
        self.hyperscan = HyperScanService(recognizers=hyperscan_recognizers) if hyperscan_recognizers else None
        self.re2 = Re2Service(recognizers=re2_recognizers) if re2_recognizers else None
        self.re = ReService(recognizers=re_recognizers) if re_recognizers else None
        self.mitie = mitie_service if not rescan_mode else None
        self.rescan_mode = rescan_mode

    def _analyze(self, text: str) -> Generator[tuple[int, str, float], None, None]:
        try:
            if self.mitie:
                for result in self.mitie.extract_entities(text):
                    yield result
            if self.hyperscan.db:  # type: ignore
                for result in self.hyperscan.extract_entities(text, self.id_name_mapper):  # type: ignore
                    yield result
            if self.re2:
                for result in self.re2.extract_entities(text):  # type: ignore
                    yield result
            if self.re:
                for result in self.re.extract_entities(text):  # type: ignore
                    yield result
        except Exception as e:
            logger.warning(f'Error _analyze: {e}')
        yield []

    @staticmethod
    def _get_region(entity_type: str) -> str:
        """
        Transcribing acronym of region.

        Args:
            entity_type: name of recognizer

        Returns:
            full region name
        """
        if entity_type[:2] == 'US':
            return "USA"
        elif entity_type[:2] == 'IN':
            return "India"
        else:
            return "All"

    def is_phi_in_file_data(self, full_name: str, file_data: Union[str, pd.DataFrame]) -> bool:
        """
        Checking name on health parts. If they exist we mark this file as medical.

        Args:
            full_name: name of the file
            file_data: detected data from object

        Returns:
            boolean result of checking
        """
        return bool(
            re.search(self.phi_pattern, full_name, re.IGNORECASE)
            or re.search(self.phi_pattern, str(file_data), re.IGNORECASE)
        )

    @staticmethod
    def hash_data(data: str) -> str:
        """
        Hashing data with hashlib.md5 .

        Args:
            data: founded PII data

        Returns:
            hash format string
        """
        return hashlib.sha384(data.encode('utf-8')).hexdigest()

    @staticmethod
    def mask_data(entity: str, data: str) -> str:
        """
        Masking found PII data from analysis processing with symbols.

        Args:
            entity: type of PII finding
            data: The data to be masked.

        Returns:
            masked PII data in string format
        """
        try:
            # Check if the 'data' string is empty, if so, return an empty string.
            if not data:
                return ''
            # If the entity is 'EMAIL' and the data contains an '@' symbol, mask the email address.
            if 'EMAIL' in entity and '@' in data:
                username, domain = data.split('@')
                # separate org email from pulic email and open chars
                if entity == 'EMAIL_ADDRESS':
                    domain = domain.split('.')[-1]
                    data = data[:1] + re.sub('[A-Za-z0-9]', '*', data[1 : (len(data) - len(domain))]) + domain
                else:
                    data = data[:2] + re.sub('[A-Za-z0-9]', '*', data[2 : (len(data) - len(domain))]) + domain
            elif entity in ('US_SSN', 'PERSON'):
                # If the entity is 'US_SSN' or 'PERSON', mask specific portions of the data.
                if len(data) <= 4:
                    data = data[:1] + re.sub('[A-Za-z0-9]', '*', data[1:])
                elif 4 < len(data) <= 6:
                    data = data[:2] + re.sub('[A-Za-z0-9]', '*', data[2:])
                else:
                    data = data[:2] + re.sub('[A-Za-z0-9]', '*', data[2:-2]) + data[-2:]
            else:
                # mask all data if not filters
                data = re.sub('[A-Za-z0-9]', '*', data)
        except Exception as e:
            logger.info(e)
        return data

    def is_db_service(self, service: str) -> bool:
        """
        Check if the service is a database service.

        Args:
            service: service name

        Returns:
            boolean result of checking
        """
        return service in [
            'RedshiftCluster',
            'SnowflakeDatabases',
            'RelationalDatabaseService',
            'DynamoDB',
            'DocumentDBCluster',
        ]

    def scan_file_object(self, content: ObjectContents) -> Generator[list[dict[str, Any]], None, None]:
        """
        Analyze data with Presidio, handling both strings and DataFrames.

        Args:
            content: metadata of file

        Returns:
            Generator yielding FileData results with PII information.
        """
        try:
            if isinstance(content.data, pd.DataFrame):
                data = content.data.drop(['id', 'row_number'], axis='columns', errors='ignore')
                for col in data.columns:
                    content.data = content.data[col].astype(str).str.cat(sep=' ')
                    yield from self._process_data(content, col if self.is_db_service(content.service) else None)
                    content.data = data
            else:
                yield from self._process_data(content)

        except Exception as e:
            logger.error(f'Error in scan_file_object: {e}')
        yield []

    def _process_data(
        self, content: ObjectContents, column_name: Optional[str] = None
    ) -> Generator[list[dict[str, Any]], None, None]:
        processed_results = []
        try:
            for result in self._analyze(text=str(content.data)):
                if not result:
                    continue  # type: ignore
                _id, value, *args = result
                score = round(args[0], 1) if args else 0.8
                recognizer = self.id_name_mapper.get(_id)
                file_data: dict[str, Any] = {
                    "id": str(uuid4()),
                    "file_name": content.object_name,
                    "pii_type": recognizer,
                    "pii_region": self._get_region(recognizer),  # type: ignore
                    "score": score,
                    "is_custom_regex": (recognizer not in regex.system_entities),
                    'pii_hash': self.hash_data(value),
                    'pii_data': self.mask_data(recognizer, value),  # type: ignore
                }
                if column_name:
                    file_data['column_name'] = column_name
                processed_results.append(json.dumps(file_data))
                if len(processed_results) >= SENSITIVE_DATA_CHUNK:
                    yield processed_results
                    processed_results.clear()
        except Exception as e:
            logger.error(f'Error in _process_data: {e}')
        yield processed_results
