import json
import os
import random
import string
import time
from distutils.util import strtobool
from enum import Enum
from typing import List, Optional, Union

import dotenv  # type: ignore
import psutil  # type: ignore
import requests  # type: ignore
from loguru import logger
from pydantic import BaseSettings, root_validator, validator

from app import description, version


class InstanceIDError(Exception):
    pass


class ExecutionMode(str, Enum):
    TEST = 'Test'
    DEVELOP = 'Develop'


class Settings(BaseSettings):
    """
    Configuration class for various settings and credentials used in the application.

    Attrs:
    - SERVER_NAME, API_V1_STR, PROJECT_NAME, VERSION, DESCRIPTION: Basic server and project information.
    - DEPLOYMENT_TYPE, SENTRY_DSN, EXECUTION_MODE: Deployment and operation configurations.
    - CUSTOMER_ACCOUNT_ID: ID of AWS account
    - IS_RANDOM: enabling randomizing mode to get objects,
    - PATTERN_LIMIT: number of PII data which one classifier could find.
    - BACKEND_CORS_ORIGINS, SERVER_DOMAIN: Network-related configurations.
    - SHARED_SECRET, CUSTOMER_ACCESS_TOKEN: Security and authentication settings.
    - AWS_DEFAULT_REGION, RDS_DATABASE_USER, SCANNER_ID: AWS specific settings.
    - GITHUB_TOKEN, GITHUB_USERNAME, BITBUCKET_LOGIN, BITBUCKET_PASSWORD, GITLAB_TOKEN: Credentials
    for various VCS platforms.
    - ENCRYPT_ITERATIONS, SECRET_TOKEN, DEFAULT_ENCODING: Encoding and security settings.
    - POSTGRES_POOL_SIZE, POSTGRES_MAX_OVERFLOW, POSTGRES_POOL_RECYCLE: PostgreSQL database connection pool settings.
    - MAX_PYTHON_PROCESSES: how much processed must be run into Pool
    - UNSUPPORTED_EXTENSIONS: extensions that excluded from scanning

    Methods:
    - assemble_cors_origins: validator for BACKEND_CORS_ORIGINS.
    - validate_scanned_id: validator to ensure SCANNER_ID is correctly set.
    - get_ec2_id: get the EC2 instance ID based on the execution mode.
    """

    dotenv.load_dotenv()

    SERVER_NAME: str = 'PII detector'
    API_V1_STR: str = f'/v1/{SERVER_NAME}'
    PROJECT_NAME: str = 'PII detector'
    VERSION: str = version()
    DESCRIPTION: str = description()
    DEPLOYMENT_TYPE: str = os.getenv('DEPLOYMENT_TYPE', 'development')
    SENTRY_DSN: Optional[str] = os.getenv('SENTRY_DSN_DATA_SCANNING')
    EXECUTION_MODE: ExecutionMode = ExecutionMode(os.getenv('EXECUTION_MODE', 'Develop'))
    CUSTOMER_ACCOUNT_ID: Optional[str] = os.getenv('CUSTOMER_ACCOUNT_ID', '')[:12]

    # Network
    BACKEND_CORS_ORIGINS: Union[str, list, None] = os.getenv('CORS_ORIGINS', '')  # type: ignore

    SERVER_DOMAIN: str = os.getenv('SERVER_DOMAIN', 'NDA.io')

    # Worker

    SHARED_SECRET: Optional[str] = os.getenv('SHARED_SECRET', 'tenant::stack::secret')
    CUSTOMER_ACCESS_TOKEN: str = ''
    WAIT_OBJECTS_LIMIT: int = 100

    # AWS
    AWS_DEFAULT_REGION: Optional[str] = os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    RDS_DATABASE_USER: Optional[str] = os.environ.get('RDS_DATABASE_USER', 'NDA-user')

    SCANNER_ID: str = ''

    # GitHubCredentials
    GITHUB_TOKEN: Optional[str] = os.environ.get('GITHUB_TOKEN', '')
    GITHUB_USERNAME: Optional[str] = os.environ.get('GITHUB_USERNAME', '')

    # BitBucketCredentials
    BITBUCKET_LOGIN: Optional[str] = os.environ.get('BITBUCKET_LOGIN', '')
    BITBUCKET_PASSWORD: Optional[str] = os.environ.get('BITBUCKET_PASSWORD', '')

    # GitLabCredentials
    GITLAB_TOKEN: Optional[str] = os.environ.get('GITLAB_TOKEN', '')

    # PII
    MAX_PYTHON_PROCESSES: int = int(os.getenv('MAX_PYTHON_PROCESSES', 5))

    # Ignore extensions
    UNSUPPORTED_EXTENSIONS = (
        '.png',
        '.jpg',
        '.jpeg',
        '.gif',
        '.bmp',
        '.svg',
        '.tif',
        '.tiff',
        '.ico',
        '.mbox',
        '.webm',
    )

    # Encoding / Security

    ENCRYPT_ITERATIONS: int = os.environ.get('ENCRYPT_ITERATIONS', 100_000)  # type: ignore
    SECRET_TOKEN: str = os.getenv('SECRET_TOKEN')  # type: ignore
    DEFAULT_ENCODING: str = os.getenv('DEFAULT_ENCODING', 'UTF-8')

    POSTGRES_POOL_SIZE: int = 100
    POSTGRES_MAX_OVERFLOW: int = 10
    POSTGRES_POOL_RECYCLE: int = 1800

    # EBS Storage
    UPLOADED_FILES_FOLDER: str = 'uploaded_files'
    LOCAL_STORED_ARCHIVES_PATH: str = os.path.abspath(__file__ + f"/../../../{UPLOADED_FILES_FOLDER}")
    INITIAL_DISK_SPACE: int = psutil.disk_usage('/').free

    # CHUNKS
    CHUNK_BYTES_CAPACITY: int = 1_000_000  # amount of bytes for files
    OVERLAP_BYTES: int = 255  # count of bytes that will be added to offset during fetch_data operation
    CHUNK_ROWS_CAPACITY: int = 100_000  # amount of rows for sql tables
    CHUNK_JSON_CAPACITY: int = 1000  # amount of json objects (for Nosql)

    # TEST_STRING_FOR_PATTERNS
    TEST_STRING_FOR_PATTERNS: str = "George Washington went to Washington."
    # Validators

    @validator('BACKEND_CORS_ORIGINS', pre=True)  # type: ignore
    def assemble_cors_origins(cls, v: Union[str, List[str]]) -> Union[List[str], str]:
        if isinstance(v, str) and not v.startswith('['):
            return [i.strip() for i in v.split(',')]
        elif isinstance(v, (list, str)):
            return v
        raise ValueError(v)

    class Config:
        case_sensitive = True

    @root_validator  # type: ignore
    def validate_scanned_id(cls, values):
        values['SCANNER_ID'] = cls.get_ec2_id(mode=ExecutionMode(values['EXECUTION_MODE']))
        return values

    @staticmethod
    def get_ec2_id(mode: ExecutionMode, attempt: int = 0) -> str:
        if attempt > 10:
            raise InstanceIDError

        if mode == ExecutionMode.TEST:
            return 'test-' + ''.join(random.choices(string.ascii_lowercase + string.digits, k=17))
        try:
            response = requests.get('http://169.254.169.254/latest/dynamic/instance-identity/document')
            instance_info = json.loads(response.text)
            return instance_info['instanceId']  # type: ignore[no-any-return]
        except requests.exceptions.RequestException as e:
            logger.error(e)
            time.sleep(attempt * 10)
            return Settings().get_ec2_id(mode, attempt + 1)


settings = Settings()
