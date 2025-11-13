import asyncio
import enum
import gzip
import json
from datetime import datetime
from types import GenericAlias
from typing import Any
from uuid import UUID

import aiohttp  # type: ignore
from loguru import logger
from pydantic import BaseModel, parse_obj_as
from pympler import asizeof  # type: ignore
from sqlmodel import SQLModel

from app.core.config import ExecutionMode, settings
from app.services.utils.token_refresher import refresh_shared_secret


class HTTPMethods(enum.Enum):
    GET = 'GET'
    POST = 'POST'
    PUT = 'PUT'
    PATCH = 'PATCH'
    DELETE = 'DELETE'


class APIEndpoints(str, enum.Enum):
    """
    Enum with endpoints for connection `PII registration server` repository.
     Endpoints located in app/api/api_v1/endpoints/customer_account.py.
    """

    SAVE_SCANNED_METADATA = 'customer_account/scanned-file-metadata'
    FILE_METADATA_FILTER = 'customer_account/file-metadata/filter'
    FILE_METADATA_BATCH = 'customer_account/batch-file-metadata'
    CLASSIFIERS_FILTER = 'customer_account/data-classifiers/filter'
    CLASSIFICATION_SOURCES = 'customer_account/data-classification-sources'
    CLASSIFICATION_FILTER = 'data-classification/filter'
    REGION_SOURCES = 'customer_account/file-data'
    USERS_ACCOUNT_ID = 'customer_account/users_account_id'
    CLASSIFICATION_GROUPS = 'customer_account/data_classification_groups'
    UPDATE_CLASSIFICATION_LAST_SCANNED = 'customer_account/data_classification_last_scanned'
    SAVE_RESCANNED_METADATA = 'customer_account/rescanned-file-metadata'
    UPDATE_WITH_IGNORE = 'customer_account/not-ignored-file-metadata'
    CLOUD_ACCOUNT = 'customer_account/cloud-account'
    DELETE_BATCH_METADATA = 'customer_account/delete-batch-metadata'
    GET_DATA_CHUNK_BY_FILTER = 'customer_account/get-data-chunk-by-filter'
    SCANNER = 'customer_account/scanner'
    CHUNKS = 'customer_account/data-chunks'
    CHUNKS_BATCH = 'customer_account/data-chunks-batch'
    CHUNKS_FILTER = 'customer_account/data-chunks/filter'
    RESCAN_CHUNKS_FILTER = 'customer_account/rescan/data-chunks/filter'
    SENSITIVE_DATA = 'customer_account/sensitive-data'
    RESCAN_CREDENTIALS = 'customer_account/rescan/credentials'

    def __init__(self, value: str) -> None:
        """
        Initializes an APIEndpoints instance with a specific endpoint URL.

        The URL is constructed based on the execution mode setting. In test mode,
        a local server URL is used. In other modes, the URL is built using shared
        secrets and the server domain settings.

        Args:
            value: The endpoint path as a string, which is appended to the base URL.
        """
        if settings.EXECUTION_MODE == ExecutionMode.TEST:
            self.url: str = f'http://server:8000{settings.API_V1_STR}/{value}/'
        else:
            tenant, stack, secret = settings.SHARED_SECRET.split("::")  # type:ignore
            self.url: str = (  # type:ignore[no-redef]
                f'https://{stack}.{settings.SERVER_DOMAIN}{settings.API_V1_STR}/{value}/'
            )


async def make_request(method: HTTPMethods, url: str, request_data: Any, attempt: int = 0) -> Any:
    """
    Asynchronously makes an HTTP request with the specified parameters.

    Handles different HTTP methods, constructs appropriate request arguments, and
    manages retries and error logging. It also refreshes shared secrets on authorization failures.

    Args:
        method: HTTP method to be used for the request.
        url: The target URL for the request.
        request_data: Data to be sent in the request.
        attempt: Current attempt number, used for handling retries.

    Returns:
        The JSON response from the request, or None in case of failures.
    """
    # create base_url to use it in recursion if raises Exception
    base_url = url
    request_args: dict[str, Any] = {
        'headers': {
            'Authorization': f'bearer {settings.CUSTOMER_ACCESS_TOKEN}',
            'Content-type': 'text/plain',
        },
    }

    if method in [HTTPMethods.GET, HTTPMethods.DELETE]:
        # adding params in url for Get and Delete methods
        if isinstance(request_data, str):
            url = url + request_data
        elif request_data:
            request_args['params'] = {key: value for key, value in request_data.items() if value is not None}
    elif method is HTTPMethods.POST:
        request_args['headers']['Content-type'] = 'application/json'
        # adding body as json for POST method
        if isinstance(request_data, dict) and isinstance(list(request_data.values())[0], list):
            request_args['data'] = gzip.compress(json.dumps(list(request_data.values())[0]).encode('utf-8'))
        else:
            request_args['data'] = gzip.compress(json.dumps(request_data).encode('utf-8'))
        request_args['headers']['Accept-Encoding'] = 'gzip'
    else:
        request_args['data'] = gzip.compress(json.dumps(request_data).encode('utf-8'))
        request_args['headers']['Content-type'] = 'application/json'
        request_args['headers']['Accept-Encoding'] = 'gzip'

    try:
        size = asizeof.asizeof(request_args)
        logger.info(f'Sending request [{method.value}] {url} {size} bytes')
        async with aiohttp.request(
            method=method.value,
            url=url,
            **request_args,
        ) as response:
            logger.debug(f'Request was sent [{method.value}] {url}. Status: {response.status}')
            if response.status in (404, 422):
                logger.warning(await response.json())
                return None
            elif response.status == 401:
                # refresh token if NDA authorize failed
                if attempt == 2:
                    return None
                refresh_shared_secret()
                return await make_request(method, base_url, request_data, attempt + 1)
            elif response.status == 424 or response.status > 500:
                logger.error(f"Status:{response.status}. Url: {url}.\nResponse = {await response.json()}")
                await asyncio.sleep(1)
                return await make_request(method, base_url, request_data)
            return await response.json()
    except Exception as e:
        logger.error(e)

def get_request_value(kwarg_key: str, kwarg_value: Any) -> Any:
    """
    Converts a request parameter value to an appropriate format for HTTP requests.

    Handles different types of values including models, dictionaries, and lists,
    and converts them to a suitable format for the request.

    Args:
        kwarg_key: The key of the parameter.
        kwarg_value: The value of the parameter to be converted.

    Returns:
        The converted value in a format suitable for HTTP requests.
    """
    if isinstance(kwarg_value, (SQLModel, BaseModel)):
        return convert_values(kwarg_value.dict(exclude_unset=True, exclude_none=True))
    elif isinstance(kwarg_value, dict):
        return kwarg_value
    elif isinstance(kwarg_value, list) and kwarg_value and isinstance(kwarg_value[0], BaseModel):
        return {kwarg_key: [convert_values(value.dict(exclude_unset=True, exclude_none=True)) for value in kwarg_value]}
    elif isinstance(kwarg_value, list):
        return {kwarg_key: kwarg_value}
    return str(kwarg_value)


def convert_value(value: Any) -> Any:
    """
    Converts a single value to an appropriate format for serialization.

    This function is used to prepare data for HTTP requests, ensuring that complex
    types like enums, datetimes, and models are serialized correctly.

    Args:
        value: The value to be converted.

    Returns:
        The value converted to a serializable format.
    """
    if isinstance(value, enum.Enum):
        return value.value
    elif isinstance(value, datetime):
        return value.isoformat()
    elif isinstance(value, BaseModel):
        return convert_values(value.dict(exclude_unset=True, exclude_none=True))
    elif isinstance(value, bool):
        return str(value)
    elif isinstance(value, dict):
        return convert_values(value)
    elif isinstance(value, UUID):
        return str(value)
    elif isinstance(value, list):
        return [convert_value(v) for v in value]
    return value


def convert_values(kwargs: dict[str, Any]) -> dict[str, Any]:
    """
    Converts a dictionary of values to appropriate formats for serialization.

    Iterates over key-value pairs in the dictionary and converts each value using
    the `convert_value` function for proper serialization.

    Args:
        kwargs: A dictionary of key-value pairs to be converted.

    Returns:
        A dictionary with all values converted to serializable formats.
    """
    return {key: convert_value(value) for key, value in kwargs.items()}


def prepare_request(kwargs: dict[str, Any]) -> Any:
    """
    Prepares a dictionary of request parameters for HTTP requests.

    Removes certain keys (like 'db' and 'session') and processes the remaining
    parameters using `get_request_value` and `convert_values` functions.

    Args:
        kwargs: A dictionary of request parameters.

    Returns:
        The processed request parameters, ready for use in an HTTP request.
    """
    kwargs.pop('db', None)
    kwargs.pop('session', None)
    if len(kwargs) == 0:
        pass
    elif len(kwargs) == 1:
        kwargs = get_request_value(*kwargs.popitem())
    return convert_values(kwargs) if isinstance(kwargs, dict) else kwargs


async def send_request(method: HTTPMethods, url: str, response_model: Any = None, **kwargs) -> Any:  # type: ignore
    """
    Asynchronously sends an HTTP request and processes the response.

    Prepares the request data, makes the HTTP request, and parses the response based
    on the provided response model. Handles various response scenarios.

    Args:
        method: The HTTP method to be used.
        url: The target URL for the request.
        response_model: The model to parse the response into. Optional.
        kwargs: Additional keyword arguments for the request.

    Returns:
        The response parsed into the specified model, or raw response data if no model is provided.
    """
    request_data = prepare_request(kwargs)
    result = await make_request(method, url, request_data)

    if isinstance(response_model, GenericAlias):
        if not result:
            return []
    elif isinstance(result, str):
        return result
    elif isinstance(response_model, SQLModel):
        if not result or not result.get('details'):
            return None
    else:
        if not result:
            return None
    if response_model:
        return parse_obj_as(response_model, result)
    return result
