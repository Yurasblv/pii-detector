import asyncio
import gc
import multiprocessing as mp
import os
import shutil
from datetime import datetime, timedelta
from typing import Any, Optional, Union

import hyperscan  # type: ignore
from apscheduler.schedulers.blocking import BlockingScheduler  # type: ignore[import]
from apscheduler.util import undefined  # type: ignore
from loguru import logger
from pydantic import parse_obj_as

from app.core.config import ExecutionMode, settings
from app.schemas import (
    AnalyzerAttributes,
    BitbucketConfig,
    DataChunkFilter,
    DataClassification,
    DataClassificationSourcesResponse,
    DataClassifiers,
    FileStatus,
    GithubConfig,
    GitlabConfig,
    ObjectContents,
    RescanObjectResponse,
    SnowflakeConfig,
    SnowflakeUser,
    SupportedServices,
)
from app.send_request import APIEndpoints, HTTPMethods, send_request
from app.services.data_analysis_service import DataAnalysisService
from app.services.mapper import ServicesMapper
from app.services.utils.mappings import repositories_mapper, resource_configuration_mapper, saas_config_mapper
from app.services.utils.sync_scheduler import sync_add_new_jobs
from app.worker_tasks.multiprocessing_tasks import run_scanner, start_processing


def clean_local_storage(folder_path: str) -> None:
    # Ensure the folder exists
    if not os.path.exists(folder_path):
        logger.info(f"The local storage {folder_path} does not exist.")
        return

    # Iterate over all items in the folder
    for item_name in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item_name)

        # Check if it's a file or a directory
        if os.path.isfile(item_path):
            os.remove(item_path)  # Remove the file
        elif os.path.isdir(item_path):
            shutil.rmtree(item_path)  # Remove the directory
    return None


async def start_rescan_task(
    account_id: str,
    data_types: list[DataClassifiers],
    rescan_object: ObjectContents,
    service: ServicesMapper,
    credentials: Union[GithubConfig, GitlabConfig, BitbucketConfig, SnowflakeUser, None],
) -> None:
    """
    Initiates a rescanning task for a specific object using updated classification criteria.
    Initializes a presidio service with the provided attributes and updates its configuration.
    After the rescanning perform cleanup operations before exiting.

    Args:
        account_id: account id that was scanned
        data_types: classifiers for rescanning
        rescan_object: object to be rescanned.
        service: service that was set up for records in dbs
        credentials: Optional credentials required for accessing the data source.

    Returns
        None
    """
    rescan_analyzer_attrs = await service.service.set_recognizers(data_types=data_types)
    analysis_service = DataAnalysisService(**rescan_analyzer_attrs.dict(), rescan_mode=True)
    analysis_service.hyperscan.compile_hyperscan_patterns()  # type: ignore
    rescan_object.current_chunk.latest_data_type = rescan_analyzer_attrs.latest_data_type  # type: ignore

    async with service.service(
        account_id=account_id,
        source=rescan_object.source,
        credentials=credentials,
        analysis_service=analysis_service,
    ) as scaner:
        try:
            logger.info(
                f'Starting {service.value} scanning task for chunk: '  # type: ignore
                f'{rescan_object.source}: offset - {rescan_object.current_chunk.offset} for account: {account_id}'
            )
            await scaner.analyze_content_data(content=rescan_object)
        except Exception as e:
            logger.warning(f"{e}")
        finally:
            logger.info(
                f'End rescanning chunk: {rescan_object.source}:'
                f' offset - {rescan_object.current_chunk.offset}'  # type: ignore
            )
            del analysis_service
            del service
            gc.collect()


def process_rescan_objects(
    account_id: str,
    data_types: list[DataClassifiers],
    object: ObjectContents,
    service: ServicesMapper,
    credentials: Union[GithubConfig, GitlabConfig, BitbucketConfig, SnowflakeUser, None],
) -> None:
    """
    Processes each rescan object by invoking an asynchronous rescan task.
    `asyncio.run` is utilized here to create a new event loop for each process spawned
    by multiprocessing. This is necessary because asyncio requires an event loop to
    run async functions, and each process needs its own event loop to execute these
    functions independently and concurrently.

    The function attempts to run the rescan task and logs an error if a `SystemExit`
    exception occurs, which can happen if the process is interrupted or exits prematurely.

    Args:
        account_id: account id that was scanned
        data_types: classifiers for rescanning
        object: object to be rescanned.
        service: service that was set up for records in dbs
        credentials: Optional credentials required for accessing the data source.

    """
    try:
        asyncio.run(
            start_rescan_task(
                account_id=account_id,
                data_types=data_types,
                rescan_object=object,
                service=service,
                credentials=credentials,
            )
        )
    except SystemExit as e:
        logger.error(f'Process was exited with {e.code}')


async def collect_rescan_attrs(
    data_types: list[DataClassifiers], object: ObjectContents, credentials: Optional[dict[str, Any]]
) -> tuple[str, list[DataClassifiers], ObjectContents, Any, Any]:
    """
    Collecting attributes for process.
    Each attribute contains account_id of user, filtered patterns for chunks, chunk object with metadata,
    service mapper which will be used to sign class of chunk service, credentials for SAAS
    and ID of rescanning instance.
    Args:
        data_types: list with data types for chunk
        object: chunk metadata
        credentials: saas credential for each chunk

    Returns:
        tuple with attributes above
    """

    return (
        object.account_id,
        data_types,
        object,
        ServicesMapper(object.service),  # type: ignore
        credentials and parse_obj_as(saas_config_mapper[object.service], credentials),
    )


async def rescan_by_data_type_task(customer_scheduler: BlockingScheduler, account_id: Optional[str] = None) -> None:
    """
    Initiates and manages scanning job based on rescanning metadata with specified data classifier.

    If an account ID is provided, the function updates the classifier service ID.
    It then retrieves file metadata based on the specified filters and proceeds to
    execute rescan tasks for each file. The method of execution depends on the
    current execution mode, and can involve asynchronous tasks or multiprocessing.

    This function handles different stages of rescanning, including updating statuses,
        collecting rescan attributes, and executing the rescan tasks in multiprocessing pool.

    Args:
        account_id: Optional account identifier. If provided, used for updating
                    classifier service ID and filtering file metadata.

    """
    next_run_time = undefined

    if not account_id:
        account_id = await send_request(
            method=HTTPMethods.GET,
            url=APIEndpoints.USERS_ACCOUNT_ID.url,
            response_model=str,
            aws_account_id=settings.CUSTOMER_ACCOUNT_ID,
        )

    rescan_chunks: list[RescanObjectResponse] = await send_request(
        method=HTTPMethods.GET,
        url=APIEndpoints.RESCAN_CHUNKS_FILTER.url,
        response_model=list[RescanObjectResponse],
        filters=DataChunkFilter(status=FileStatus.SCANNED.value, instance_id=settings.SCANNER_ID),
    )
    if not rescan_chunks:
        # if no scanned records for rescanning , skip job
        logger.info(f"Nothing to rescanning")
        return None

    # collecting credentials for saas sources
    chunk_saas_accounts: set[str] = {
        obj.rescan_object.account_id
        for obj in rescan_chunks
        if not SupportedServices(obj.rescan_object.service).is_aws()  # type:ignore
    }
    # request to cloud account for saas login and password or token instead
    saas_credentials = {
        account_id: await send_request(
            method=HTTPMethods.GET,
            url=APIEndpoints.CLOUD_ACCOUNT.url,
            account_id=account_id,
        )
        for account_id in chunk_saas_accounts
    }
    del chunk_saas_accounts
    # collecting argument to launch them in mp pool
    pool_attrs = [
        await collect_rescan_attrs(
            data_types=obj.data_types,
            object=obj.rescan_object,
            credentials=saas_credentials.get(obj.rescan_object.account_id),  # type: ignore
        )
        for obj in rescan_chunks
    ]
    del saas_credentials
    # for test mode we launch rescan for one by one object
    if settings.EXECUTION_MODE == ExecutionMode.TEST:
        [await start_rescan_task(*attr) for attr in pool_attrs]  # type:ignore
    else:
        # configure number of processes `MAX_PYTHON_PROCESSES` from env
        with mp.get_context('spawn').Pool(processes=settings.MAX_PYTHON_PROCESSES) as mp_pool:
            mp_pool.starmap(func=process_rescan_objects, iterable=pool_attrs)
    del rescan_chunks
    if not pool_attrs:
        logger.success(f"End rescanning for {account_id}")
        next_run_time = datetime.utcnow() + timedelta(minutes=15)
    customer_scheduler.add_job(
        sync_add_new_jobs,
        args=(rescan_by_data_type_task,),
        kwargs={
            'customer_scheduler': customer_scheduler,
            'account_id': account_id,
        },
        misfire_grace_time=None,
        next_run_time=next_run_time,
    )
    clean_local_storage(settings.LOCAL_STORED_ARCHIVES_PATH)
    return None


async def search_for_changes(
    account_id: str,
    source: Any,
    service: ServicesMapper,
    credentials: Optional[SnowflakeUser],
) -> None:
    """
    Search for changes in source and update the status of the object to 'WAIT_FOR_SCAN' if changes are found.

    Args:
        account_id: UUID of NDA user
        source: source for scanning
        service: scanning service class
        credentials: Optional. Credentials for accessing the data sources, if required.:

    Returns:
        None
    """

    logger.info(f'Starting {service.value} scanning task for source: {source} account: {account_id}')
    async with service.service(
        account_id=account_id,
        source=source,
        credentials=credentials,
    ) as scanner:
        await scanner.scanner_preparation()


async def prepare_waiting_attrs(
    account_id: str,
    waiting_object: ObjectContents,
    service: ServicesMapper,
    analyzer_attrs: AnalyzerAttributes,
    credentials: Optional[SnowflakeUser] = None,
) -> tuple[dict[str, Any], ServicesMapper, ObjectContents, AnalyzerAttributes, str]:
    """
     Prepares the attributes required for scanning a list of objects using a specified service and scanner.
     This function creates the necessary attributes for each object that needs to be scanned
    without filtering and creating new objects.

     Args:
         account_id: UUID of NDA user
         waiting_object: objects that need to be scanned.
         service: service mapper object defining the service to be used for scanning.
         analyzer_attrs: attrs for analyzer setup
         credentials: Optional. Credentials for accessing the data sources, if required.

     Returns:
         list of tuples, each containing scanner attributes,
          service mapping, ObjectContents as scanning object.
    """

    async with service.service(
        account_id=account_id,
        source=waiting_object.source,
        credentials=credentials,
    ) as scanner:
        scanner_attrs = {
            'account_id': scanner.account_id,
            'source': waiting_object.source,
            'credentials': scanner.credentials,
        }
        return scanner_attrs, service, waiting_object, analyzer_attrs, settings.SCANNER_ID


async def run_periodic_scanning_task(
    account_id: str,
    supported_service: ServicesMapper,
    credentials: Optional[SnowflakeUser] = None,
    customer_scheduler: Optional[BlockingScheduler] = None,
    classification_id: Optional[str] = None,  # todo : check always or not
) -> None:
    """
    Performs a periodic scanning task on specified data sources, using given credentials and service mappings.
    This function orchestrates the process of identifying data sources for scanning,preparing scanning attributes,
    and executing the scanning tasks either in test mode or using multiprocessing for efficiency.
    It schedules the next scan based on classification settings.

    Args:
        account_id: UUID of NDA user
        supported_service: scanning service class
        credentials: Optional. Credentials for accessing the data sources, if required.
        customer_scheduler: Optional. The scheduler object to schedule periodic scanning tasks.
        classification_id: Optional. The ID of the data classification to apply during scanning.

    Returns:
        list of tuples containing scanning attributes and object contents if scanning tasks are generated,
        None otherwise.
    """

    pool_attrs = []
    start_time = datetime.utcnow()
    async with supported_service.service(account_id=account_id, source='w/o', credentials=credentials) as service:
        # taking classification scanning period with resources input schema to set services attrs
        classification_sources: DataClassificationSourcesResponse = await service.get_classification_sources(
            account_id=account_id,
            classification_id=classification_id,
            response_model=DataClassificationSourcesResponse,
        )
        if not classification_sources:
            return None
        # !for multi scanners! taking <wait for scan> objects to prevents re-collection of attributes
        wait_for_scan_objects = await service.get_wait_for_scan_objects()
        analyzer_attrs = await service.set_recognizers()

        if wait_for_scan_objects:
            pool_attrs.extend(
                [
                    await prepare_waiting_attrs(account_id, obj, supported_service, analyzer_attrs)
                    for obj in wait_for_scan_objects
                ]
            )
        else:
            # for each source schema from inventory set attr for mp Pool
            for source in classification_sources.sources:
                await search_for_changes(
                    account_id=account_id,
                    credentials=service.credentials,
                    source=resource_configuration_mapper[supported_service.native_resource].parse_obj(source),
                    service=supported_service,
                )
            wait_for_scan_objects = await service.get_wait_for_scan_objects()
            if wait_for_scan_objects:
                pool_attrs.extend(
                    [
                        await prepare_waiting_attrs(account_id, obj, supported_service, analyzer_attrs)
                        for obj in wait_for_scan_objects
                    ]
                )
            del classification_sources.sources
    del service
    del wait_for_scan_objects
    gc.collect()
    if settings.EXECUTION_MODE == ExecutionMode.TEST:
        # for test mode we launch rescan for one by one object
        for attr in pool_attrs:
            await run_scanner(*attr)
    else:
        # configure number of processes `MAX_PYTHON_PROCESSES` from env
        with mp.get_context('spawn').Pool(processes=settings.MAX_PYTHON_PROCESSES) as mp_pool:
            mp_pool.starmap(func=start_processing, iterable=pool_attrs)
    if not pool_attrs:
        start_time += timedelta(minutes=classification_sources.scanning_period_minutes)
    # add next task after finishing
    customer_scheduler.add_job(  # type: ignore
        sync_add_new_jobs,
        args=(run_periodic_scanning_task,),
        kwargs={
            'account_id': account_id,
            'supported_service': supported_service,
            'classification_id': classification_id,
            'customer_scheduler': customer_scheduler,  # todo: add creds for snowflake
        },
        next_run_time=start_time,
        misfire_grace_time=None,
    )
    logger.info(f'End {supported_service.value} scanning for account: {account_id}')
    clean_local_storage(settings.LOCAL_STORED_ARCHIVES_PATH)
