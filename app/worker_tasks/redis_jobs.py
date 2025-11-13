import asyncio
import multiprocessing as mp
import re
from typing import Any, Optional
from uuid import UUID

from apscheduler.schedulers.blocking import BlockingScheduler  # type: ignore[import]
from loguru import logger

from app import schemas
from app.core.config import ExecutionMode, settings
from app.schemas import (
    Category,
    DataChunkFilter,
    DataClassificationGroupRead,
    DataClassifierFilters,
    DataClassifiers,
    DataClassifiersUpdate,
    DataClassifierType,
    FileMetadata,
    FileMetadataRead,
    FileStatus,
    ObjectContents,
    SnowflakeUser,
    SupportedServices,
)
from app.send_request import APIEndpoints, HTTPMethods, send_request
from app.services.mapper import ServicesMapper
from app.services.utils.sync_scheduler import sync_add_new_jobs
from app.worker_tasks.redis_tasks import clean_local_storage, run_periodic_scanning_task, start_rescan_task

list_of_job_args = set()


async def detect_new_tasks_job(customer_scheduler: Optional[BlockingScheduler] = None) -> None:
    """
    Asynchronously detects new tasks and schedules them for execution.

    This function queries for the AWS account UUID and retrieves classification groups.
    It iterates through these groups and their classifications to identify new tasks
    that need to be scheduled. Only tasks that match certain conditions (like scanner IDs,
    service types, and account UUIDs) are considered. One job will start for each classification.

    If a new task is detected (not already in the global list of job arguments), it's
    logged and added to the scheduler with relevant arguments.

    Args:
        customer_scheduler: An instance of BlockingScheduler to schedule new tasks.
                            If None, the scheduler is not used.

    """
    logger.info("Start new job detection")
    global list_of_job_args
    # get account_id from inventory for aws resources
    aws_account_uuid = await send_request(
        method=HTTPMethods.GET,
        url=APIEndpoints.USERS_ACCOUNT_ID.url,
        response_model=str,
        aws_account_id=settings.CUSTOMER_ACCOUNT_ID,
    )
    classification_groups: list[schemas.DataClassificationGroup] = await send_request(
        HTTPMethods.GET, APIEndpoints.CLASSIFICATION_GROUPS.url, response_model=list[DataClassificationGroupRead]
    )
    for classification_group in classification_groups:
        if (
            settings.SCANNER_ID
            and classification_group.scanner_ids
            and settings.SCANNER_ID not in classification_group.scanner_ids
        ):
            # scanner_id must be equal to classification which launch job , else we skip processing
            continue
        # iterate through all classifications in group
        # TODO: remove logic when group can have multiple classifications
        for classification in classification_group.data_classifications:
            if (
                not SupportedServices(classification.service).is_aws()  # type: ignore
                and str(classification_group.scanner_account_id) != aws_account_uuid
            ):
                # if classification service not relate to aws and
                # scanner id is not in classification scanner_account_id we skip job
                continue
            for service_id in classification_group.service_ids:
                if (
                    SupportedServices(classification.service).is_aws()  # type: ignore
                    and service_id != aws_account_uuid
                ):
                    # for aws services we must ensure that account id is present in inventory,
                    # if this id does not exist we skip job
                    continue
                kwargs = {
                    'account_id': service_id,
                    'supported_service': ServicesMapper(classification.service),  # type: ignore
                    'classification_id': str(classification.id),
                }
                if (args := tuple(kwargs.values())) not in list_of_job_args:
                    # if job does not start we don't add it to global list
                    logger.info(f'New task detected. {kwargs=}')
                    list_of_job_args.add(args)
                    kwargs.update({'customer_scheduler': customer_scheduler})  # type: ignore
                    customer_scheduler.add_job(  # type: ignore
                        sync_add_new_jobs,
                        args=(run_periodic_scanning_task,),
                        kwargs=kwargs,
                        id=str(classification.id),
                        misfire_grace_time=None,
                    )

    logger.info('End new job detection')

