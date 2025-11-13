import asyncio
from datetime import datetime

import requests  # type: ignore[import]

from app.core.config import ExecutionMode, settings
from app.core.scheduler import backdrop_scheduler, customer_scheduler
from app.schemas import Instances, InstancesUpdate
from app.send_request import APIEndpoints, HTTPMethods, send_request
from app.services.utils.logger import configure_logging
from app.services.utils.sync_scheduler import cron_update_instance_record, sync_add_new_jobs
from app.services.utils.token_refresher import refresh_shared_secret
from app.signal_handler import SignalHandler
from app.worker_tasks.redis_jobs import detect_new_tasks_job
from app.worker_tasks.redis_tasks import rescan_by_data_type_task


async def create_instance_record(account_id: str, id_ec2: str) -> Instances:
    return await send_request(  # type: ignore
        method=HTTPMethods.POST,
        url=APIEndpoints.SCANNER.url,
        response_model=Instances,
        obj_in=Instances(instance_id=id_ec2, account_id=account_id, region=settings.AWS_DEFAULT_REGION),
    )


def token_refresher_job() -> None:
    """Updating token job for connection to NDA.

    By default, token expires after 300 seconds.

    """
    token_period = refresh_shared_secret()
    if customer_scheduler.get_job('refresh_shared_secret_id'):
        customer_scheduler.remove_job('refresh_shared_secret_id')
    customer_scheduler.add_job(
        token_refresher_job,
        trigger='interval',
        seconds=token_period,
        id='refresh_shared_secret_id',
    )


async def set_instance_id() -> Instances:
    """Save id of ec2 instance to database for continue using.

    Returns:
        save instance information from server in Instances scheme format

    """
    account_id = await send_request(
        method=HTTPMethods.GET,
        url=APIEndpoints.USERS_ACCOUNT_ID.url,
        response_model=str,
        aws_account_id=settings.CUSTOMER_ACCOUNT_ID,
    )
    return await create_instance_record(account_id=account_id, id_ec2=settings.SCANNER_ID)


async def main() -> None:

    # Registing signal handler
    SignalHandler()
    # set level for logs
    configure_logging()
    # before launching we clear all previous job
    customer_scheduler.remove_all_jobs()
    backdrop_scheduler.remove_all_jobs()
    if settings.EXECUTION_MODE != ExecutionMode.TEST:
        token_refresher_job()
    await set_instance_id()
    # add job for regular scanning procedure
    customer_scheduler.add_job(
        sync_add_new_jobs,
        args=(detect_new_tasks_job,),
        kwargs={'customer_scheduler': customer_scheduler},
        trigger='interval',
        minutes=15,
        next_run_time=datetime.utcnow(),
        misfire_grace_time=None,
    )
    # # add rescanning job
    customer_scheduler.add_job(
        sync_add_new_jobs,
        args=(rescan_by_data_type_task,),
        kwargs={'customer_scheduler': customer_scheduler},
        trigger='interval',
        minutes=15,
        next_run_time=datetime.utcnow(),
        misfire_grace_time=None,
    )
    # add background job which ping alive status of scanner
    backdrop_scheduler.add_job(
        sync_add_new_jobs,
        args=(cron_update_instance_record,),
        kwargs={'instance_id': settings.SCANNER_ID},
        trigger='interval',
        minutes=1,
        misfire_grace_time=None,
    )
    # start schedulers jobs
    backdrop_scheduler.start()
    customer_scheduler.start()
    return None


if __name__ == '__main__':
    asyncio.run(main())
