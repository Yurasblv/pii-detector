import asyncio
from typing import Any

from loguru import logger

from app.schemas import InstancesUpdate
from app.send_request import APIEndpoints, HTTPMethods, send_request


def sync_add_new_jobs(func: Any, **kwargs) -> Any:  # type: ignore[no-untyped-def]
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(func(**kwargs))
    finally:
        loop.close()


async def cron_update_instance_record(instance_id: str) -> None:
    """
    Updates the instance record in the database. This works as a health check of the instance.
    """
    try:
        await send_request(
            method=HTTPMethods.PATCH,
            url=f"{APIEndpoints.SCANNER.url}{instance_id}",
        )
    except Exception as e:
        logger.error(e)
