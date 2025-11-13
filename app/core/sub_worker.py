import asyncio
from typing import Any

from loguru import logger


class SubWorker:
    @staticmethod
    async def run(concurrency: int = 10, *tasks: Any) -> Any:
        if concurrency == 0:
            return await asyncio.gather(*tasks)
        semaphore = asyncio.Semaphore(concurrency)

        async def run_task(task: Any) -> Any:
            async with semaphore:
                try:
                    return await task
                except Exception as e:
                    logger.error(f'error while running task. Error: {e}')
                    return None

        return await asyncio.gather(*[run_task(task) for task in tasks])
