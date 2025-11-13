import asyncio
import gc
from datetime import datetime
from typing import Any

from loguru import logger

from app.schemas import AnalyzerAttributes, FileStatus, ObjectContents
from app.services.data_analysis_service import DataAnalysisService
from app.services.mapper import ServicesMapper


async def run_scanner(
    scanner_attrs: dict[str, Any],
    supported_service: ServicesMapper,
    object_content: ObjectContents,
    analyzer_attrs: AnalyzerAttributes,
    scanner_id: str,
) -> None:
    service = None
    try:
        analysis_service: DataAnalysisService = DataAnalysisService(**analyzer_attrs.dict())
        object_content.current_chunk.latest_data_type = analyzer_attrs.latest_data_type  # type: ignore
        async with supported_service.service(**scanner_attrs, analysis_service=analysis_service) as service:
            update_result = await service.scanning_update_status(
                object_content=object_content,
                status=FileStatus.IN_PROGRESS,
                scanner_id=scanner_id,
                filter_params={'status': FileStatus.WAIT_FOR_SCAN},
            )

            if not update_result:
                return None
            service.analysis_service.hyperscan.compile_hyperscan_patterns()
            await service.analyze_content_data(object_content)
            await service.update_classification_group_with_last_scanned(last_scanned=datetime.utcnow())
    except Exception as e:
        logger.warning(f'{e}')
    finally:
        if service:
            del service
            gc.collect()


def start_processing(
    scanner_attrs: dict[str, Any],
    supported_service: ServicesMapper,
    object_content: ObjectContents,
    analyzer_attrs: AnalyzerAttributes,
    scanner_id: str,
) -> None:
    """
    Function use asyncio to execute result coroutine with asyncio inside multiprocessing Pool.

    Args:
        scanner_attrs: dictionary with args for initializing service class for specific resource
        analyzer_attrs: dictionary with args for initializing presidio class service
        supported_service: service class for specific resource
        object_content: schema with prepared metadata for objects from source

    Returns:
        None
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(
            run_scanner(
                scanner_attrs=scanner_attrs,
                supported_service=supported_service,
                object_content=object_content,
                analyzer_attrs=analyzer_attrs,
                scanner_id=scanner_id,
            )
        )
    except Exception as e:
        logger.error(f'Process was exited with {e}')
    finally:
        loop.close()
