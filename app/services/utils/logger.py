import sys

from loguru import logger

from app.core.config import ExecutionMode, settings


def configure_logging() -> None:
    """
    Configuring logging level based on EXECUTION_MODE. For test mode level is DEBUG. For other cases INFO.
    """
    match settings.EXECUTION_MODE:
        case ExecutionMode.TEST:
            log_level = 'DEBUG'
        case _:
            log_level = 'INFO'

    logger.remove()
    logger.add(sys.stderr, level=log_level)
    return None
