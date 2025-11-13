import signal
from typing import Any

from loguru import logger

from app.core.scheduler import backdrop_scheduler, customer_scheduler


class SignalHandler:
    """
    Registers SIGINT AND SIGTERM signals and handle the app termination gracefully!
    """

    def __init__(self) -> None:
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self, signum: Any, frame: Any) -> None:
        logger.info('Recived the termination signal, shutting down all schedulers!!!')
        customer_scheduler.shutdown()
        backdrop_scheduler.shutdown()
        logger.info('customer_scheduler and backdrop_scheduler  shut down successfully')
        return None
