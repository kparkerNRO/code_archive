import logging
import sys

import structlog
from pydantic_settings import BaseSettings


class LoggingSettings(BaseSettings):
    log_level: str = "INFO"


def configure_structlog():
    settings = LoggingSettings()
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        stream=sys.stdout,
        level=settings.log_level,
        force=True,
    )

    # Configure structlog to wrap around the built-in logger
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.stdlib.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.processors.dict_tracebacks,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )


configure_structlog()


def getLogger(name=None):
    """
    Returns a structlog logger.
    If a name is provided, it will be used as the logger's name, similar to logging.getLogger.
    """
    return structlog.get_logger(name)
