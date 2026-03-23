import logging
from src.config.settings import LOG_FORMAT, LOG_LEVEL

def setup_logging():
    logger = logging.getLogger()
    logger.handlers = []
    logger.setLevel(LOG_LEVEL)

    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter(LOG_FORMAT))
    logger.addHandler(console_handler)

    return logger