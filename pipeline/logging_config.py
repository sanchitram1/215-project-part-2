"""
Logging configuration for the ETL pipeline.

Sets up logging based on environment variables:
- DEBUG=1: Enable DEBUG level logging
- Otherwise: INFO level logging (default)
"""

import logging
import os


def get_logger(name: str) -> logging.Logger:
    """
    Get a configured logger instance.

    Sets logging level based on DEBUG environment variable.
    If DEBUG=1, uses DEBUG level; otherwise uses INFO level.

    Args:
        name: Logger name (typically __name__)

    Returns:
        Configured logger instance
    """
    logger = logging.getLogger(name)

    # Set logging level based on DEBUG environment variable
    if os.getenv("DEBUG") == "1":
        log_level = logging.DEBUG
    else:
        log_level = logging.INFO

    logger.setLevel(log_level)

    # Create console handler if not already present
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setLevel(log_level)

        # Create formatter
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)

        logger.addHandler(handler)

    return logger
