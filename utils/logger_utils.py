import logging
import sys
from logging.handlers import RotatingFileHandler
from typing import Optional

# Constants
LOG_FORMAT = "%(asctime)s - %(name)s - [%(levelname)s] - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
MAX_LOG_SIZE_BYTES = 10 * 1024 * 1024  # 10MB
LOG_BACKUP_COUNT = 5

# Global formatter instance
FORMATTER = logging.Formatter(fmt=LOG_FORMAT, datefmt=LOG_DATE_FORMAT)


def _get_console_handler() -> logging.StreamHandler:
    """Creates and configures a console logging handler."""
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(FORMATTER)
    return console_handler


def _get_file_handler(log_file: str) -> RotatingFileHandler:
    """Creates and configures a rotating file logging handler."""
    file_handler = RotatingFileHandler(log_file, maxBytes=MAX_LOG_SIZE_BYTES, backupCount=LOG_BACKUP_COUNT)
    file_handler.setFormatter(FORMATTER)
    return file_handler


def configure_logging(filename: Optional[str] = None, log_level: int = logging.INFO) -> None:
    """
    Configures the root logger for the application.
    This function should be called exactly once at the application entry point.

    Args:
        filename: Optional path to a log file. If provided, file logging is enabled.
        log_level: The logging level (e.g., logging.INFO, logging.DEBUG).
    """
    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)

    # Remove existing handlers to prevent duplicate logging
    if root_logger.hasHandlers():
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)

    # Always add Console Handler
    root_logger.addHandler(_get_console_handler())

    # Add File Handler if a filename is provided
    if filename:
        try:
            root_logger.addHandler(_get_file_handler(filename))
        except OSError as e:
            # Fallback to stderr if file logging fails
            sys.stderr.write(f"Warning: Could not set up file logging to '{filename}': {e}\n")

    # Suppress noisy logs from third-party libraries
    logging.getLogger("urllib3").setLevel(logging.WARNING)


def get_logger(name: str) -> logging.Logger:
    """
    Retrieves a logger instance with the specified name.
    """
    return logging.getLogger(name)
