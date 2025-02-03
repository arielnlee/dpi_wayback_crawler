import logging
from pathlib import Path
from typing import Optional

from colorama import Style


def setup_logger(
    log_file: str = "wayback_client.log",
    log_level: int = logging.DEBUG,
    log_format: str = "%(asctime)s - %(levelname)s - %(message)s",
) -> logging.Logger:
    """
    Sets up file logging configuration.

    Parameters
    ----------
    log_file : str
        Path to the log file
    log_level : int
        Logging level (e.g. logging.DEBUG)
    log_format : str
        Log format string

    Returns
    -------
    logging.Logger
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger("wayback")
    logger.setLevel(log_level)

    # Remove any existing handlers
    logger.handlers = []

    # Create logs directory if it doesn't exist
    log_path = Path(log_file)
    log_path.parent.mkdir(exist_ok=True)

    # File handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(log_level)
    file_formatter = logging.Formatter(log_format)
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)

    return logger


def print_colored(
    message: str, color: str, logger: Optional[logging.Logger] = None
) -> None:
    """
    Print colored message to console and optionally log it.

    Parameters
    ----------
    message : str
        Message to print
    color : str
        Color to use (from colorama.Fore)
    logger : Optional[logging.Logger]
        Logger instance to use for file logging
    """
    print(f"{color}{message}{Style.RESET_ALL}")
    if logger:
        logger.info(message)
