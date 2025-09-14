from datetime import datetime
import logging
import os


def setup_logging(log_level="INFO", log_to_file=True, log_dir="logs"):
    """
    Sets up centralized logging configuration for the entire project

    Args:
        log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file (bool): Whether to log to file in addition to the console
        log_dir (str): The directory to store the log files
    """
    current_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))

    if log_to_file:
        log_dir = os.path.join(project_root, log_dir)
        os.makedirs(log_dir, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, log_level.upper()))

    root_logger.handlers.clear()

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    console_handler = logging.StreamHandler()
    console_handler.setLevel(getattr(logging, log_level.upper()))
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    if log_to_file:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_filename = f"{project_root.lower()}_{timestamp}.log"
        log_filepath = os.path.join(log_dir, log_filename)

        file_handler = logging.FileHandler(log_filepath)
        file_handler.setLevel(getattr(logging, log_level.upper()))
        file_handler.setFormatter(formatter)
        root_logger.addHandler(file_handler)

        logging.info(f"Logging to file: {log_filepath}")


def get_logger(name):
    """
    Get a logger for a specific module

    Args:
        name (str): __name__ of the calling module

    Returns:
        logging.Logger: Configured logger instance
    """

    return logging.getLogger(name)
