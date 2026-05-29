import logging
import sys
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)


def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger

    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setLevel(logging.INFO)
    stdout_handler.setFormatter(formatter)

    error_handler = logging.FileHandler(LOG_DIR / "errors.log")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    logger.setLevel(logging.INFO)
    logger.addHandler(stdout_handler)
    logger.addHandler(error_handler)

    return logger
