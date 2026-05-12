import logging
from pathlib import Path

LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
LOG_DIR.mkdir(exist_ok=True)

def get_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)

    if logger.handlers:
        return logger
    
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # INFO+ -> logs/ingestion.log
    ingestion_handler = logging.FileHandler(LOG_DIR / "ingestion.log")
    ingestion_handler.setLevel(logging.INFO)
    ingestion_handler.setFormatter(formatter)
    
    # INFO+ -> logs/processing.log
    processing_handler = logging.FileHandler(LOG_DIR / "processing.log")
    processing_handler.setLevel(logging.INFO)
    processing_handler.setFormatter(formatter)
    
    # INFO+ -> logs/analytic.log
    analytic_handler = logging.FileHandler(LOG_DIR / "analytic.log")
    analytic_handler.setLevel(logging.INFO)
    analytic_handler.setFormatter(formatter)

    # ERROR+ -> logs/errors.log
    error_handler = logging.FileHandler(LOG_DIR / "errors.log")
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(formatter)

    logger.setLevel(logging.INFO)
    logger.addHandler(ingestion_handler)
    logger.addHandler(processing_handler)
    logger.addHandler(analytic_handler)
    logger.addHandler(error_handler)

    return logger