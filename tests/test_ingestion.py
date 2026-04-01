from src.ingestion.ingestion_job import run_ingestion_tweets
from src.storage.s3.downloader import download_from_s3
from src.storage.s3.partitioning import get_s3_key
from pathlib import Path
import json
from src.ingestion.api.enums.query_type import QueryType
from src.utils.logger import get_logger

logger = get_logger(__name__)

def save_local(data: list[dict], ingestion_time) -> Path:
    folder = Path("data/raw") / f"tweets_{ingestion_time.strftime('%Y-%m-%d')}"
    folder.mkdir(parents=True, exist_ok=True)

    file_path = folder / f"tweets_{ingestion_time.strftime('%Y-%m-%dT%H-%M-%S')}.json"
    file_path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    return file_path

def test_ingestion(query: str, query_type: QueryType, max_pages: int = 1):
    logger.info("======= TESTING INGESTION =======")

    result = run_ingestion_tweets(query, query_type, max_pages)
    if result is None:
        logger.error("No data ingested. Test failed.")
        return
    
    _, tweets, ingestion_time = result

    local_path = save_local(tweets, ingestion_time)

    s3_key = get_s3_key(
        layer="raw",
        dataset="tweets",
        filename=f"tweets_{ingestion_time.strftime('%Y%m%d_%H%M%S')}.json",
        ingestion_time=ingestion_time,
        partition_cols=["ingestion_date"]
    )
    cloud_data = download_from_s3(s3_key)
    local_data = json.loads(local_path.read_text(encoding="utf-8"))

    assert cloud_data == local_data, "Data mismatch between S3 and local file. Test failed."
    logger.info("Ingestion test passed successfully.")

