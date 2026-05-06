import asyncio
import aiohttp
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from src.ingestion.api.fetcher import TwitterDataFetcher
from src.ingestion.api.client import TwitterAPIClient
from src.storage.s3.uploader import upload_to_bronze_s3
from src.ingestion.api.enums.query_type import QueryType
from src.utils.config_loader import load_config
from src.utils.logger import get_logger

load_dotenv()
CONFIG = load_config()
logger = get_logger(__name__)

BUFFER_LIMIT_BYTES = 150 * 1024 * 1024  # 150MB


def add_ingestion_date(records: list[dict], ingestion_time: datetime) -> list[dict]:
    ingestion_date = ingestion_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    for record in records:
        record["ingestion_date"] = ingestion_date
    return records


async def run_ingestion_tweets(query: str, query_type: QueryType, tweets_number: int) -> list[str]:
    ingestion_time = datetime.now(timezone.utc)
    buffer: list[dict] = []
    buffer_size = 0
    s3_paths: list[str] = []

    client = TwitterAPIClient(
        api_key=os.getenv("MY_API_KEY"),
        base_url=CONFIG["api"]["url"]
    )
    fetcher = TwitterDataFetcher(client)

    logger.info(f"Starting ingestion: '{query}' | {query_type.name} | target={tweets_number}")

    async with aiohttp.ClientSession() as session:
        async for batch in fetcher.fetch_tweets(session, query, query_type, tweets_number):
            batch = add_ingestion_date(batch, ingestion_time)
            buffer.extend(batch)
            buffer_size += sum(len(json.dumps(r, ensure_ascii=False).encode()) for r in batch)

            if buffer_size >= BUFFER_LIMIT_BYTES:
                s3_path = upload_to_bronze_s3(data=buffer, dataset="tweets", ingestion_time=ingestion_time)
                logger.info(f"Buffer flushed: {len(buffer)} tweets → {s3_path}")
                s3_paths.append(s3_path)
                buffer.clear()
                buffer_size = 0

    if buffer:
        s3_path = upload_to_bronze_s3(data=buffer, dataset="tweets", ingestion_time=ingestion_time)
        logger.info(f"Final flush: {len(buffer)} tweets → {s3_path}")
        s3_paths.append(s3_path)

    elapsed = (datetime.now(timezone.utc) - ingestion_time).total_seconds()
    logger.info(f"Ingestion completed in {elapsed:.2f}s — {len(s3_paths)} chunk(s) uploaded")

    return s3_paths


async def main() -> None:
    query = os.getenv("QUERY")
    await run_ingestion_tweets(query=query, query_type=QueryType.LATEST, tweets_number=100)


if __name__ == "__main__":
    asyncio.run(main())
