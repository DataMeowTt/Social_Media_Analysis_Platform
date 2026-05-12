import asyncio
import aiohttp
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from src.ingestion.api.fetcher import TwitterDataFetcher
from src.ingestion.api.client import TwitterAPIClient, CreditsExhaustedError
from src.storage.s3.uploader import upload_to_bronze_s3
from src.ingestion.api.enums.query_type import QueryType
from src.utils.config_loader import load_config
from src.utils.logger import get_logger

load_dotenv()
CONFIG = load_config()
logger = get_logger(__name__)

BUFFER_LIMIT_BYTES = 150 * 1024 * 1024  # 150MB


def _load_api_keys() -> list[str]:
    """Return [MY_API_KEY_1, MY_API_KEY_2, ...] in order, stopping at the first missing index."""
    keys = []
    i = 1
    while True:
        key = os.getenv(f"MY_API_KEY_{i}")
        if not key:
            break
        keys.append(key)
        i += 1
    return keys


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
    total_fetched = 0

    api_keys = _load_api_keys()
    if not api_keys:
        logger.error("No API keys found — set MY_API_KEY_1, MY_API_KEY_2, ... in .env")
        return s3_paths

    logger.info(
        f"Starting ingestion: '{query}' | {query_type.name} | "
        f"target={tweets_number} | keys={len(api_keys)}"
    )

    async with aiohttp.ClientSession() as session:
        for key_idx, api_key in enumerate(api_keys, start=1):
            remaining = tweets_number - total_fetched
            if remaining <= 0:
                break

            logger.info(f"[key {key_idx}/{len(api_keys)}] fetching {remaining} more tweets")
            client = TwitterAPIClient(api_key=api_key, base_url=CONFIG["api"]["url"])
            fetcher = TwitterDataFetcher(client)

            try:
                async for batch in fetcher.fetch_tweets(session, query, query_type, remaining):
                    batch = add_ingestion_date(batch, ingestion_time)
                    buffer.extend(batch)
                    total_fetched += len(batch)
                    buffer_size += sum(len(json.dumps(r, ensure_ascii=False).encode()) for r in batch)

                    if buffer_size >= BUFFER_LIMIT_BYTES:
                        s3_path = upload_to_bronze_s3(data=buffer, dataset="tweets", ingestion_time=ingestion_time)
                        logger.info(f"Buffer flushed: {len(buffer)} tweets → {s3_path}")
                        s3_paths.append(s3_path)
                        buffer.clear()
                        buffer_size = 0

                break  # key worked (or no more pages) — no need to try next key

            except CreditsExhaustedError:
                logger.warning(f"[key {key_idx}/{len(api_keys)}] out of credits — switching to next key")

    if total_fetched == 0 and not buffer:
        logger.error("All API keys exhausted — 0 tweets fetched")
        return s3_paths

    if buffer:
        s3_path = upload_to_bronze_s3(data=buffer, dataset="tweets", ingestion_time=ingestion_time)
        logger.info(f"Final flush: {len(buffer)} tweets → {s3_path}")
        s3_paths.append(s3_path)

    elapsed = (datetime.now(timezone.utc) - ingestion_time).total_seconds()
    logger.info(f"Ingestion completed in {elapsed:.2f}s — {total_fetched} tweets, {len(s3_paths)} chunk(s) uploaded")

    return s3_paths


async def main() -> None:
    query = os.getenv("QUERY")
    await run_ingestion_tweets(query=query, query_type=QueryType.LATEST, tweets_number=10000)


if __name__ == "__main__":
    asyncio.run(main())
