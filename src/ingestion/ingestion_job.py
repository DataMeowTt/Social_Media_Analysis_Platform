import asyncio
import aiohttp
import json
import os
from dotenv import load_dotenv
from datetime import datetime, timezone

from src.ingestion.api.fetcher import TwitterDataFetcher
from src.ingestion.api.client import TwitterAPIClient   
from src.storage.s3.uploader import upload_to_s3
from src.ingestion.api.enums.query_type import QueryType
from src.utils.config_loader import load_config
from src.utils.logger import get_logger 

load_dotenv()
CONFIG = load_config()
logger = get_logger(__name__)

BUFFER_LIMIT_BYTES = 150 * 1024 * 1024 # 150MB

def add_ingestion_date(records: list[dict], ingestion_time: datetime) -> list[dict]:
    ingestion_date = ingestion_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    for record in records:
        record["ingestion_date"] = ingestion_date

    return records

class IngestionBuffer:
    def __init__(self):
        self._data: list[dict] = []
        self._size_bytes: int = 0
        self._lock = asyncio.Lock()

    async def add(self, records: list[dict]) -> int:
        chunk_size = sum(len(json.dumps(r, ensure_ascii=False).encode("utf-8")) for r in records)
        
        async with self._lock:
            self._data.extend(records)
            self._size_bytes += chunk_size
            return self._size_bytes

    async def flush(self) -> tuple[list[dict], int]:
        async with self._lock:
            data, size = self._data.copy(), self._size_bytes
            self._data.clear()
            self._size_bytes = 0
            
            return data, size
        
async def _upload_chunk(chunk: list[dict], ingestion_time: datetime, dataset: str, layer: str) -> str:
    loop = asyncio.get_event_loop()

    # Offload the blocking upload to a thread pool executor
    s3_path = await loop.run_in_executor(
        None,
        lambda: upload_to_s3(
            data=chunk,
            dataset=dataset,
            layer=layer,
            ingestion_time=ingestion_time
        )
    )
    logger.info(f"Chunk of {len(chunk)} records uploaded to S3 at {s3_path}")
    return s3_path

async def run_ingestion_tweets(query : str, query_type: QueryType, tweets_number: int) -> list[str]:
    ingestion_time = datetime.now(timezone.utc)
    buffer = IngestionBuffer()
    upload_tasks: list[asyncio.Task] = []

    client = TwitterAPIClient(
        api_key=os.getenv("MY_API_KEY"),
        base_url=CONFIG["api"]["url"]
    )
    fetcher = TwitterDataFetcher(client)
    
    logger.info(f"Starting ingestion for query: '{query}' with type: {query_type.name} aiming for {tweets_number} tweets")

    async with aiohttp.ClientSession() as session:
        async for batch in fetcher.fetch_tweets(session, query, query_type, tweets_number):
            batch = add_ingestion_date(batch, ingestion_time)
            current_size = await buffer.add(batch)

            if current_size >= BUFFER_LIMIT_BYTES:
                chunk, size = await buffer.flush()
                logger.info(f"Buffer limit reached ({size} bytes). Uploading chunk of {len(chunk)} records to S3...")

                upload_tasks.append(
                    asyncio.create_task(
                        _upload_chunk(
                            chunk=chunk,
                            ingestion_time=ingestion_time,
                            dataset="tweets",
                            layer="raw"
                        )
                    )
                )

    # Final flush after fetching is done
    remaining, size = await buffer.flush()
    if remaining:
        logger.info(f"final flushing: {size / 1e6:.1f} MB ({len(remaining)} tweets)")
        upload_tasks.append(
            asyncio.create_task(
                _upload_chunk(
                    chunk=remaining,
                    ingestion_time=ingestion_time,
                    dataset="tweets",
                    layer="raw"
                )
            )
        )

    results = await asyncio.gather(*upload_tasks, return_exceptions=True)
    s3_paths = []
    for r in results:
        if isinstance(r, Exception):
            logger.error(f"Error during upload: {r}")
        else:
            s3_paths.append(r)

    elapsed = (datetime.now(timezone.utc) - ingestion_time).total_seconds()
    logger.info(f"Ingestion completed in {elapsed:.2f} seconds with {len(s3_paths)} chunks uploaded to S3")

    return s3_paths