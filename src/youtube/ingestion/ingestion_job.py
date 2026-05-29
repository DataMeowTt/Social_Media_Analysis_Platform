import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from src.youtube.ingestion.api.client import YouTubeAPIClient
from src.youtube.ingestion.api.fetcher import YouTubeDataFetcher
from src.storage.s3.uploader import upload_to_bronze_s3
from src.utils.constants import BUFFER_LIMIT_BYTES
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


def run_ingestion_comments(video_id: str, num_comments: int | None = None) -> list[str]:
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        logger.error("YOUTUBE_API_KEY not set in .env")
        return []

    ingestion_time = datetime.now(timezone.utc)
    target = num_comments or "all"
    logger.info(f"Starting YouTube ingestion: video={video_id} | target={target} comments")

    client = YouTubeAPIClient(api_key)
    fetcher = YouTubeDataFetcher(client)

    buffer: list[dict] = []
    buffer_size = 0
    s3_paths: list[str] = []
    total_fetched = 0

    for batch in fetcher.fetch_comments(video_id, num_comments):
        buffer.extend(batch)
        total_fetched += len(batch)
        buffer_size += sum(len(json.dumps(r, ensure_ascii=False).encode()) for r in batch)

        if buffer_size >= BUFFER_LIMIT_BYTES:
            s3_path = upload_to_bronze_s3(data=buffer, dataset="youtube", ingestion_time=ingestion_time, youtube_id=video_id)
            logger.info(f"Buffer flushed: {len(buffer)} comments → {s3_path}")
            s3_paths.append(s3_path)
            buffer.clear()
            buffer_size = 0

    if total_fetched == 0:
        logger.error(f"0 comments fetched for video {video_id}")
        return s3_paths

    if buffer:
        s3_path = upload_to_bronze_s3(data=buffer, dataset="youtube", ingestion_time=ingestion_time, youtube_id=video_id)
        logger.info(f"Final flush: {len(buffer)} comments → {s3_path}")
        s3_paths.append(s3_path)

    elapsed = (datetime.now(timezone.utc) - ingestion_time).total_seconds()
    logger.info(f"Ingestion complete in {elapsed:.2f}s — {total_fetched} comments, {len(s3_paths)} chunk(s)")
    return s3_paths


if __name__ == "__main__":
    video_id = "nHkKJ87FS6s"
    run_ingestion_comments(video_id=video_id)
