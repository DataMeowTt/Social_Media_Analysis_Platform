import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from src.youtube.ingestion.api.client import YouTubeAPIClient
from src.youtube.ingestion.api.fetcher import YouTubeDataFetcher
from src.storage.s3.uploader import upload_to_bronze_s3
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


def run_ingestion_comments(video_id: str, num_comments: int) -> str | None:
    api_key = os.getenv("YOUTUBE_API_KEY")
    if not api_key:
        logger.error("YOUTUBE_API_KEY not set in .env")
        return None

    ingestion_time = datetime.now(timezone.utc)
    logger.info(f"Starting YouTube ingestion: video={video_id} | target={num_comments} comments")

    client = YouTubeAPIClient(api_key)
    fetcher = YouTubeDataFetcher(client)
    comments = fetcher.fetch_comments(video_id, num_comments)

    if not comments:
        logger.error(f"0 comments fetched for video {video_id}")
        return None

    s3_path = upload_to_bronze_s3(data=comments, dataset="youtube", ingestion_time=ingestion_time)
    logger.info(f"Ingestion complete: {len(comments)} comments → {s3_path}")
    return s3_path
