from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from src.utils.logger import get_logger

logger = get_logger(__name__)


class YouTubeAPIClient:
    def __init__(self, api_key: str):
        self._youtube = build("youtube", "v3", developerKey=api_key)

    def get_comment_threads(self, video_id: str, page_token: str | None = None) -> dict | None:
        try:
            return (
                self._youtube.commentThreads()
                .list(
                    part="snippet,replies",
                    videoId=video_id,
                    maxResults=100,
                    pageToken=page_token,
                )
                .execute()
            )
        except HttpError as e:
            logger.error(f"YouTube API error for video {video_id}: {e}")
            return None
