from src.youtube.ingestion.api.client import YouTubeAPIClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


class YouTubeDataFetcher:
    def __init__(self, client: YouTubeAPIClient):
        self.client = client

    def fetch_comments(self, video_id: str, num_comments: int) -> list[dict]:
        comments: list[dict] = []
        page_token: str | None = None
        page = 1

        while len(comments) < num_comments:
            logger.info(f"[video {video_id}] fetching page {page} ({len(comments)}/{num_comments})...")
            response = self.client.get_comment_threads(video_id, page_token)

            if response is None:
                break

            for item in response.get("items", []):
                if len(comments) >= num_comments:
                    break

                thread_id = item["id"]
                top_snippet = item["snippet"]["topLevelComment"]["snippet"]

                comments.append({
                    "comment_id": thread_id,
                    "parent_id": None,
                    "author": top_snippet.get("authorDisplayName"),
                    "text": top_snippet.get("textOriginal"),
                    "likes": top_snippet.get("likeCount", 0),
                    "published_at": top_snippet.get("publishedAt"),
                    "is_reply": False,
                })

                for reply in item.get("replies", {}).get("comments", []):
                    if len(comments) >= num_comments:
                        break
                    reply_snippet = reply["snippet"]
                    comments.append({
                        "comment_id": reply["id"],
                        "parent_id": thread_id,
                        "author": reply_snippet.get("authorDisplayName"),
                        "text": reply_snippet.get("textOriginal"),
                        "likes": reply_snippet.get("likeCount", 0),
                        "published_at": reply_snippet.get("publishedAt"),
                        "is_reply": True,
                    })

            page_token = response.get("nextPageToken")
            if not page_token:
                break

            page += 1

        return comments
