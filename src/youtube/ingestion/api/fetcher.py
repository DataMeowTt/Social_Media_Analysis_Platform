from typing import Generator

from src.youtube.ingestion.api.client import YouTubeAPIClient
from src.utils.logger import get_logger

logger = get_logger(__name__)


class YouTubeDataFetcher:
    def __init__(self, client: YouTubeAPIClient):
        self.client = client

    def fetch_comments(
        self, video_id: str, num_comments: int | None = None
    ) -> Generator[list[dict], None, None]:
        fetched = 0
        page_token: str | None = None
        page = 1

        while True:
            if num_comments and fetched >= num_comments:
                break

            progress = f"{fetched}/{num_comments}" if num_comments else str(fetched)
            logger.info(f"[video {video_id}] fetching page {page} ({progress})...")
            response = self.client.get_comment_threads(video_id, page_token)

            if response is None:
                break

            batch: list[dict] = []
            for item in response.get("items", []):
                if num_comments and fetched + len(batch) >= num_comments:
                    break

                thread_id = item["id"]
                top_snippet = item["snippet"]["topLevelComment"]["snippet"]

                batch.append({
                    "comment_id": thread_id,
                    "parent_id": None,
                    "author": top_snippet.get("authorDisplayName"),
                    "text": top_snippet.get("textOriginal"),
                    "likes": top_snippet.get("likeCount", 0),
                    "published_at": top_snippet.get("publishedAt"),
                    "is_reply": False,
                })

                total_reply_count = item["snippet"].get("totalReplyCount", 0)
                inline_replies = item.get("replies", {}).get("comments", [])

                if total_reply_count > len(inline_replies):
                    reply_page_token = None
                    while True:
                        reply_response = self.client.get_reply_comments(thread_id, reply_page_token)
                        if reply_response is None:
                            break
                        for reply in reply_response.get("items", []):
                            reply_snippet = reply["snippet"]
                            batch.append({
                                "comment_id": reply["id"],
                                "parent_id": thread_id,
                                "author": reply_snippet.get("authorDisplayName"),
                                "text": reply_snippet.get("textOriginal"),
                                "likes": reply_snippet.get("likeCount", 0),
                                "published_at": reply_snippet.get("publishedAt"),
                                "is_reply": True,
                            })
                        reply_page_token = reply_response.get("nextPageToken")
                        if not reply_page_token:
                            break
                else:
                    for reply in inline_replies:
                        reply_snippet = reply["snippet"]
                        batch.append({
                            "comment_id": reply["id"],
                            "parent_id": thread_id,
                            "author": reply_snippet.get("authorDisplayName"),
                            "text": reply_snippet.get("textOriginal"),
                            "likes": reply_snippet.get("likeCount", 0),
                            "published_at": reply_snippet.get("publishedAt"),
                            "is_reply": True,
                        })

            if batch:
                fetched += len(batch)
                yield batch

            page_token = response.get("nextPageToken")
            if not page_token:
                break

            page += 1
