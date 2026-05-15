import asyncio
import aiohttp
from typing import AsyncGenerator

from src.twitter.ingestion.api.client import TwitterAPIClient
from src.twitter.ingestion.api.endpoints import TwitterEndpoints
from src.twitter.ingestion.api.enums.query_type import QueryType
from src.utils.logger import get_logger

logger = get_logger(__name__)

_CALL_DELAY = 5.0  # free tier: 1 request per 5 seconds
class TwitterDataFetcher:
    def __init__(self, client: TwitterAPIClient):
        self.client = client

    async def fetch_tweets(
        self,
        session: aiohttp.ClientSession,
        query: str,
        query_type: QueryType,
        tweets_number: int = 1000,
    ) -> AsyncGenerator[list[dict], None]:
        cursor: str | None = None
        seen_ids: set = set()
        total_fetched = 0
        first_call = True

        while total_fetched < tweets_number:
            if not first_call:
                await asyncio.sleep(_CALL_DELAY)
            first_call = False

            endpoint, params = TwitterEndpoints.advanced_search(query, query_type, cursor)
            data, retry_after = await self.client.get(session, endpoint, params)

            if data is None:
                if retry_after is not None:
                    logger.warning(f"Rate limited (429) — waiting {retry_after}s")
                    await asyncio.sleep(retry_after)
                    continue
                break

            batch: list[dict] = []
            for tweet in data.get("tweets", []):
                if total_fetched + len(batch) >= tweets_number:
                    break
                tid = tweet.get("id")
                if tid and tid not in seen_ids:
                    seen_ids.add(tid)
                    batch.append(tweet)

            if batch:
                total_fetched += len(batch)
                yield batch

            if not data.get("has_next_page") or not data.get("next_cursor"):
                break

            cursor = data.get("next_cursor")

    async def fetch_trends(self, session: aiohttp.ClientSession, woeid: int, count: int = 30) -> list[dict]:
        endpoint, params = TwitterEndpoints.search_trends(woeid, count)
        data, _ = await self.client.get(session, endpoint, params)

        if not data:
            return []

        return data.get("trends", [])
