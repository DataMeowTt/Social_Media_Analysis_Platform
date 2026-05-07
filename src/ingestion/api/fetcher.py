import asyncio
import aiohttp
from typing import AsyncGenerator

from src.ingestion.api.client import TwitterAPIClient
from src.ingestion.api.endpoints import TwitterEndpoints
from src.ingestion.api.enums.query_type import QueryType
from src.utils.logger import get_logger

logger = get_logger(__name__)

_CALL_DELAY = 0.4  # QPS limit = 3 → min interval = 0.333s, dùng 0.4s để có margin

# --- Adaptive rate limiting (commented out) ---
# _QPS_LIMIT = 3
# _MIN_DELAY = 1.0 / _QPS_LIMIT        # 0.333s — hard floor from QPS=3
# _BASELINE_DELAY = _MIN_DELAY * 1.2   # 0.4s  — 20% margin above QPS limit
# _MAX_DELAY = 30.0


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

        # --- Adaptive fetch_tweets (commented out) ---
        # call_delay = _BASELINE_DELAY
        # ...
        # if retry_after is not None:
        #     logger.warning(f"Rate limited (429) — waiting {retry_after}s, then bumping delay {call_delay:.2f}s → {min(call_delay * 1.5, _MAX_DELAY):.2f}s")
        #     await asyncio.sleep(retry_after)
        #     call_delay = min(call_delay * 1.5, _MAX_DELAY)
        #     data, retry_after = await self.client.get(session, endpoint, params)
        #     if data is None:
        #         logger.error("Still rate limited after retry — stopping")
        #         break
        # call_delay = max(call_delay * 0.9, _BASELINE_DELAY)

    async def fetch_trends(self, session: aiohttp.ClientSession, woeid: int, count: int = 30) -> list[dict]:
        endpoint, params = TwitterEndpoints.search_trends(woeid, count)
        data, _ = await self.client.get(session, endpoint, params)

        if not data:
            return []

        return data.get("trends", [])
