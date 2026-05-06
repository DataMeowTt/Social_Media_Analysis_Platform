import asyncio
import aiohttp
from typing import AsyncGenerator

from src.ingestion.api.client import TwitterAPIClient
from src.ingestion.api.endpoints import TwitterEndpoints
from src.ingestion.api.enums.query_type import QueryType

class TwitterDataFetcher:
    def __init__(self, client: TwitterAPIClient):
        self.client = client

    async def fetch_tweets(
        self,
        session: aiohttp.ClientSession,
        query: str,
        query_type: QueryType,
        tweets_number: int = 100000,
        call_delay: float = 3.0,
    ) -> AsyncGenerator[list[dict], None]:
        cursor: str | None = None
        seen_ids: set = set()
        total_fetched = 0
        first_call = True

        while total_fetched < tweets_number:
            if not first_call:
                await asyncio.sleep(call_delay)
            first_call = False

            endpoint, params = TwitterEndpoints.advanced_search(query, query_type, cursor)
            data = await self.client.get(session, endpoint, params)

            if not data:
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
        data = await self.client.get(session, endpoint, params)

        if not data:
            return []
        
        return data.get("trends", [])