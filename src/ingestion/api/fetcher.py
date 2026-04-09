import asyncio
import aiohttp
from typing import AsyncGenerator

from src.ingestion.api.client import TwitterAPIClient
from src.ingestion.api.endpoints import TwitterEndpoints
from src.ingestion.api.enums.query_type import QueryType

N_CONCURRENT = 20 # chain of responsibility pattern for rate limiting

class TwitterDataFetcher:
    def __init__(self, client : TwitterAPIClient):
        self.client = client

    async def fetch_tweets(
        self, 
        session: aiohttp.ClientSession,
        query: str,
        query_type: QueryType,
        tweets_number: int = 100000,
    ) -> AsyncGenerator[list[dict], None]:
        cursors: list[str | None | bool] = [None] * N_CONCURRENT
        seen_ids: set = set()
        total_fetched = 0
        
        while total_fetched < tweets_number:
            active = [(i, c) for i, c in enumerate(cursors) if c is not False]
            if not active:
                break # no more data to fetch

            tasks = []
            indices = []
            for i, cursor in active:
                endpoint, params = TwitterEndpoints.advanced_search(query, query_type, cursor)
                tasks.append(self.client.get(session, endpoint, params))
                indices.append(i)

            result = await asyncio.gather(*tasks)

            batch: list[dict] = []
            any_has_next = False

            for i, data in zip(indices, result):
                if not data: # API error or no data
                    cursors[i] = False
                    continue

                for tweet in data.get("tweets", []):
                    if total_fetched + len(batch) >= tweets_number:
                        break

                    tid = tweet.get("id")
                    if tid and tid not in seen_ids:
                        seen_ids.add(tid)
                        batch.append(tweet)
                
                if data.get("has_next_page"):
                    cursors[i] = data.get("next_cursor")
                    any_has_next = True
                else:
                    cursors[i] = False
            
            if batch:
                total_fetched += len(batch)
                yield batch
            
            if total_fetched >= tweets_number or not any_has_next:
                break
    
    async def fetch_trends(self, session: aiohttp.ClientSession, woeid: int, count: int = 30) -> list[dict]:
        endpoint, params = TwitterEndpoints.search_trends(woeid, count)
        data = await self.client.get(session, endpoint, params)

        if not data:
            return []
        
        return data.get("trends", [])