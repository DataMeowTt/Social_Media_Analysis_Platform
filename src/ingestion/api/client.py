import asyncio
import aiohttp
from src.utils.logger import get_logger

logger = get_logger(__name__)


class TwitterAPIClient:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self._header = {"X-API-Key": self.api_key}

    async def get(self, session: aiohttp.ClientSession, endpoint: str, params: dict = None):
        url = f"{self.base_url}{endpoint}"
        wait = 5
        for attempt in range(3):
            try:
                async with session.get(url, headers=self._header, params=params) as response:
                    if response.status == 429:
                        logger.warning(f"Rate limited (429) — waiting {wait}s (attempt {attempt + 1}/3)")
                        await asyncio.sleep(wait)
                        wait *= 2
                        continue
                    response.raise_for_status()
                    return await response.json()
            except aiohttp.ClientError as e:
                logger.error(f"HTTP error: {e}")
                return None
        logger.error("Max retries reached — giving up")
        return None
