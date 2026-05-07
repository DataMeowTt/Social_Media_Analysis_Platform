import aiohttp
from src.utils.logger import get_logger

logger = get_logger(__name__)


class TwitterAPIClient:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self._header = {"X-API-Key": self.api_key}

    async def get(
        self, session: aiohttp.ClientSession, endpoint: str, params: dict = None
    ) -> tuple[dict | None, int | None]:
        url = f"{self.base_url}{endpoint}"
        try:
            async with session.get(url, headers=self._header, params=params) as response:
                if response.status == 429:
                    retry_after = int(response.headers.get("Retry-After", 3))
                    return None, retry_after
                response.raise_for_status()
                return await response.json(), None
        except aiohttp.ClientError as e:
            logger.error(f"HTTP error: {e}")
            return None, None
