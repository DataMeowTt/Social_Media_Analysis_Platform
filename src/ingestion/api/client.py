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
                if not response.ok:
                    body = await response.text()
                    key_hint = (self.api_key or "")[:6] + "..." if self.api_key else "None"
                    logger.error(
                        f"HTTP {response.status} from {url}\n"
                        f"  api_key : {key_hint}\n"
                        f"  body    : {body}"
                    )
                    return None, None
                return await response.json(), None
        except aiohttp.ClientError as e:
            logger.error(f"HTTP error: {e}")
            return None, None
