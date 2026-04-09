import aiohttp
import asyncio
from src.utils.logger import get_logger 

logger = get_logger(__name__)

class AIMDRateLimiter:
    def __init__(
        self,
        initial_rate: float = 3.0,
        additive_increase: float = 1.0,
        multiplicative_decrease: float = 0.5,
        min_rate: float = 3.0,
        max_rate: float = 20.0
    ):
        self._rate = initial_rate
        self._ai = additive_increase
        self._md = multiplicative_decrease
        self._min_rate = min_rate
        self._max_rate = max_rate
        self._timestamps: list[float] = []
        self._lock = asyncio.Lock()
        self._success_count: int = 0
        self._success_window_start: float = 0.0

    @property
    def current_rate(self) -> float:
        return self._rate
    
    async def acquire(self) -> None:
        while True:
            async with self._lock:
                now = asyncio.get_event_loop().time()
                self._timestamps = [t for t in self._timestamps if now - t < 1.0]
                limit = max(int(self._min_rate), int(self._rate))
                if len(self._timestamps) < limit:
                    self._timestamps.append(now)
                    return
                
                wait = 1.0 - (now - self._timestamps[0])
            
            await asyncio.sleep(max(wait, 0.0))

    async def on_success(self) -> None:
        async with self._lock:
            now = asyncio.get_event_loop().time()
            if now - self._success_window_start >= 1.0:
                if self._success_count > 0:
                    self._rate = min(self._rate + self._ai, self._max_rate)
                    logger.debug(f"Rate increased to {self._rate:.2f} req/s")
                self._success_count = 0
                self._success_window_start = now
            self._success_count += 1

    async def on_throttled(self) -> None:
        async with self._lock:
            old = self._rate
            self._rate = max(self._rate * self._md, self._min_rate)
            self._timestamps.clear()
            self._success_count = 0
            logger.warning(f"Throttled! Rate decreased from {old:.2f} to {self._rate:.2f} req/s")

# Singleton - shared rate limiter across all API clients
_global_rate_limit = AIMDRateLimiter(
    initial_rate=3.0,
    additive_increase=1.0,
    multiplicative_decrease=0.5,
    min_rate=3.0,
    max_rate=20.0
)

class TwitterAPIClient:
    def __init__(self, api_key: str, base_url: str):
        self.api_key = api_key
        self.base_url = base_url
        self._header = {"X-API-Key": self.api_key}

    async def get(self, session: aiohttp.ClientSession, endpoint: str, params:dict = None):
        await _global_rate_limit.acquire()
        url = f"{self.base_url}{endpoint}"
        
        try: 
            async with session.get(url, headers=self._header, params=params) as response:
                if response.status == 429:
                    await _global_rate_limit.on_throttled()
                    await asyncio.sleep(1)  # Cooldown before retrying
                    return None
                response.raise_for_status()
                await _global_rate_limit.on_success()
                return await response.json()
        except aiohttp.ClientError as e:
            logger.error(f"HTTP error: {e}")
            return None