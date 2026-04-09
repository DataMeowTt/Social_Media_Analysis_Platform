import asyncio
import pytest
from unittest.mock import AsyncMock, patch, MagicMock
from datetime import datetime, timezone

from src.ingestion.api.client import AIMDRateLimiter, TwitterAPIClient
from src.ingestion.api.fetcher import TwitterDataFetcher
from src.ingestion.api.enums.query_type import QueryType
from src.ingestion.ingestion_job import IngestionBuffer, run_ingestion_tweets

def make_tweets(n: int, id_offset: int = 0) -> list[dict]:
    return [{"id": str(i + id_offset), "text": f"tweet {i}"} for i in range(n)]

def make_api_response(tweets: list[dict], has_next: bool = True, cursor: str = "next") -> dict:
    return {"tweets": tweets, "has_next_page": has_next, "next_cursor": cursor}

@pytest.mark.asyncio
async def test_rate_limiter_acquire_respects_limit():
    """Không cho phép vượt quá limit trong 1s."""
    limiter = AIMDRateLimiter(initial_rate=3.0, min_rate=3.0, max_rate=20.0)
    import time
    start = asyncio.get_event_loop().time()
    for _ in range(6):  # 2x limit → phải chờ
        await limiter.acquire()
    elapsed = asyncio.get_event_loop().time() - start
    assert elapsed >= 1.0, "Phải mất ít nhất 1s khi vượt rate limit"

@pytest.mark.asyncio
async def test_rate_limiter_on_success_increases_rate():
    limiter = AIMDRateLimiter(initial_rate=3.0, additive_increase=1.0, min_rate=3.0, max_rate=20.0)
    initial = limiter.current_rate

    await limiter.on_success()
    await asyncio.sleep(1.1)  # đảm bảo window mới
    await limiter.on_success()

    assert limiter.current_rate == initial + 1.0

@pytest.mark.asyncio
async def test_rate_limiter_on_throttled_decreases_rate():
    limiter = AIMDRateLimiter(initial_rate=10.0, multiplicative_decrease=0.5, min_rate=3.0, max_rate=20.0)
    await limiter.on_throttled()
    assert limiter.current_rate == 5.0

@pytest.mark.asyncio
async def test_rate_limiter_not_below_min_rate():
    limiter = AIMDRateLimiter(initial_rate=3.0, multiplicative_decrease=0.5, min_rate=3.0, max_rate=20.0)
    await limiter.on_throttled()
    assert limiter.current_rate == 3.0  # không được xuống dưới min

@pytest.mark.asyncio
async def test_buffer_add_returns_size():
    buf = IngestionBuffer()
    size = await buf.add([{"id": "1", "text": "hello"}])
    assert size > 0

@pytest.mark.asyncio
async def test_buffer_flush_clears_data():
    buf = IngestionBuffer()
    await buf.add(make_tweets(5))
    data, size = await buf.flush()
    assert len(data) == 5
    assert size > 0
    # Sau flush phải rỗng
    data2, size2 = await buf.flush()
    assert data2 == []
    assert size2 == 0

@pytest.mark.asyncio
async def test_buffer_concurrent_add():
    """Nhiều coroutine add đồng thời không bị race condition."""
    buf = IngestionBuffer()
    await asyncio.gather(*[buf.add(make_tweets(10, i * 10)) for i in range(5)])
    data, _ = await buf.flush()
    assert len(data) == 50

@pytest.mark.asyncio
async def test_fetch_tweets_stops_at_tweets_number():
    from itertools import count
    id_counter = count()

    def make_unique_response(*args, **kwargs):
        tweets = [{"id": str(next(id_counter)), "text": "tweet"} for _ in range(20)]
        return {"tweets": tweets, "has_next_page": True, "next_cursor": "next"}

    mock_client = MagicMock()
    mock_client.get = AsyncMock(side_effect=make_unique_response)

    fetcher = TwitterDataFetcher(mock_client)

    collected = []
    async for batch in fetcher.fetch_tweets(MagicMock(), "test", QueryType.LATEST, tweets_number=50):
        collected.extend(batch)

    assert len(collected) == 50

@pytest.mark.asyncio
async def test_fetch_tweets_deduplication():
    same_tweets = make_tweets(20, id_offset=0)
    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=make_api_response(same_tweets, has_next=False))  # has_next=False

    fetcher = TwitterDataFetcher(mock_client)

    collected = []
    async for batch in fetcher.fetch_tweets(MagicMock(), "test", QueryType.LATEST, tweets_number=100):
        collected.extend(batch)

    ids = [t["id"] for t in collected]
    assert len(ids) == len(set(ids))

@pytest.mark.asyncio
async def test_fetch_tweets_stops_when_no_next_page():
    """Dừng lại khi tất cả chain báo hết dữ liệu."""
    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=make_api_response(make_tweets(5), has_next=False))

    fetcher = TwitterDataFetcher(mock_client)
    mock_session = MagicMock()

    collected = []
    async for batch in fetcher.fetch_tweets(mock_session, "test", QueryType.LATEST, tweets_number=10000):
        collected.extend(batch)

    # Chỉ lấy được 5 * 20 chains = 100 tweets (tất cả unique) rồi dừng
    assert len(collected) <= 20 * 5  # 20 chains × 5 tweets/chain

@pytest.mark.asyncio
async def test_run_ingestion_tweets_returns_s3_paths():
    """End-to-end: mock API + mock upload, kiểm tra trả về list s3 paths."""
    fake_response = make_api_response(make_tweets(20), has_next=False)

    with patch("src.ingestion.ingestion_job.TwitterAPIClient") as MockClient, \
         patch("src.ingestion.ingestion_job.upload_to_s3", return_value="s3://bucket/key"):

        mock_instance = MockClient.return_value
        mock_instance.get = AsyncMock(return_value=fake_response)

        result = await run_ingestion_tweets(
            query="test query",
            query_type=QueryType.LATEST,
            tweets_number=100,
        )

    assert isinstance(result, list)
    assert len(result) > 0
    assert all(s.startswith("s3://") for s in result)

@pytest.mark.asyncio
async def test_run_ingestion_tweets_no_data():
    """Khi API không trả về dữ liệu, không crash và trả về list rỗng."""
    with patch("src.ingestion.ingestion_job.TwitterAPIClient") as MockClient:
        mock_instance = MockClient.return_value
        mock_instance.get = AsyncMock(return_value=None)

        result = await run_ingestion_tweets(
            query="test query",
            query_type=QueryType.LATEST,
            tweets_number=100,
        )

    assert result == []
