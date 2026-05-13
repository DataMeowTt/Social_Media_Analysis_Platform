import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from datetime import datetime, timezone

from src.ingestion.api.client import CreditsExhaustedError
from src.ingestion.api.fetcher import TwitterDataFetcher
from src.ingestion.api.enums.query_type import QueryType
from src.ingestion.ingestion_job import add_ingestion_date, run_ingestion_tweets


def make_tweets(n: int, id_offset: int = 0) -> list[dict]:
    return [{"id": str(i + id_offset), "text": f"tweet {i}"} for i in range(n)]


def make_api_response(tweets: list[dict], has_next: bool = True, cursor: str = "next") -> dict:
    return {"tweets": tweets, "has_next_page": has_next, "next_cursor": cursor}


# ── add_ingestion_date ────────────────────────────────────────────────────────

def test_add_ingestion_date_format():
    records = [{"id": "1"}, {"id": "2"}]
    ts = datetime(2026, 5, 13, 10, 0, 0, tzinfo=timezone.utc)
    result = add_ingestion_date(records, ts)
    assert all(r["ingestion_date"] == "2026-05-13T10:00:00Z" for r in result)


def test_add_ingestion_date_mutates_in_place():
    records = [{"id": "1"}]
    result = add_ingestion_date(records, datetime(2026, 1, 1, tzinfo=timezone.utc))
    assert result is records


# ── TwitterDataFetcher ────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_fetch_tweets_stops_at_tweets_number():
    from itertools import count
    id_counter = count()

    def make_response(*args, **kwargs):
        tweets = [{"id": str(next(id_counter)), "text": "t"} for _ in range(20)]
        return ({"tweets": tweets, "has_next_page": True, "next_cursor": "next"}, None)

    mock_client = MagicMock()
    mock_client.get = AsyncMock(side_effect=make_response)
    fetcher = TwitterDataFetcher(mock_client)

    collected = []
    async for batch in fetcher.fetch_tweets(MagicMock(), "q", QueryType.LATEST, tweets_number=50):
        collected.extend(batch)

    assert len(collected) == 50


@pytest.mark.asyncio
async def test_fetch_tweets_deduplication():
    same_tweets = make_tweets(20)
    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=(make_api_response(same_tweets, has_next=False), None))
    fetcher = TwitterDataFetcher(mock_client)

    collected = []
    async for batch in fetcher.fetch_tweets(MagicMock(), "q", QueryType.LATEST, tweets_number=100):
        collected.extend(batch)

    ids = [t["id"] for t in collected]
    assert len(ids) == len(set(ids))


@pytest.mark.asyncio
async def test_fetch_tweets_stops_when_no_next_page():
    mock_client = MagicMock()
    mock_client.get = AsyncMock(return_value=(make_api_response(make_tweets(5), has_next=False), None))
    fetcher = TwitterDataFetcher(mock_client)

    collected = []
    async for batch in fetcher.fetch_tweets(MagicMock(), "q", QueryType.LATEST, tweets_number=10000):
        collected.extend(batch)

    assert len(collected) == 5


# ── run_ingestion_tweets ──────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_no_api_keys_returns_empty():
    with patch("src.ingestion.ingestion_job._load_api_keys", return_value=[]):
        result = await run_ingestion_tweets("q", QueryType.LATEST, 100)
    assert result == []


@pytest.mark.asyncio
async def test_run_ingestion_returns_s3_paths():
    fake_response = (make_api_response(make_tweets(20), has_next=False), None)

    with patch("src.ingestion.ingestion_job._load_api_keys", return_value=["key1"]), \
         patch("src.ingestion.ingestion_job.TwitterAPIClient") as MockClient, \
         patch("src.ingestion.ingestion_job.upload_to_bronze_s3", return_value="s3://bucket/key"):

        MockClient.return_value.get = AsyncMock(return_value=fake_response)
        result = await run_ingestion_tweets("q", QueryType.LATEST, 20)

    assert len(result) == 1
    assert result[0].startswith("s3://")


@pytest.mark.asyncio
async def test_credits_exhausted_switches_to_next_key():
    """Key 1 hết credits → dùng key 2 và hoàn thành."""
    fake_response = (make_api_response(make_tweets(10), has_next=False), None)

    def make_client(api_key, base_url):
        mock = MagicMock()
        if api_key == "key1":
            mock.get = AsyncMock(side_effect=CreditsExhaustedError("key1"))
        else:
            mock.get = AsyncMock(return_value=fake_response)
        return mock

    with patch("src.ingestion.ingestion_job._load_api_keys", return_value=["key1", "key2"]), \
         patch("src.ingestion.ingestion_job.TwitterAPIClient", side_effect=make_client), \
         patch("src.ingestion.ingestion_job.upload_to_bronze_s3", return_value="s3://bucket/key"):

        result = await run_ingestion_tweets("q", QueryType.LATEST, 10)

    assert len(result) == 1


@pytest.mark.asyncio
async def test_all_keys_exhausted_returns_empty():
    def make_client(api_key, base_url):
        mock = MagicMock()
        mock.get = AsyncMock(side_effect=CreditsExhaustedError(api_key))
        return mock

    with patch("src.ingestion.ingestion_job._load_api_keys", return_value=["key1", "key2"]), \
         patch("src.ingestion.ingestion_job.TwitterAPIClient", side_effect=make_client):

        result = await run_ingestion_tweets("q", QueryType.LATEST, 100)

    assert result == []
