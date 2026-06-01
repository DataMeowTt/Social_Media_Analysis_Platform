import asyncio
import os
from datetime import date

from src.twitter.ingestion.ingestion_job import run_ingestion_tweets
from src.twitter.ingestion.api.enums.query_type import QueryType

_DEFAULT_QUERY = (
    "(VinFast OR Tesla OR BYD OR Toyota OR BMW "
    "OR Mercedes OR Hyundai OR Ford OR Porsche OR Volkswagen) lang:en"
)


def run_ingestion_task(**_):
    base_query = os.getenv("TWITTER_QUERY", _DEFAULT_QUERY)
    query = f"{base_query} since:{date(2026, 5, 28)} until:{date(2026, 5, 29)}"
    asyncio.run(
        run_ingestion_tweets(
            query=query,
            query_type=QueryType.LATEST,
            tweets_number=100,
        )
    )
