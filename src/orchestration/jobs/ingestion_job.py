import asyncio
import os

from src.ingestion.ingestion_job import run_ingestion_tweets
from src.ingestion.api.enums.query_type import QueryType

_QUERY = (
    "(VinFast OR Tesla OR BYD OR Toyota OR BMW "
    "OR Mercedes OR Hyundai OR Ford OR Porsche OR Volkswagen) lang:en"
)

def run_ingestion_task(**context):
    asyncio.run(
        run_ingestion_tweets(
            query=_QUERY,
            query_type=QueryType.LATEST,
            tweets_number=int(os.getenv("TWEETS_NUMBER", "10000")),
        )
    )
