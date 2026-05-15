import asyncio
import os
from datetime import date

from src.ingestion.ingestion_job import run_ingestion_tweets
from src.ingestion.api.enums.query_type import QueryType

_QUERY_BASE = (
    "(VinFast OR Tesla OR BYD OR Toyota OR BMW "
    "OR Mercedes OR Hyundai OR Ford OR Porsche OR Volkswagen) lang:en"
)

def run_ingestion_task(**context):
    query = f"{_QUERY_BASE} since:{date.today()}"
    asyncio.run(
        run_ingestion_tweets(
            query=query,
            query_type=QueryType.LATEST,
            tweets_number=int(os.getenv("TWEETS_NUMBER", "500")),
        )
    )
