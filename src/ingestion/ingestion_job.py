from src.ingestion.api.fetcher import TwitterDataFetcher
from src.ingestion.api.client import TwitterAPIClient   
from src.storage.s3.uploader import upload_to_s3
from src.ingestion.api.enums.query_type import QueryType

from dotenv import load_dotenv
from src.utils.config_loader import load_config
from src.utils.logger import get_logger 
from datetime import datetime, timezone
import os

load_dotenv()
CONFIG = load_config()
logger = get_logger(__name__)

def add_ingestion_date(records: list[dict], ingestion_time: datetime) -> list[dict]:
    ingestion_date = ingestion_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    for record in records:
        record["ingestion_date"] = ingestion_date

    return records

def run_ingestion_tweets(query : str, query_type: QueryType, max_pages: int = 20) -> tuple[str, list[dict], datetime]:
    logger.info(f"Starting ingestion for query: {query}, query_type: {query_type}, max_pages: {max_pages}") 
   
    client = TwitterAPIClient(
        api_key = os.getenv("MY_API_KEY"),
        base_url = CONFIG["api"]["url"]
    )
    fetcher = TwitterDataFetcher(client)

    ingestion_time = datetime.now(timezone.utc)

    logger.info("Fetching tweets from Twitter API...")
    tweets = fetcher.fetch_tweets(
        query=query, 
        query_type=query_type, 
        max_pages=max_pages
    )

    if not tweets:
        logger.warning("No tweets fetched for the given query and query type.")
        return None
    
    logger.info(f"Fetched {len(tweets)} tweets. Adding ingestion date...")
    
    tweets = add_ingestion_date(tweets, ingestion_time)
    
    logger.info("Uploading data to S3...")
    s3_path = upload_to_s3(
        data=tweets,
        dataset="tweets",
        layer="raw",
        ingestion_time=ingestion_time
    )
    logger.info(f"Ingestion completed. Data uploaded to: {s3_path}")
    
    logger.info(f"Total ingestion time: {(datetime.now(timezone.utc) - ingestion_time).total_seconds()} seconds")
    return s3_path, tweets, ingestion_time

