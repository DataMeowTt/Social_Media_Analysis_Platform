from src.ingestion.api.client import TwitterAPIClient
from src.ingestion.api.endpoints import TwitterEndpoints
from src.ingestion.api.enums.query_type import QueryType

class TwitterDataFetcher:
    def __init__(self, client : TwitterAPIClient):
        self.client = client

    def fetch_tweets(self, query: str, query_type: QueryType, cursor: str = None, max_pages = 2):
        all_tweets = []
        
        for _ in range(max_pages):
            endpoint, params = TwitterEndpoints.advanced_search(query, query_type, cursor)

            data = self.client.get(endpoint, params)

            if not data:
                break
            
            tweets = data.get("tweets", [])
            all_tweets.extend(tweets)

            if not data.get("has_next_page"):
                break

            cursor = data.get("next_cursor")

        return all_tweets
    
    def fetch_trends(self, woeid: int, count: int = 30):
        endpoint, params = TwitterEndpoints.search_trends(woeid, count)
        data = self.client.get(endpoint, params)

        if not data:
            return []
        
        return data.get("trends", [])