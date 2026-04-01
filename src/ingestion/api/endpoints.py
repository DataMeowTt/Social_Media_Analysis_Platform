from src.ingestion.api.enums.query_type import QueryType
from src.ingestion.api.enums.woeid import Woeid

class TwitterEndpoints:
    SEARCH_TWEETS = "/twitter/tweet/advanced_search"
    SEARCH_TRENDS = "/twitter/trends"

    @staticmethod
    def advanced_search(query: str, query_type: QueryType, cursor: str = None):
        params = {
            "query": query,
            "query_type": query_type.value
        }

        if cursor:
            params["cursor"] = cursor

        return TwitterEndpoints.SEARCH_TWEETS, params
    
    @staticmethod
    def search_trends(woeid: Woeid, count: int = 30):
        params = {
            "woeid": woeid.value,
            "count": count
        }
        return TwitterEndpoints.SEARCH_TRENDS, params