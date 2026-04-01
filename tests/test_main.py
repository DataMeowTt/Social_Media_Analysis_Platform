from tests.test_ingestion import test_ingestion
from src.ingestion.api.enums.query_type import QueryType


def main():
    query = "Donald Trump"
    query_type = QueryType.LATEST
    max_pages = 1

    test_ingestion(
        query=query,
        query_type=query_type,
        max_pages=max_pages
    )


if __name__ == "__main__":
    main()