import os

from src.facebook.ingestion.ingestion_job import run_group_ingestion, run_simple_ingestion

_MODE       = "post"
_GROUP_URL  = "https://www.facebook.com/groups/1167278443782686/?locale=vi_VN"
_LIMIT      = 10
_MIN_COMMENTS = 5
_POST_URLS  = "https://www.facebook.com/groups/vf3clubvn369/permalink/4545974505634471/?locale=vi_VN"
_PAGE_NAME  = "VinFast VF3 Việt Nam - VINNO CLUB"

if __name__ == "__main__":
    if _MODE == "group":
        if not _GROUP_URL:
            raise ValueError("FB_GROUP_URL env var is required for group ingestion mode")
        run_group_ingestion(group_url=_GROUP_URL, limit=_LIMIT, min_comments=_MIN_COMMENTS)
    else:
        if not _POST_URLS:
            raise ValueError("FB_POST_URLS env var is required for simple ingestion mode")
        run_simple_ingestion(post_urls=_POST_URLS, page_name=_PAGE_NAME)
