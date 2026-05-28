from __future__ import annotations

import os
from datetime import datetime, timezone

from dotenv import load_dotenv

from src.facebook.ingestion.api.client import FacebookClient
from src.facebook.ingestion.api.comment_scraper import fetch_post_with_comments
from src.facebook.ingestion.api.group_scraper import fetch_group_posts_with_comments
from src.facebook.ingestion.api.proxy_utils import select_proxy
from src.facebook.ingestion.api.url_utils import extract_group_id_from_url, extract_post_id_from_url, post_id_to_feedback_id
from src.storage.s3.uploader import upload_to_bronze_s3
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)


def _make_client() -> FacebookClient:
    cookies_raw = os.getenv("FB_COOKIES", "")
    cookies: dict = {}
    if cookies_raw:
        for part in cookies_raw.split(";"):
            part = part.strip()
            if "=" in part:
                k, v = part.split("=", 1)
                cookies[k.strip()] = v.strip()

    fb_dtsg  = os.getenv("FB_DTSG", "")
    proxies  = select_proxy(has_cookies=bool(cookies))
    return FacebookClient(proxies=proxies, cookies=cookies, fb_dtsg=fb_dtsg)


def _add_ingestion_date(posts: list[dict], ingestion_time: datetime) -> list[dict]:
    ts = ingestion_time.strftime("%Y-%m-%dT%H:%M:%SZ")
    for p in posts:
        p["ingestion_date"] = ts
    return posts


def run_simple_ingestion(post_urls: list[str] | str, page_name: str) -> None:
    if isinstance(post_urls, str):
        post_urls = [u.strip() for u in post_urls.split(",") if u.strip()]

    ingestion_time = datetime.now(timezone.utc)
    client = _make_client()
    posts  = []

    for url in post_urls:
        logger.info(f"[ingestion] resolving post URL: {url}")
        post_id = extract_post_id_from_url(url, cookies=client.cookies, proxies=client.proxies)
        if not post_id:
            logger.error(f"[ingestion] could not extract post_id from URL: {url}")
            continue
        feedback_id = post_id_to_feedback_id(post_id)
        logger.info(f"[ingestion] fetching post post_id={post_id}")
        try:
            post = fetch_post_with_comments(client, feedback_id, page_name, post_id=post_id)
            posts.append(post)
        except Exception as exc:
            logger.error(f"[ingestion] failed for post_id={post_id}: {exc}")

    if not posts:
        logger.warning("[ingestion] no posts fetched — skipping upload")
        return

    _add_ingestion_date(posts, ingestion_time)
    path = upload_to_bronze_s3(data=posts, dataset="facebook", ingestion_time=ingestion_time)
    logger.info(f"[ingestion] uploaded {len(posts)} simple posts → {path}")


def run_group_ingestion(
    group_url: str,
    limit: int = 50,
    min_comments: int = 0,
) -> None:
    ingestion_time = datetime.now(timezone.utc)
    client = _make_client()

    logger.info(f"[ingestion] resolving group URL: {group_url}")
    group_id = extract_group_id_from_url(group_url, cookies=client.cookies, proxies=client.proxies)
    if not group_id:
        logger.error(f"[ingestion] could not extract group_id from URL: {group_url}")
        return

    logger.info(f"[ingestion] fetching group_id={group_id} limit={limit} min_comments={min_comments}")
    try:
        posts = fetch_group_posts_with_comments(client, group_id, limit, min_comments)
    except Exception as exc:
        logger.error(f"[ingestion] group fetch failed: {exc}")
        return

    if not posts:
        logger.warning("[ingestion] no posts fetched — skipping upload")
        return

    _add_ingestion_date(posts, ingestion_time)
    path = upload_to_bronze_s3(data=posts, dataset="facebook", ingestion_time=ingestion_time)
    logger.info(f"[ingestion] uploaded {len(posts)} group posts → {path}")
