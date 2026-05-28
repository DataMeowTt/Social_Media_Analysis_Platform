"""
Temporary: patch text cho post 4548950928670162 trong raw layer.
Sau đó re-trigger facebook_processing + facebook_analytics DAG trên Airflow.

Usage:
    FB_COOKIES=... FB_DTSG=... python patch_raw_text.py
"""
import gzip
import json
import os
import re
from html import unescape

import requests
from dotenv import load_dotenv

load_dotenv()

from src.facebook.ingestion.api.client import FacebookClient
from src.facebook.ingestion.api.proxy_utils import select_proxy
from src.facebook.ingestion.api.url_utils import post_id_to_feedback_id
from src.utils.config_loader import load_config
from src.utils.session import get_s3_client

POST_ID        = "4548950928670162"
YEAR, MONTH, DAY = "2026", "05", "27"
RAW_PREFIX     = f"raw/facebook/year={YEAR}/month={MONTH}/day={DAY}/"

config = load_config()
BUCKET = config["s3"]["bucket_name"]

_PERMALINK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _make_client() -> FacebookClient:
    cookies_raw = os.getenv("FB_COOKIES", "")
    cookies: dict = {}
    if cookies_raw:
        for part in cookies_raw.split(";"):
            part = part.strip()
            if "=" in part:
                k, v = part.split("=", 1)
                cookies[k.strip()] = v.strip()
    return FacebookClient(
        proxies=select_proxy(has_cookies=bool(cookies)),
        cookies=cookies,
        fb_dtsg=os.getenv("FB_DTSG", ""),
    )


def _fetch_text_from_permalink(client: FacebookClient, permalink_url: str) -> str:
    r = requests.get(
        permalink_url,
        headers=_PERMALINK_HEADERS,
        cookies=client.cookies,
        proxies=client.proxies or None,
        timeout=30,
        allow_redirects=True,
    )
    if r.status_code != 200:
        raise RuntimeError(f"HTTP {r.status_code}")
    html = r.text

    m = re.search(r'"message":\{"text":"((?:[^"\\]|\\.)*)","ranges":', html)
    if m:
        try:
            text = json.loads(f'"{m.group(1)}"').strip()
            if len(text) > 10:
                return text
        except Exception:
            pass

    m = re.search(r'"message":\{"text":"((?:[^"\\]|\\.){10,}?)"\}', html)
    if m:
        try:
            text = json.loads(f'"{m.group(1)}"').strip()
            if len(text) > 10:
                return text
        except Exception:
            pass

    m = re.search(
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']{5,})["\']',
        html, re.IGNORECASE,
    )
    if m:
        return unescape(m.group(1)).strip()

    raise RuntimeError("Cannot extract post text from HTML")


def _fetch_permalink_from_group_api(client: FacebookClient) -> str:
    from src.facebook.ingestion.api.comment_scraper import _fetch_comments_page
    feedback_id = post_id_to_feedback_id(POST_ID)
    j = _fetch_comments_page(client, feedback_id, cursor=None)
    edges = (
        j.get("data", {})
         .get("node", {})
         .get("comment_rendering_instance_for_feed_location", {})
         .get("comments", {})
         .get("edges", [])
    )
    if not edges:
        raise RuntimeError("No comments found — cannot get permalink")
    raw_url = edges[0].get("node", {}).get("feedback", {}).get("url", "")
    if not raw_url:
        raise RuntimeError("No url in first comment feedback")
    return raw_url.split("?")[0].rstrip("/") + "/"


def patch_raw_layer(post_text: str) -> None:
    s3 = get_s3_client()
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=RAW_PREFIX)
    keys = [obj["Key"] for obj in resp.get("Contents", []) if obj["Key"].endswith(".json.gz")]

    if not keys:
        print(f"[ERROR] No files found at s3://{BUCKET}/{RAW_PREFIX}")
        return

    patched = 0
    for key in keys:
        print(f"[raw] checking s3://{BUCKET}/{key} ...")
        obj   = s3.get_object(Bucket=BUCKET, Key=key)
        data  = json.loads(gzip.decompress(obj["Body"].read()).decode("utf-8"))
        dirty = False

        for record in data:
            if record.get("post_id") == POST_ID and not record.get("text"):
                record["text"] = post_text
                dirty = True
                print(f"  → patched post_id={POST_ID}")

        if dirty:
            body = gzip.compress(json.dumps(data, ensure_ascii=False).encode("utf-8"))
            s3.put_object(Bucket=BUCKET, Key=key, Body=body,
                          ContentType="application/json", ContentEncoding="gzip")
            print(f"  → re-uploaded s3://{BUCKET}/{key}")
            patched += 1

    if patched == 0:
        print("[raw] No matching records found with null text — nothing patched")
    else:
        print(f"[raw] Done — patched {patched} file(s)")


if __name__ == "__main__":
    print("=== Step 1: Fetch post text from permalink ===")
    client    = _make_client()
    permalink = _fetch_permalink_from_group_api(client)
    print(f"Permalink: {permalink}")

    post_text = _fetch_text_from_permalink(client, permalink)
    print(f"Text ({len(post_text)} chars): {post_text[:200]}...")

    print("\n=== Step 2: Patch raw layer ===")
    patch_raw_layer(post_text)

    print("""
=== Step 3: Re-trigger Airflow DAGs ===
Trên Airflow UI, trigger manual với date 2026-05-27:
  1. facebook_processing  (cập nhật silver)
  2. facebook_analytics   (cập nhật dim_posts với Gemini sentiment)
""")
