from __future__ import annotations

import base64
import re

import requests

from src.utils.logger import get_logger

logger = get_logger(__name__)

_GRAPHQL_URL = "https://www.facebook.com/api/graphql/"
_UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"


def extract_group_id_from_url(url: str, cookies: dict | None = None, proxies: dict | None = None) -> str | None:
    for pattern in [r"/groups/(\d+)", r"group_id=(\d+)", r"gid=(\d+)"]:
        m = re.search(pattern, url)
        if m:
            return m.group(1)

    headers = {"User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9"}
    try:
        resp = requests.get(url, headers=headers, cookies=cookies or {}, proxies=proxies, timeout=20)
        html = resp.text
        for pattern in [r'fb://group/(\d+)', r'fb://group/\?id=(\d+)', r'"group_id":"(\d+)"', r'"groupID":"(\d+)"']:
            m = re.search(pattern, html)
            if m:
                return m.group(1)
    except Exception as exc:
        logger.warning(f"[url_utils] could not resolve group_id from URL: {exc}")
    return None


def extract_post_id_from_url(url: str, cookies: dict | None = None, proxies: dict | None = None) -> str | None:
    for pattern in [r"/groups/[^/]+/posts/(\d+)", r"/posts/(\d+)"]:
        m = re.search(pattern, url)
        if m:
            return m.group(1)

    from html import unescape
    headers = {"User-Agent": _UA, "Accept-Language": "en-US,en;q=0.9"}
    try:
        resp = requests.get(url, headers=headers, cookies=cookies or {}, proxies=proxies, timeout=20)
        html = resp.text

        if cookies:
            m = re.search(r'"storyID":"([^"]+)"', html)
            if m:
                try:
                    decoded = base64.b64decode(m.group(1)).decode("utf-8")
                    parts = decoded.split(":")
                    if len(parts) >= 2:
                        return parts[-1]
                except Exception:
                    pass

        m = re.search(r'<meta property="og:url" content="([^"]+)"', html)
        if m:
            og_url = unescape(m.group(1))
            m2 = re.search(r"/posts/(?:[^/]+/)?(\d+)", og_url) or re.search(r"story_fbid=(\d+)", og_url)
            if m2:
                return m2.group(1)
    except Exception as exc:
        logger.warning(f"[url_utils] could not resolve post_id from URL: {exc}")
    return None


def post_id_to_feedback_id(post_id: str) -> str:
    return base64.b64encode(f"feedback:{post_id}".encode()).decode()
