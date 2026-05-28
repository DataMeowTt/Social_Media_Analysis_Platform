from __future__ import annotations

import json
import re
from html import unescape

import requests

from src.facebook.ingestion.api.client import FacebookClient
from src.utils.logger import get_logger

logger = get_logger(__name__)

_DOC_COMMENTS = "25550760954572974"
_DOC_REPLIES  = "26570577339199586"

_PERMALINK_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


def _extract_post_text(html: str) -> str:
    # Full text from relay JSON: "message":{"text":"...","ranges":
    m = re.search(r'"message":\{"text":"((?:[^"\\]|\\.)*)","ranges":', html)
    if m:
        try:
            text = json.loads(f'"{m.group(1)}"').strip()
            if len(text) > 10:
                return text
        except Exception:
            pass
    # "message":{"text":"..."} without ranges
    m = re.search(r'"message":\{"text":"((?:[^"\\]|\\.){10,}?)"\}', html)
    if m:
        try:
            text = json.loads(f'"{m.group(1)}"').strip()
            if len(text) > 10:
                return text
        except Exception:
            pass
    # og:description fallback (truncated ~300 chars)
    m = re.search(
        r'<meta[^>]+property=["\']og:description["\'][^>]+content=["\']([^"\']{5,})["\']',
        html,
        re.IGNORECASE,
    )
    if m:
        return unescape(m.group(1)).strip()
    return ""


def _fetch_post_text_from_permalink(client: FacebookClient, permalink_url: str) -> str:
    try:
        r = requests.get(
            permalink_url,
            headers=_PERMALINK_HEADERS,
            cookies=client.cookies,
            proxies=client.proxies or None,
            timeout=30,
            allow_redirects=True,
        )
        if r.status_code == 200:
            return _extract_post_text(r.text)
    except Exception as exc:
        logger.warning(f"[comment_scraper] fetch post text from permalink failed: {exc}")
    return ""


def _parse(response_text: str) -> dict:
    text = response_text.strip()
    if text.startswith("for (;;);"):
        text = text[len("for (;;);"):]
    return json.loads(text.split("\n")[0].strip())


def _fetch_comments_page(client: FacebookClient, feedback_id: str, cursor: str | None) -> dict:
    variables = {
        "commentsAfterCount":  -1,
        "commentsAfterCursor": cursor,
        "commentsIntentToken": "REVERSE_CHRONOLOGICAL_UNFILTERED_INTENT_V1",
        "feedLocation":        "DEDICATED_COMMENTING_SURFACE",
        "focusCommentID":      None,
        "scale":               2,
        "useDefaultActor":     False,
        "id":                  feedback_id,
    }
    r = client.post_graphql(_DOC_COMMENTS, variables, "CommentsListComponentsPaginationQuery")
    return _parse(r.text)


def _fetch_replies_page(client: FacebookClient, comment_feedback_id: str, expansion_token: str) -> dict:
    variables = {
        "clientKey":       None,
        "expansionToken":  expansion_token,
        "feedLocation":    "POST_PERMALINK_DIALOG",
        "focusCommentID":  None,
        "scale":           2,
        "useDefaultActor": False,
        "id":              comment_feedback_id,
    }
    r = client.post_graphql(_DOC_REPLIES, variables, "Depth1CommentsListPaginationQuery")
    return _parse(r.text)


def _extract_replies(client: FacebookClient, raw_comment: dict) -> list[dict]:
    try:
        j = _fetch_replies_page(client, raw_comment["_feedback_id"], raw_comment["_expansion_token"])
        edges = (
            j.get("data", {})
             .get("node", {})
             .get("replies_connection", {})
             .get("edges", [])
        )
        replies = []
        for e in edges:
            n = e["node"]
            author = n.get("author") or {}
            author_id = author.get("id", "")
            replies.append({
                "author_id":      author_id,
                "author_name":    author.get("name", ""),
                "author_url":     f"https://www.facebook.com/profile.php?id={author_id}" if author_id else "",
                "text":           (n.get("body") or {}).get("text", ""),
                "reaction_count": (n.get("feedback", {}).get("reactors", {}).get("count_reduced", "0")),
            })
        return replies
    except Exception as exc:
        logger.warning(f"[comment_scraper] fetch_replies failed: {exc}")
        return []


def fetch_post_with_comments(
    client: FacebookClient,
    feedback_id: str,
    page_name: str,
    post_id: str | None = None,
) -> dict:
    raw_comments: list[dict] = []
    cursor: str | None = None
    post_permalink: str | None = None

    while True:
        j = _fetch_comments_page(client, feedback_id, cursor)
        block = (
            j.get("data", {})
             .get("node", {})
             .get("comment_rendering_instance_for_feed_location", {})
             .get("comments", {})
        )
        edges = block.get("edges", [])
        if not edges:
            break

        if post_permalink is None:
            raw_url = edges[0].get("node", {}).get("feedback", {}).get("url", "")
            if raw_url:
                post_permalink = raw_url.split("?")[0].rstrip("/") + "/"

        for e in edges:
            n  = e["node"]
            fb = n["feedback"]
            author    = n.get("author") or {}
            author_id = author.get("id", "")
            raw_comments.append({
                "author_id":        author_id,
                "author_name":      author.get("name", ""),
                "author_url":       f"https://www.facebook.com/profile.php?id={author_id}" if author_id else "",
                "text":             (n.get("body") or {}).get("text", ""),
                "reaction_count":   fb.get("reactors", {}).get("count_reduced", "0"),
                "_feedback_id":     fb["id"],
                "_expansion_token": (fb.get("expansion_info") or {}).get("expansion_token", ""),
            })

        cursor = block.get("page_info", {}).get("end_cursor")
        if not cursor:
            break

    post_text: str | None = None
    if post_permalink:
        logger.info(f"[comment_scraper] fetching post text from: {post_permalink}")
        text = _fetch_post_text_from_permalink(client, post_permalink)
        post_text = text or None

    comments = []
    for rc in raw_comments:
        replies = _extract_replies(client, rc)
        comments.append({
            "author_id":         rc["author_id"],
            "author_name":       rc["author_name"],
            "author_url":        rc["author_url"],
            "text":              rc["text"],
            "reaction_count":    rc["reaction_count"],
            "parent_comment_id": None,
            "replies":           replies,
        })

    derived_post_id = post_id or feedback_id
    return {
        "post_id":       derived_post_id,
        "source_type":   "simple",
        "page_name":     page_name,
        "text":          post_text,
        "feedback_id":   feedback_id,
        "comment_count": len(comments),
        "permalink":     post_permalink,
        "comments":      comments,
    }
