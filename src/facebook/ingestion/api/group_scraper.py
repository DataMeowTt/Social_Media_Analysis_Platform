from __future__ import annotations

import json
import time

from src.facebook.ingestion.api.client import FacebookClient
from src.facebook.ingestion.api.comment_scraper import fetch_post_with_comments
from src.utils.logger import get_logger

logger = get_logger(__name__)

_DOC_GROUP_FEED = "25716860671307636"


def _parse_blocks(text: str) -> list[dict]:
    text = text.replace("for (;;);", "").strip()
    blocks, i, n = [], 0, len(text)
    while True:
        idx = text.find('"data"', i)
        if idx == -1:
            break
        brace = text.find("{", idx)
        if brace == -1:
            break
        depth = 0
        for j in range(brace, n):
            if text[j] == "{":
                depth += 1
            elif text[j] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        blocks.append(json.loads(text[brace : j + 1]))
                    except Exception:
                        pass
                    i = j + 1
                    break
        else:
            break
    return blocks



def _extract_group_name(node: dict) -> str | None:
    try:
        to_obj = (
            node.get("comet_sections", {})
            .get("context_layout", {})
            .get("story", {})
            .get("comet_sections", {})
            .get("title", {})
            .get("story", {})
            .get("to", {})
        )
        if to_obj.get("__typename") == "Group":
            return to_obj.get("name")
    except Exception:
        pass
    return None


def _extract_comment_count(node: dict) -> int:
    paths = [
        lambda n: n.get("feedback", {})
                   .get("comment_rendering_instance", {})
                   .get("comments", {})
                   .get("total_count"),
        lambda n: (
            n.get("comet_sections", {})
             .get("feedback", {})
             .get("story", {})
             .get("story_ufi_container", {})
             .get("story", {})
             .get("feedback_context", {})
             .get("feedback_target_with_context", {})
             .get("comment_rendering_instance", {})
             .get("comments", {})
             .get("total_count")
        ),
    ]
    for fn in paths:
        try:
            val = fn(node)
            if val is not None:
                return val
        except Exception:
            pass
    return 0


def _is_reel(node: dict) -> bool:
    for attachment in node.get("attachments", []):
        media = attachment.get("media", {})
        if media.get("__typename") == "Video":
            return True
        styles_media = attachment.get("styles", {}).get("attachment", {}).get("media", {})
        if styles_media.get("__typename") == "Video":
            return True
    return False


def _extract_post(node: dict, group_name: str | None) -> dict | None:
    if not node or node.get("__typename") != "Story":
        return None
    if _is_reel(node):
        return None

    post_id = node.get("post_id")
    if not post_id:
        return None

    message = (
        node.get("comet_sections", {})
        .get("content", {})
        .get("story", {})
        .get("message", {})
        .get("text", "")
    ) or ""

    feedback    = node.get("feedback", {})
    feedback_id = feedback.get("id")

    return {
        "post_id":       post_id,
        "feedback_id":   feedback_id,
        "message":       message,
        "comment_count": _extract_comment_count(node),
        "group_name":    group_name or _extract_group_name(node),
        "permalink":     node.get("permalink_url", ""),
    }



def fetch_group_posts(
    client: FacebookClient,
    group_id: str,
    limit: int = 50,
    min_comments: int = 0,
) -> list[dict]:
    posts: list[dict] = []
    cursor: str | None = None
    group_name: str | None = None

    while len(posts) < limit:
        variables = {
            "count":                  3,
            "cursor":                 cursor,
            "feedLocation":           "GROUP",
            "feedType":               "DISCUSSION",
            "feedbackSource":         0,
            "filterTopicId":          None,
            "focusCommentID":         None,
            "privacySelectorRenderLocation": "COMET_STREAM",
            "renderLocation":         "group",
            "scale":                  2,
            "stream_initial_count":   1,
            "useDefaultActor":        False,
            "id":                     group_id,
        }
        r = client.post_graphql(_DOC_GROUP_FEED, variables)
        blocks = _parse_blocks(r.text)
        if not blocks:
            logger.warning("[group_scraper] empty response — stopping")
            break

        next_cursor: str | None = None
        page_posts_found = 0

        for item in blocks:
            if not isinstance(item, dict):
                continue

            node = item.get("node", {})
            story_nodes = []

            if node.get("__typename") == "Story":
                story_nodes.append(node)
            elif node.get("__typename") == "Group":
                for edge in node.get("group_feed", {}).get("edges", []):
                    sn = edge.get("node", {})
                    if sn.get("__typename") == "Story":
                        story_nodes.append(sn)

            for sn in story_nodes:
                comment_count = _extract_comment_count(sn)
                if min_comments > 0 and comment_count < min_comments:
                    continue
                if not group_name:
                    group_name = _extract_group_name(sn)
                post = _extract_post(sn, group_name)
                if post:
                    posts.append(post)
                    page_posts_found += 1
                    logger.info(f"[group_scraper] found post {post['post_id']} ({comment_count} comments)")
                if len(posts) >= limit:
                    break

            if "page_info" in item:
                pi = item["page_info"]
                if pi.get("has_next_page"):
                    next_cursor = pi.get("end_cursor")

        logger.info(f"[group_scraper] page: {page_posts_found} posts, total: {len(posts)}/{limit}")

        if not next_cursor or len(posts) >= limit:
            break
        cursor = next_cursor
        time.sleep(2)

    return posts[:limit]


def fetch_group_posts_with_comments(
    client: FacebookClient,
    group_id: str,
    limit: int = 50,
    min_comments: int = 0,
) -> list[dict]:
    posts_meta = fetch_group_posts(client, group_id, limit, min_comments)
    results = []
    for meta in posts_meta:
        if not meta.get("feedback_id"):
            logger.warning(f"[group_scraper] post {meta['post_id']} has no feedback_id — skipping comments")
            post_data = {
                "post_id":       meta["post_id"],
                "source_type":   "group",
                "page_name":     meta.get("group_name") or "",
                "text":          meta.get("message") or "",
                "feedback_id":   None,
                "comment_count": meta.get("comment_count", 0),
                "permalink":     meta.get("permalink") or "",
                "comments":      [],
            }
        else:
            post_data = fetch_post_with_comments(
                client=client,
                feedback_id=meta["feedback_id"],
                page_name=meta.get("group_name") or "",
                post_id=meta["post_id"],
            )
            post_data["source_type"] = "group"
            post_data["text"]        = meta.get("message") or ""
            post_data["permalink"]   = meta.get("permalink") or ""

        results.append(post_data)
        time.sleep(1)

    return results
