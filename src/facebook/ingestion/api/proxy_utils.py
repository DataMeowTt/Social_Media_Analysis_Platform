from __future__ import annotations

import os
import random

import requests
from dotenv import load_dotenv

load_dotenv()

_PORT_MIN = int(os.getenv("PROXY_PORT_MIN", "10000"))
_PORT_MAX = int(os.getenv("PROXY_PORT_MAX", "10000"))


def _replace_port(proxy_url: str, port: int) -> str:
    head, sep, tail = proxy_url.rpartition(":")
    if not sep or not tail.isdigit():
        return proxy_url
    return f"{head}:{port}"


def _build(proxy_url: str) -> dict:
    return {"http": proxy_url, "https": proxy_url}


def rotate_static_proxy() -> dict | None:
    base = os.getenv("STATIC_PROXY", "").strip() or os.getenv("PROXY", "").strip()
    if not base:
        return None
    port = random.randint(_PORT_MIN, _PORT_MAX)
    return _build(_replace_port(base, port))


def select_proxy(has_cookies: bool) -> dict | None:
    if has_cookies:
        url = os.getenv("STATIC_PROXY", "").strip() or os.getenv("PROXY", "").strip()
    else:
        url = os.getenv("ROTATING_PROXY", "").strip() or os.getenv("PROXY", "").strip()
    return _build(url) if url else None


def is_proxy_infra_error(exc=None, status_code: int | None = None) -> bool:
    if status_code == 407:
        return True
    if exc is not None:
        if isinstance(exc, (requests.exceptions.ProxyError, requests.exceptions.ConnectionError)):
            return True
        msg = str(exc).lower()
        if any(k in msg for k in ("proxy", "407", "tunnel", "connection refused", "eof occurred")):
            return True
    return False


def is_ip_blocked(status_code: int | None = None, response_text: str | None = None) -> bool:
    if status_code in (403, 429, 503):
        return True
    if response_text:
        txt = response_text[:500].lower()
        if any(k in txt for k in ("checkpoint", "login_required", "you must log in", "blocked")):
            return True
    return False
