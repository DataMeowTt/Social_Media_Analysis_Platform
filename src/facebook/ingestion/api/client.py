from __future__ import annotations

import json
import time

import requests

from src.facebook.ingestion.api.proxy_utils import is_ip_blocked, is_proxy_infra_error, rotate_static_proxy
from src.utils.logger import get_logger

logger = get_logger(__name__)

GRAPHQL_URL = "https://www.facebook.com/api/graphql/"
_MAX_RETRIES = 5
_HEADERS = {
    "user-agent": "Mozilla/5.0",
    "content-type": "application/x-www-form-urlencoded",
    "origin": "https://www.facebook.com",
}


class FacebookClient:
    def __init__(
        self,
        proxies: dict | None = None,
        cookies: dict | None = None,
        fb_dtsg: str = "",
    ) -> None:
        self.proxies = proxies or {}
        self.cookies = cookies or {}
        self.fb_dtsg = fb_dtsg

    def post_graphql(self, doc_id: str, variables: dict, friendly_name: str = "") -> requests.Response:
        headers = {**_HEADERS}
        if friendly_name:
            headers["x-fb-friendly-name"] = friendly_name

        payload = {
            "av":       self.cookies.get("c_user", "0"),
            "__user":   self.cookies.get("c_user", "0"),
            "__a":      "1",
            "fb_dtsg":  self.fb_dtsg,
            "doc_id":   doc_id,
            "variables": json.dumps(variables),
        }
        return self._retry(headers, payload)

    def _retry(self, headers: dict, payload: dict) -> requests.Response:
        proxies = self.proxies.copy()
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                r = requests.post(
                    GRAPHQL_URL,
                    headers=headers,
                    data=payload,
                    proxies=proxies or None,
                    cookies=self.cookies,
                    timeout=30,
                )
                if r.status_code == 200:
                    return r
                if is_proxy_infra_error(status_code=r.status_code):
                    proxies = self._rotate(proxies)
                elif is_ip_blocked(status_code=r.status_code, response_text=r.text):
                    proxies = self._rotate(proxies)
                else:
                    logger.warning(f"[FacebookClient] attempt {attempt}: HTTP {r.status_code}")
            except requests.exceptions.ProxyError:
                proxies = self._rotate(proxies)
            except Exception as exc:
                if is_proxy_infra_error(exc=exc):
                    proxies = self._rotate(proxies)
                else:
                    logger.warning(f"[FacebookClient] attempt {attempt}: {exc}")

            if attempt < _MAX_RETRIES:
                time.sleep(attempt * 2)

        raise RuntimeError(f"Facebook GraphQL failed after {_MAX_RETRIES} attempts")

    def _rotate(self, current: dict) -> dict:
        new = rotate_static_proxy()
        if new:
            self.proxies = new
            return new
        return current
