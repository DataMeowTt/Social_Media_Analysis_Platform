import os

import httpx

SUPERSET_URL = os.getenv("SUPERSET_URL", "http://localhost:8088")
_USER = os.getenv("SUPERSET_ADMIN_USER", "admin")
_PASS = os.getenv("SUPERSET_ADMIN_PASS", "admin")


def _parse_dashboards(env_key: str) -> list[dict]:
    raw = os.getenv(env_key, "")
    if not raw:
        return []
    result = []
    for item in raw.split(","):
        parts = item.strip().split(":", 1)
        result.append({"id": parts[0].strip(), "label": parts[1].strip() if len(parts) > 1 else "Dashboard"})
    return result


PLATFORM_DASHBOARDS = {
    "twitter": _parse_dashboards("SUPERSET_DASHBOARDS_TWITTER") or _parse_dashboards("SUPERSET_DASHBOARDS"),
    "youtube": _parse_dashboards("SUPERSET_DASHBOARDS_YOUTUBE"),
}


async def _access_token() -> str:
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPERSET_URL}/api/v1/security/login",
            json={"username": _USER, "password": _PASS, "provider": "db", "refresh": True},
        )
        r.raise_for_status()
        return r.json()["access_token"]


async def get_guest_token(dashboard_id: str) -> str:
    token = await _access_token()
    async with httpx.AsyncClient() as client:
        r = await client.post(
            f"{SUPERSET_URL}/api/v1/security/guest_token/",
            headers={"Authorization": f"Bearer {token}"},
            json={
                "user": {"username": "guest_user", "first_name": "Guest", "last_name": "User"},
                "resources": [{"type": "dashboard", "id": dashboard_id}],
                "rls": [],
            },
        )
        if not r.is_success:
            raise ValueError(f"Superset guest_token error {r.status_code}: {r.text}")
        return r.json()["token"]
