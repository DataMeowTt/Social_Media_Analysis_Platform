import os
from typing import Optional
from datetime import datetime, timezone

import httpx

AIRFLOW_URL = os.getenv("AIRFLOW_URL", "http://localhost:8080")
AIRFLOW_USER = os.getenv("AIRFLOW_USER", "admin")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "admin")

_BASE = f"{AIRFLOW_URL}/api/v2"

DAGS = {
    "social_media_pipeline": [
        "ingestion_tweets",
        "processing_silver",
        "analytics_gold",
    ],
    "social_media_pipeline_test": [
        "ingestion_tweets",
        "delete_raw_duplicates",
        "processing_silver",
        "delete_processed_duplicates",
        "analytics_gold",
    ],
}

_token: Optional[str] = None


async def _get_token() -> str:
    global _token
    if _token:
        return _token
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.post(
            f"{AIRFLOW_URL}/auth/token",
            json={"username": AIRFLOW_USER, "password": AIRFLOW_PASSWORD},
        )
        r.raise_for_status()
        _token = r.json()["access_token"]
    return _token


async def _get(path: str, params: dict = None) -> dict:
    global _token
    token = await _get_token()
    async with httpx.AsyncClient(timeout=10) as client:
        r = await client.get(f"{_BASE}{path}", headers={"Authorization": f"Bearer {token}"}, params=params)
        if r.status_code == 401:
            _token = None
            token = await _get_token()
            r = await client.get(f"{_BASE}{path}", headers={"Authorization": f"Bearer {token}"}, params=params)
        r.raise_for_status()
    return r.json()


def _duration_seconds(start: Optional[str], end: Optional[str]) -> Optional[float]:
    if not start:
        return None

    def parse(s):
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            return datetime.strptime(s, "%Y-%m-%dT%H:%M:%S%z")

    try:
        t0 = parse(start)
        t1 = parse(end) if end else datetime.now(timezone.utc)
        return round((t1 - t0).total_seconds(), 1)
    except Exception:
        return None


def _clean_run(run: dict) -> dict:
    start = run.get("start_date") or run.get("logical_date")
    end = run.get("end_date")
    return {
        "run_id": run.get("dag_run_id"),
        "state": run.get("state"),
        "start_date": start,
        "end_date": end,
        "duration_seconds": _duration_seconds(start, end),
        "triggered_by": run.get("run_type", "unknown"),
    }


def _clean_task(ti: dict) -> dict:
    start = ti.get("start_date")
    end = ti.get("end_date")
    return {
        "task_id": ti.get("task_id"),
        "state": ti.get("state") or "none",
        "start_date": start,
        "end_date": end,
        "duration_seconds": _duration_seconds(start, end),
        "try_number": ti.get("try_number", 0),
    }


async def get_latest_run(dag_id: str) -> Optional[dict]:
    data = await _get(f"/dags/{dag_id}/dagRuns", {"limit": 1, "order_by": "-start_date"})
    runs = data.get("dag_runs", [])
    return _clean_run(runs[0]) if runs else None


async def get_task_instances(dag_id: str, run_id: str) -> list[dict]:
    data = await _get(f"/dags/{dag_id}/dagRuns/{run_id}/taskInstances")
    tis = data.get("task_instances", [])
    by_id = {ti["task_id"]: _clean_task(ti) for ti in tis}
    return [
        by_id.get(tid, {"task_id": tid, "state": "none", "start_date": None,
                        "end_date": None, "duration_seconds": None, "try_number": 0})
        for tid in DAGS[dag_id]
    ]


async def get_runs(dag_id: str, limit: int = 10) -> list[dict]:
    data = await _get(f"/dags/{dag_id}/dagRuns", {"limit": limit, "order_by": "-start_date"})
    return [_clean_run(r) for r in data.get("dag_runs", [])]
