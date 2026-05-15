import asyncio
import os
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import httpx
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

import airflow_client as ac
import superset_client as sc

app = FastAPI(title="Social Media Pipeline Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)

_TEMPLATES_DIR = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(_TEMPLATES_DIR))


@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


async def _fetch_status(dag_id: str) -> dict:
    try:
        run = await ac.get_latest_run(dag_id)
        tasks = await ac.get_task_instances(dag_id, run["run_id"]) if run else []
        return {"run": run, "tasks": tasks}
    except Exception as exc:
        return {"run": None, "tasks": [], "error": str(exc)}


async def _fetch_runs(dag_id: str) -> list:
    try:
        return await ac.get_runs(dag_id, limit=10)
    except Exception:
        return []


@app.get("/api/status")
async def api_status():
    results = await asyncio.gather(*[_fetch_status(dag_id) for dag_id in ac.DAGS])
    return dict(zip(ac.DAGS.keys(), results))


@app.get("/api/runs")
async def api_runs():
    results = await asyncio.gather(*[_fetch_runs(dag_id) for dag_id in ac.DAGS])
    return dict(zip(ac.DAGS.keys(), results))


@app.get("/api/superset/config")
async def superset_config():
    return {"dashboards": sc.DASHBOARDS, "superset_url": sc.SUPERSET_URL}


@app.get("/api/superset/guest-token")
async def superset_guest_token(dashboard_id: str):
    token = await sc.get_guest_token(dashboard_id)
    return {"token": token}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
