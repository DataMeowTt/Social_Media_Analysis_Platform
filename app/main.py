import asyncio
import os
import uuid
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env", override=True)

from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel

import airflow_client as ac
import superset_client as sc
import s3_insights
import fb_insights

app = FastAPI(title="Social Media Pipeline Dashboard")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

_USERNAME = os.getenv("USERNAME", "admin")
_PASSWORD = os.getenv("PASSWORD", "admin")
_valid_tokens: set[str] = set()

security = HTTPBearer()


def require_auth(credentials: HTTPAuthorizationCredentials = Depends(security)):
    if credentials.credentials not in _valid_tokens:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


class LoginRequest(BaseModel):
    username: str
    password: str


@app.post("/api/auth/login")
async def login(body: LoginRequest):
    if body.username != _USERNAME or body.password != _PASSWORD:
        raise HTTPException(status_code=401, detail="Invalid credentials")
    token = str(uuid.uuid4())
    _valid_tokens.add(token)
    return {"token": token}


@app.post("/api/auth/logout")
async def logout(credentials: HTTPAuthorizationCredentials = Depends(security)):
    _valid_tokens.discard(credentials.credentials)
    return {"ok": True}


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


@app.get("/api/status", dependencies=[Depends(require_auth)])
async def api_status():
    results = await asyncio.gather(*[_fetch_status(dag_id) for dag_id in ac.DAGS])
    return dict(zip(ac.DAGS.keys(), results))


@app.get("/api/runs", dependencies=[Depends(require_auth)])
async def api_runs():
    results = await asyncio.gather(*[_fetch_runs(dag_id) for dag_id in ac.DAGS])
    return dict(zip(ac.DAGS.keys(), results))


@app.get("/api/superset/config", dependencies=[Depends(require_auth)])
async def superset_config():
    return {"platforms": sc.PLATFORM_DASHBOARDS, "superset_url": sc.SUPERSET_URL}


@app.get("/api/superset/guest-token", dependencies=[Depends(require_auth)])
async def superset_guest_token(dashboard_id: str):
    token = await sc.get_guest_token(dashboard_id)
    return {"token": token}


@app.get("/api/youtube/thread-insights", dependencies=[Depends(require_auth)])
async def youtube_thread_insights():
    return await asyncio.to_thread(s3_insights.read_thread_insights)


@app.get("/api/facebook/controversial-posts", dependencies=[Depends(require_auth)])
async def fb_controversial_posts():
    return await asyncio.to_thread(fb_insights.controversial_posts)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
