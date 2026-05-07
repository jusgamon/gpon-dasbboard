# main.py
import asyncio
import time
import os
from datetime import datetime, timezone
from typing import Optional

import uvicorn
import socketio
from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from core.config import CONFIG, _UPDATABLE_KEYS, reset_config
from core.database import init_db, insert_metric, fetch_history, clear_db
from core.simulation import (
    seed_runtime_state,
    reset_simulation,
    get_raw_metrics,
    _load_csv,
)
from core.analytics import (
    add_telemetry_sample,
    clear_telemetry,
    compute_gradients,
    split_sessions,
    compute_service_health,
    projected_queue_metrics
)
from core.actions import (
    resolve_action,
    reset_decision_state,
    ACTION_REGISTRY,
)
from core.models import (
    calculate_qos,
    calculate_mm1_wait,
    classify_status,
    severity_rank,
    mm1_curve,
)
from core.utils import _log, _coerce_config_value, _validate_config_value

# ---------------------------------------------------------------------------
# FastAPI & SocketIO setup
# ---------------------------------------------------------------------------
sio = socketio.AsyncServer(async_mode="asgi", cors_allowed_origins="*", logger=True, transports=["websocket"])
app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
application = socketio.ASGIApp(sio, app)

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
templates = Jinja2Templates(directory="templates")

# ---------------------------------------------------------------------------
# Application state
# ---------------------------------------------------------------------------
_push_task: Optional[asyncio.Task] = None
_push_task_lock = asyncio.Lock()

_last_emit: dict = {"action_sig": None, "ims_total": None, "service_sessions": None}

# ---------------------------------------------------------------------------
# Snapshot assembly (simulation + logic)
# ---------------------------------------------------------------------------
def _action_signature(action: dict) -> tuple:
    if not action:
        return None
    return (
        action.get("scenario"),
        action.get("status"),
        round(float(action.get("confidence_score", 0.0)), 2),
        action.get("optimization_result"),
    )

def _build_delta(snapshot: dict) -> dict:
    action_sig = _action_signature(snapshot.get("action_center"))
    delta = {
        "ts": snapshot["ts"],
        "cpu": snapshot["cpu"],
        "jitter": snapshot["jitter"],
        "delay": snapshot["delay"],
        "lambda_": snapshot["lambda_"],
        "qos": snapshot["qos"],
        "wq": snapshot["wq"],
        "status": snapshot["status"],
    }
    if snapshot.get("ims_total") != _last_emit["ims_total"]:
        delta["ims_total"] = snapshot["ims_total"]
    if snapshot.get("service_sessions") != _last_emit["service_sessions"]:
        delta["service_sessions"] = snapshot["service_sessions"]
    if action_sig != _last_emit["action_sig"]:
        delta["action_center"] = snapshot["action_center"]
        _log("decision",
             f"{snapshot['action_center']['scenario']}  "
             f"status={snapshot['status']}  "
             f"confidence={snapshot['action_center']['confidence_score']:.2f}")
    _last_emit["action_sig"] = action_sig
    _last_emit["ims_total"] = snapshot.get("ims_total")
    _last_emit["service_sessions"] = snapshot.get("service_sessions")
    return delta

def build_snapshot() -> dict:
    raw = get_raw_metrics()
    now_epoch = time.time()
    ims_total = int(raw.get("ims_total") or 0)
    sessions = split_sessions(ims_total)
    add_telemetry_sample({
        "ts_epoch": now_epoch,
        "cpu": raw["cpu"],
        "jitter": raw["jitter"],
        "delay": raw["delay"],
        "lambda_": raw["lambda_"],
    })
    gradients = compute_gradients(raw, now_epoch)
    shi = compute_service_health(raw)
    queue_proj = projected_queue_metrics(raw, gradients)
    qos = calculate_qos(raw["delay"], raw["jitter"], raw["cpu"])
    wq = calculate_mm1_wait(raw["lambda_"])
    status = classify_status(qos)
    decision = resolve_action(raw, sessions, gradients, shi, queue_proj, status, CONFIG)
    if severity_rank(decision["status"]) > severity_rank(status):
        status = decision["status"]
    return {
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cpu": raw["cpu"],
        "jitter": raw["jitter"],
        "delay": raw["delay"],
        "lambda_": raw["lambda_"],
        "ims_total": ims_total,
        "service_sessions": sessions,
        "qos": qos,
        "wq": wq,
        "status": status,
        "analytics": {
            "gradients": gradients,
            "shi": shi,
            "queue_projection": queue_proj,
        },
        "action_center": {**decision, "status": status},
    }

# ---------------------------------------------------------------------------
# Background push loop
# ---------------------------------------------------------------------------
async def push_loop():
    while True:
        try:
            snapshot = await asyncio.to_thread(build_snapshot)
            await asyncio.to_thread(insert_metric, snapshot)
            delta = await asyncio.to_thread(_build_delta, snapshot)
            await sio.emit("metric_update", delta)
        except Exception as exc:
            _log("push_loop", f"error: {exc}")
        await asyncio.sleep(CONFIG["PUSH_INTERVAL"])

# ---------------------------------------------------------------------------
# SocketIO events
# ---------------------------------------------------------------------------
connected_clients = set()

@sio.event
async def connect(sid, environ):
    connected_clients.add(sid)

    global _push_task

    async with _push_task_lock:
        if _push_task is None or _push_task.done():
            _push_task = asyncio.create_task(push_loop())

    snapshot = await asyncio.to_thread(build_snapshot)
    history = await asyncio.to_thread(fetch_history, 30)
    await sio.emit("bootstrap_data", {
        "snapshot": snapshot,
        "history": history,
    }, to=sid)

@sio.event
async def disconnect(sid):
    connected_clients.discard(sid)

    global _push_task

    if not connected_clients and _push_task:
        _push_task.cancel()
        _push_task = None

# ---------------------------------------------------------------------------
# REST API endpoints
# ---------------------------------------------------------------------------
@app.get("/", response_class=HTMLResponse)
async def index(request: Request):
    return templates.TemplateResponse(request, "index.html")

@app.get("/api/snapshot")
async def api_snapshot():
    snap = await asyncio.to_thread(build_snapshot)
    await asyncio.to_thread(insert_metric, snap)
    return snap

@app.get("/api/history")
async def api_history(limit: int = 60):
    return await asyncio.to_thread(fetch_history, limit)

@app.get("/api/mm1_curve")
async def api_mm1_curve(mu: float = None):
    mu = mu or CONFIG["MU"]
    return {"mu": mu, "curve": await asyncio.to_thread(mm1_curve, mu)}

@app.api_route("/api/config", methods=["GET", "POST"])
async def api_config(request: Request):
    if request.method == "POST":
        data = await request.json() if request.headers.get("content-type") == "application/json" else {}
        applied = {}
        rejected = {}
        ignored = [k for k in data if k not in _UPDATABLE_KEYS]
        for key, value in data.items():
            if key not in _UPDATABLE_KEYS:
                continue
            ok, err = _validate_config_value(key, value)
            if not ok:
                rejected[key] = err
            else:
                CONFIG[key] = _coerce_config_value(key, value)
                applied[key] = CONFIG[key]
        _log("config", f"applied={sorted(applied)} rejected={sorted(rejected)}")
        return {
            "ok": not rejected,
            "applied": applied,
            "rejected": rejected,
            "ignored": ignored,
            "available_actions": sorted(ACTION_REGISTRY),
            "config": dict(CONFIG),
        }
    # GET
    return {"config": dict(CONFIG), "available_actions": sorted(ACTION_REGISTRY)}

@app.post("/api/reset")
async def api_reset():
    """Reset simulation and clear all stored data / memory."""
    await asyncio.to_thread(reset_simulation)
    await asyncio.to_thread(clear_telemetry)
    await asyncio.to_thread(reset_decision_state)
    await asyncio.to_thread(clear_db)
    await asyncio.to_thread(init_db)
    await asyncio.to_thread(seed_runtime_state)
    await asyncio.to_thread(reset_config)
    return {"status": "reset", "message": "Simulation restarted, storage cleared."}

app.mount("/static", StaticFiles(directory="static"), name="static")
# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------
@app.on_event("startup")
async def startup_event():
    init_db()
    _load_csv()
    seed_runtime_state()
    _log("startup", f"GPON/IMS Monitor  →  http://0.0.0.0:5000  (push_interval={CONFIG['PUSH_INTERVAL']}s)")

if __name__ == "__main__":
    uvicorn.run(application, host="0.0.0.0", port=5000, log_level="info")