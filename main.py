# main.py

from __future__ import annotations

import threading
import time
from datetime import datetime, timezone
from typing import Optional

from flask import Flask, jsonify, render_template, request
from flask import request as http_request
from flask_cors import CORS
from flask_socketio import SocketIO, emit

from core.actions import ACTION_REGISTRY, reset_decision_state, resolve_action
from core.analytics import (
    add_telemetry_sample,
    clear_telemetry,
    compute_gradients,
    compute_service_health,
    projected_queue_metrics,
    split_sessions,
)
from core.config import CONFIG, _UPDATABLE_KEYS, reset_config
from core.database import clear_db, fetch_history, init_db, insert_metric
from core.models import (
    calculate_mm1_wait,
    calculate_qos,
    classify_status,
    mm1_curve,
    severity_rank,
)
from core.simulation import _load_csv, get_raw_metrics, reset_simulation, seed_runtime_state
from core.utils import _coerce_config_value, _log, _validate_config_value
from core.store import RomStore

# ---------------------------------------------------------------------------
# App & SocketIO setup
# ---------------------------------------------------------------------------
app = Flask(__name__, template_folder="templates", static_folder="static")
CORS(app)

sio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading",
    ping_timeout=20,
    ping_interval=25,
    max_http_buffer_size=1_000_000,
    logger=True,
    engineio_logger=True,
)

# ---------------------------------------------------------------------------
# ROM store
# ---------------------------------------------------------------------------
rom = RomStore()

# ---------------------------------------------------------------------------
# Delta helper
# ---------------------------------------------------------------------------
_last_delta_state: dict = {
    "action_sig": None,
    "ims_total": None,
    "service_sessions": None,
}
_last_delta_lock = threading.Lock()


def _action_signature(action: Optional[dict]) -> Optional[tuple]:
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

    delta: dict = {
        "ts":      snapshot["ts"],
        "cpu":     snapshot["cpu"],
        "jitter":  snapshot["jitter"],
        "delay":   snapshot["delay"],
        "lambda_": snapshot["lambda_"],
        "qos":     snapshot["qos"],
        "wq":      snapshot["wq"],
        "status":  snapshot["status"],
    }

    with _last_delta_lock:
        if snapshot.get("ims_total") != _last_delta_state["ims_total"]:
            delta["ims_total"] = snapshot["ims_total"]
            _last_delta_state["ims_total"] = snapshot["ims_total"]

        if snapshot.get("service_sessions") != _last_delta_state["service_sessions"]:
            delta["service_sessions"] = snapshot["service_sessions"]
            _last_delta_state["service_sessions"] = snapshot["service_sessions"]

        if action_sig != _last_delta_state["action_sig"]:
            delta["action_center"] = snapshot["action_center"]
            _last_delta_state["action_sig"] = action_sig
            _log(
                "decision",
                f"{snapshot['action_center']['scenario']}  "
                f"status={snapshot['status']}  "
                f"confidence={snapshot['action_center']['confidence_score']:.2f}",
            )

    return delta


# ---------------------------------------------------------------------------
# Snapshot builder
# ---------------------------------------------------------------------------
def _build_snapshot() -> dict:
    raw = get_raw_metrics()
    now_epoch = time.time()
    ims_total = int(raw.get("ims_total") or 0)
    sessions = split_sessions(ims_total)

    add_telemetry_sample({
        "ts_epoch": now_epoch,
        "cpu":     raw["cpu"],
        "jitter":  raw["jitter"],
        "delay":   raw["delay"],
        "lambda_": raw["lambda_"],
    })

    gradients  = compute_gradients(raw, now_epoch)
    shi        = compute_service_health(raw)
    queue_proj = projected_queue_metrics(raw, gradients)
    qos        = calculate_qos(raw["delay"], raw["jitter"], raw["cpu"])
    wq         = calculate_mm1_wait(raw["lambda_"])
    status     = classify_status(qos)
    decision   = resolve_action(raw, sessions, gradients, shi, queue_proj, status, CONFIG)

    if severity_rank(decision["status"]) > severity_rank(status):
        status = decision["status"]

    return {
        "ts":               datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "cpu":              raw["cpu"],
        "jitter":           raw["jitter"],
        "delay":            raw["delay"],
        "lambda_":          raw["lambda_"],
        "ims_total":        ims_total,
        "service_sessions": sessions,
        "qos":              qos,
        "wq":               wq,
        "status":           status,
        "analytics": {
            "gradients":        gradients,
            "shi":              shi,
            "queue_projection": queue_proj,
        },
        "action_center": {**decision, "status": status},
    }


# ---------------------------------------------------------------------------
# Simulation loop
# ---------------------------------------------------------------------------
_sim_thread: Optional[threading.Thread] = None
_sim_stop = threading.Event()


def _simulation_loop() -> None:
    while not _sim_stop.is_set():
        try:
            snapshot = _build_snapshot()
            insert_metric(snapshot)
            rom.set(snapshot)
        except Exception as exc:
            _log("sim_loop", f"error: {exc}")
        _sim_stop.wait(CONFIG["PUSH_INTERVAL"])


def _start_simulation() -> None:
    global _sim_thread
    _sim_stop.clear()
    if _sim_thread is None or not _sim_thread.is_alive():
        _sim_thread = threading.Thread(
            target=_simulation_loop, name="sim_loop", daemon=True
        )
        _sim_thread.start()

# ---------------------------------------------------------------------------
# Socket
# ---------------------------------------------------------------------------
connected_clients: set[str] = set()

@sio.on("connect")
def on_connect():
    sid = request.sid
    connected_clients.add(sid)
    _log("connect", f"sid={sid} total={len(connected_clients)}")


@sio.on("disconnect")
def on_disconnect():
    sid = request.sid
    connected_clients.discard(sid)
    _log("disconnect", f"sid={sid} remaining={len(connected_clients)}")


@sio.on("request_update")
def on_request_update():
    snapshot = rom.get()
    if snapshot is None:
        return {"status": "pending"}
    return _build_delta(snapshot)


# ---------------------------------------------------------------------------
# REST endpoints
# ---------------------------------------------------------------------------
@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/snapshot")
def api_snapshot():
    snapshot = rom.get() or _build_snapshot()
    insert_metric(snapshot)
    return jsonify(snapshot)


@app.get("/api/history")
def api_history():
    limit = http_request.args.get("limit", 60, type=int)
    return jsonify(fetch_history(limit))


@app.get("/api/mm1_curve")
def api_mm1_curve():
    mu = http_request.args.get("mu", CONFIG["MU"], type=float)
    return jsonify({"mu": mu, "curve": mm1_curve(mu)})


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    if http_request.method == "POST":
        data: dict = http_request.get_json(silent=True) or {}
        applied, rejected = {}, {}
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
        return jsonify({
            "ok":                not rejected,
            "applied":           applied,
            "rejected":          rejected,
            "ignored":           ignored,
            "available_actions": sorted(ACTION_REGISTRY),
            "config":            dict(CONFIG),
        })
    return jsonify({"config": dict(CONFIG), "available_actions": sorted(ACTION_REGISTRY)})


@app.post("/api/reset")
def api_reset():
    reset_simulation()
    clear_telemetry()
    reset_decision_state()
    clear_db()
    init_db()
    seed_runtime_state()
    reset_config()
    rom.clear()
    return jsonify({"status": "reset", "message": "Simulation restarted, storage cleared."})


@app.get("/api/summary")
def api_summary():
    snapshot = rom.get()
    if snapshot is None:
        return ("", 204)
    action = snapshot.get("action_center") or {}
    return jsonify({
        "ts":                  snapshot["ts"],
        "status":              snapshot["status"],
        "qos":                 snapshot["qos"],
        "scenario":            action.get("scenario"),
        "diagnosis":           action.get("diagnosis") or action.get("analysis"),
        "rationale":           action.get("rationale") or action.get("decision"),
        "proposed_patch":      action.get("proposed_patch") or action.get("optimization"),
        "optimization_result": action.get("optimization_result"),
        "confidence_score":    action.get("confidence_score"),
    })


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------
def _initialise() -> None:
    init_db()
    _load_csv()
    seed_runtime_state()
    _start_simulation()


_initialise()

if __name__ == "__main__":
    sio.run(app, host="0.0.0.0", port=5000, debug=False, log_output=True, allow_unsafe_werkzeug=True)