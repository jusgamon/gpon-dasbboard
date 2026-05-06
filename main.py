"""
GPON/IMS Network Monitoring Dashboard backend.
"""

import csv
import os
import random
import sqlite3
from datetime import datetime, timezone
from threading import Lock

from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config["SECRET_KEY"] = "gpon-ims-poc-secret"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

CONFIG = {
    "MU": 10.0,
    "PUSH_INTERVAL": 1,
    "DATA_SOURCE": "simulate",
    "CSV_PATH": "data.csv",
    "CPU_RANGE": (5, 95),
    "JITTER_RANGE": (1, 30),
    "DELAY_RANGE": (2, 50),
    "LAMBDA_RANGE": (0.5, 9.5),
    "IMS_SESSION_RANGE": (18, 72),
    "MAX_HISTORY": 200,
    "W_DELAY": 0.4,
    "W_JITTER": 0.3,
    "W_CPU": 0.3,
    "SERVICE_WEIGHTS": {"voip": 0.4, "video": 0.35, "web": 0.25},
    "VOIP_JITTER_THRESHOLD": 20,
    "VIDEO_DELAY_THRESHOLD": 28,
    "CPU_WARNING_THRESHOLD": 85,
    "LAMBDA_WARNING_RATIO": 0.82,
    "ACTION_MODE": "auto",
    "FORCED_ACTION": "observe",
    "FORCED_STATUS": "Normal",
}

ACTION_TEMPLATES = {
    "observe": {
        "service": "All Services",
        "priority": "LOW",
        "analysis": "Current telemetry stays inside the expected service envelope.",
        "decision": "Keep baseline scheduling and continue observation.",
        "optimization": "No active optimization is applied.",
    },
    "voip_priority": {
        "service": "VoIP",
        "priority": "HIGH",
        "analysis": "Voice sessions are exposed to elevated jitter during active IMS load.",
        "decision": "Raise VoIP priority ahead of video and best-effort traffic.",
        "optimization": "Apply QoS preference and reserve queue headroom for RTP flows.",
    },
    "video_reserve": {
        "service": "Video",
        "priority": "MEDIUM",
        "analysis": "Video demand is at risk from rising delay and queue pressure.",
        "decision": "Protect video sessions with extra reserved throughput.",
        "optimization": "Increase bandwidth reservation and smooth burst handling.",
    },
    "load_balance": {
        "service": "Transport",
        "priority": "HIGH",
        "analysis": "Core load indicators show growing CPU or queue saturation pressure.",
        "decision": "Distribute load before service quality degrades further.",
        "optimization": "Shift traffic away from the hottest path and relax best-effort share.",
    },
}

DB_PATH = "network_metrics.db"

_csv_reader_state = {"rows": [], "index": 0}
_push_thread = None
_push_lock = Lock()


def init_db():
    """Create the metrics table if needed."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS metrics (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                ts        TEXT    NOT NULL,
                cpu       REAL    NOT NULL,
                jitter    REAL    NOT NULL,
                delay     REAL    NOT NULL,
                lambda_   REAL    NOT NULL,
                qos       REAL    NOT NULL,
                wq        REAL    NOT NULL,
                status    TEXT    NOT NULL
            )
            """
        )
        conn.commit()


def insert_metric(snapshot: dict):
    """Persist the history fields needed by the charts."""
    row = {
        "ts": snapshot["ts"],
        "cpu": snapshot["cpu"],
        "jitter": snapshot["jitter"],
        "delay": snapshot["delay"],
        "lambda_": snapshot["lambda_"],
        "qos": snapshot["qos"],
        "wq": snapshot["wq"],
        "status": snapshot["status"],
    }
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute(
            """
            INSERT INTO metrics (ts, cpu, jitter, delay, lambda_, qos, wq, status)
            VALUES (:ts, :cpu, :jitter, :delay, :lambda_, :qos, :wq, :status)
            """,
            row,
        )
        conn.execute(
            f"""
            DELETE FROM metrics WHERE id NOT IN (
                SELECT id FROM metrics ORDER BY id DESC LIMIT {CONFIG["MAX_HISTORY"]}
            )
            """
        )
        conn.commit()


def fetch_history(limit=60):
    """Return latest history rows ordered oldest-first."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM metrics ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
    return [dict(row) for row in reversed(rows)]


def calculate_qos(delay: float, jitter: float, cpu: float) -> float:
    score = 100.0 - (
        CONFIG["W_DELAY"] * delay
        + CONFIG["W_JITTER"] * jitter
        + CONFIG["W_CPU"] * cpu
    )
    return round(max(0.0, min(100.0, score)), 2)


def calculate_mm1_wait(lambda_: float, mu: float | None = None) -> float:
    mu = CONFIG["MU"] if mu is None else mu
    if lambda_ >= mu:
        return float("inf")
    return round(1.0 / (mu - lambda_), 4)


def classify_status(qos: float) -> str:
    if qos > 80:
        return "Normal"
    if qos > 50:
        return "Warning"
    return "Critical"


def mm1_curve(mu: float | None = None, steps: int = 50):
    mu = CONFIG["MU"] if mu is None else mu
    points = []
    for idx in range(1, steps + 1):
        lam = round(mu * (idx / (steps + 1)), 4)
        wait = calculate_mm1_wait(lam, mu)
        points.append({"lambda": lam, "W": wait if wait != float("inf") else None})
    return points


def _load_csv():
    path = CONFIG["CSV_PATH"]
    if not os.path.exists(path):
        return False
    with open(path, newline="") as csv_file:
        reader = csv.DictReader(csv_file)
        _csv_reader_state["rows"] = list(reader)
    _csv_reader_state["index"] = 0
    return bool(_csv_reader_state["rows"])


def _next_csv_row():
    rows = _csv_reader_state["rows"]
    if not rows:
        return None
    idx = _csv_reader_state["index"] % len(rows)
    _csv_reader_state["index"] += 1
    row = rows[idx]
    return {
        "cpu": float(row["cpu"]),
        "jitter": float(row["jitter"]),
        "delay": float(row["delay"]),
        "lambda_": float(row["lambda"]),
        "ims_total": int(float(row.get("ims_total", 0) or 0)),
    }


def get_raw_metrics() -> dict:
    """Return a raw metric sample from the configured data source."""
    if CONFIG["DATA_SOURCE"] == "csv":
        if not _csv_reader_state["rows"]:
            _load_csv()
        row = _next_csv_row()
        if row:
            return row

    def rnd(lo, hi):
        return round(random.uniform(lo, hi), 2)

    return {
        "cpu": rnd(*CONFIG["CPU_RANGE"]),
        "jitter": rnd(*CONFIG["JITTER_RANGE"]),
        "delay": rnd(*CONFIG["DELAY_RANGE"]),
        "lambda_": rnd(*CONFIG["LAMBDA_RANGE"]),
        "ims_total": random.randint(*CONFIG["IMS_SESSION_RANGE"]),
    }


def normalize_weights(weights: dict) -> dict:
    base = {
        "voip": max(0.0, float(weights.get("voip", 0))),
        "video": max(0.0, float(weights.get("video", 0))),
        "web": max(0.0, float(weights.get("web", 0))),
    }
    total = sum(base.values()) or 1.0
    return {key: value / total for key, value in base.items()}


def split_sessions(total_sessions: int) -> dict:
    weights = normalize_weights(CONFIG["SERVICE_WEIGHTS"])
    voip = int(round(total_sessions * weights["voip"]))
    video = int(round(total_sessions * weights["video"]))
    web = max(0, total_sessions - voip - video)
    return {"voip": voip, "video": video, "web": web}


def build_action_payload(action_key: str, status: str, mode: str) -> dict:
    template = ACTION_TEMPLATES.get(action_key, ACTION_TEMPLATES["observe"])
    return {
        "mode": mode,
        "scenario": action_key,
        "status": status,
        "service": template["service"],
        "priority": template["priority"],
        "analysis": template["analysis"],
        "decision": template["decision"],
        "optimization": template["optimization"],
    }


def resolve_action(raw: dict, sessions: dict, current_status: str) -> dict:
    if CONFIG["ACTION_MODE"] == "manual":
        forced_status = CONFIG["FORCED_STATUS"]
        return build_action_payload(CONFIG["FORCED_ACTION"], forced_status, "manual")

    lambda_ratio = raw["lambda_"] / CONFIG["MU"] if CONFIG["MU"] else 0

    if raw["jitter"] >= CONFIG["VOIP_JITTER_THRESHOLD"] and sessions["voip"] >= 10:
        status = "Critical" if raw["jitter"] >= CONFIG["VOIP_JITTER_THRESHOLD"] + 5 else "Warning"
        return build_action_payload("voip_priority", status, "auto")

    if raw["delay"] >= CONFIG["VIDEO_DELAY_THRESHOLD"] and sessions["video"] >= 8:
        status = "Critical" if raw["delay"] >= CONFIG["VIDEO_DELAY_THRESHOLD"] + 8 else "Warning"
        return build_action_payload("video_reserve", status, "auto")

    if raw["cpu"] >= CONFIG["CPU_WARNING_THRESHOLD"] or lambda_ratio >= CONFIG["LAMBDA_WARNING_RATIO"]:
        status = "Critical" if current_status == "Critical" else "Warning"
        return build_action_payload("load_balance", status, "auto")

    return build_action_payload("observe", "Normal", "auto")


def build_snapshot() -> dict:
    raw = get_raw_metrics()
    ims_total = int(raw.get("ims_total") or random.randint(*CONFIG["IMS_SESSION_RANGE"]))
    sessions = split_sessions(ims_total)
    qos = calculate_qos(raw["delay"], raw["jitter"], raw["cpu"])
    wait = calculate_mm1_wait(raw["lambda_"])
    status = classify_status(qos)
    action_center = resolve_action(raw, sessions, status)
    if action_center["status"] == "Critical":
        status = "Critical"
    elif action_center["status"] == "Warning" and status == "Normal":
        status = "Warning"

    return {
        "cpu": raw["cpu"],
        "jitter": raw["jitter"],
        "delay": raw["delay"],
        "lambda_": raw["lambda_"],
        "ims_total": ims_total,
        "service_sessions": sessions,
        "qos": qos,
        "wq": wait,
        "status": status,
        "action_center": {**action_center, "status": status},
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def normalize_config_value(key: str, value):
    if key in {"CPU_RANGE", "JITTER_RANGE", "DELAY_RANGE", "LAMBDA_RANGE", "IMS_SESSION_RANGE"} and isinstance(value, list):
        return tuple(value)
    if key == "SERVICE_WEIGHTS" and isinstance(value, dict):
        return normalize_weights(value)
    return value


def ensure_push_loop():
    """Start the metric publisher once for the current process."""
    global _push_thread
    with _push_lock:
        if _push_thread is None:
            _push_thread = socketio.start_background_task(_push_loop)


def _push_loop():
    while True:
        try:
            snapshot = build_snapshot()
            insert_metric(snapshot)
            socketio.emit("metric_update", snapshot)
        except Exception as exc:
            print(f"[push_loop] error: {exc}")
        socketio.sleep(CONFIG["PUSH_INTERVAL"])


@app.route("/")
def index():
    ensure_push_loop()
    return render_template("index.html")


@app.route("/api/snapshot")
def api_snapshot():
    snapshot = build_snapshot()
    insert_metric(snapshot)
    return jsonify(snapshot)


@app.route("/api/history")
def api_history():
    limit = int(request.args.get("limit", 60))
    return jsonify(fetch_history(limit))


@app.route("/api/mm1_curve")
def api_mm1_curve():
    mu = float(request.args.get("mu", CONFIG["MU"]))
    return jsonify({"mu": mu, "curve": mm1_curve(mu)})


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    updatable = {
        "MU",
        "PUSH_INTERVAL",
        "DATA_SOURCE",
        "CPU_RANGE",
        "JITTER_RANGE",
        "DELAY_RANGE",
        "LAMBDA_RANGE",
        "IMS_SESSION_RANGE",
        "W_DELAY",
        "W_JITTER",
        "W_CPU",
        "SERVICE_WEIGHTS",
        "VOIP_JITTER_THRESHOLD",
        "VIDEO_DELAY_THRESHOLD",
        "CPU_WARNING_THRESHOLD",
        "LAMBDA_WARNING_RATIO",
        "ACTION_MODE",
        "FORCED_ACTION",
        "FORCED_STATUS",
    }
    if request.method == "POST":
        data = request.get_json(force=True)
        for key, value in data.items():
            if key in updatable:
                CONFIG[key] = normalize_config_value(key, value)
        return jsonify({
            "ok": True,
            "config": {key: CONFIG[key] for key in sorted(updatable)},
            "available_actions": sorted(ACTION_TEMPLATES.keys()),
        })
    return jsonify({
        "config": {key: CONFIG[key] for key in sorted(updatable)},
        "available_actions": sorted(ACTION_TEMPLATES.keys()),
    })


@socketio.on("connect")
def on_connect():
    ensure_push_loop()
    emit("history", fetch_history(30))
    emit("current_snapshot", build_snapshot())


if __name__ == "__main__":
    init_db()
    _load_csv()
    ensure_push_loop()
    print("GPON/IMS Monitor running on http://0.0.0.0:5000")
    socketio.run(
        app,
        host="0.0.0.0",
        port=5000,
        debug=False,
        allow_unsafe_werkzeug=True,
    )
