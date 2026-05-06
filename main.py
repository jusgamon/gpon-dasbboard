"""
GPON/IMS Network Monitoring Dashboard - Backend
================================================
Flask + SocketIO server with QoS scoring, M/M/1 queuing model,
SQLite persistence, and configurable simulation parameters.
"""

import os
import csv
import math
import time
import random
import sqlite3
import threading
from datetime import datetime
from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO, emit

# ─────────────────────────────────────────────
# App Initialisation
# ─────────────────────────────────────────────
app = Flask(__name__)
app.config["SECRET_KEY"] = "gpon-ims-poc-secret"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ─────────────────────────────────────────────
# Configurable Simulation Parameters
# ─────────────────────────────────────────────
CONFIG = {
    # M/M/1 service rate (packets/ms or requests/s — conceptual unit)
    "MU": 10.0,
    # Simulation push interval (seconds)
    "PUSH_INTERVAL": 1,
    # Data source: "simulate" | "csv"
    "DATA_SOURCE": "simulate",
    "CSV_PATH": "data.csv",
    # Simulation ranges (min, max)
    "CPU_RANGE": (5, 95),
    "JITTER_RANGE": (1, 30),
    "DELAY_RANGE": (2, 50),
    "LAMBDA_RANGE": (0.5, 9.5),
    # History kept in DB (rows)
    "MAX_HISTORY": 200,
    # QoS weights  (must sum weights × metrics sensibly)
    "W_DELAY": 0.4,
    "W_JITTER": 0.3,
    "W_CPU": 0.3,
}

DB_PATH = "network_metrics.db"

# ─────────────────────────────────────────────
# Database helpers
# ─────────────────────────────────────────────

def init_db():
    """Create metrics table if it doesn't exist."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
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
        """)
        conn.commit()


def insert_metric(row: dict):
    """Insert one metric snapshot and prune old rows."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO metrics (ts, cpu, jitter, delay, lambda_, qos, wq, status)
            VALUES (:ts, :cpu, :jitter, :delay, :lambda_, :qos, :wq, :status)
        """, row)
        conn.execute(f"""
            DELETE FROM metrics WHERE id NOT IN (
                SELECT id FROM metrics ORDER BY id DESC LIMIT {CONFIG['MAX_HISTORY']}
            )
        """)
        conn.commit()


def fetch_history(limit=60):
    """Return last `limit` rows ordered oldest-first."""
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.execute(
            "SELECT * FROM metrics ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = cur.fetchall()
    return [dict(r) for r in reversed(rows)]

# ─────────────────────────────────────────────
# Core Computation Functions
# ─────────────────────────────────────────────

def calculate_qos(delay: float, jitter: float, cpu: float) -> float:
    """
    QoS Score formula:
        QoS = 100 - (W_DELAY×Delay + W_JITTER×Jitter + W_CPU×CPU)
    Clamped to [0, 100].
    """
    score = 100.0 - (
        CONFIG["W_DELAY"] * delay +
        CONFIG["W_JITTER"] * jitter +
        CONFIG["W_CPU"] * cpu
    )
    return round(max(0.0, min(100.0, score)), 2)


def calculate_mm1_wait(lambda_: float, mu: float = None) -> float:
    """
    M/M/1 expected waiting time in queue:
        W = 1 / (μ - λ),  valid only when λ < μ.
    Returns float('inf') when system is saturated.
    """
    if mu is None:
        mu = CONFIG["MU"]
    if lambda_ >= mu:
        return float("inf")
    return round(1.0 / (mu - lambda_), 4)


def classify_status(qos: float) -> str:
    """Map QoS score to operational status label."""
    if qos > 80:
        return "Normal"
    elif qos > 50:
        return "Warning"
    else:
        return "Critical"


def mm1_curve(mu: float = None, steps: int = 50):
    """
    Generate (λ, W) pairs for the M/M/1 theoretical curve
    from λ=0.1 to λ=0.99×μ.
    """
    if mu is None:
        mu = CONFIG["MU"]
    points = []
    for i in range(1, steps + 1):
        lam = round(mu * (i / (steps + 1)), 4)
        w = calculate_mm1_wait(lam, mu)
        points.append({"lambda": lam, "W": w if w != float("inf") else None})
    return points

# ─────────────────────────────────────────────
# Data Source
# ─────────────────────────────────────────────

_csv_reader_state = {"rows": [], "index": 0}


def _load_csv():
    path = CONFIG["CSV_PATH"]
    if not os.path.exists(path):
        return False
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
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
        "cpu":     float(row["cpu"]),
        "jitter":  float(row["jitter"]),
        "delay":   float(row["delay"]),
        "lambda_": float(row["lambda"]),
    }


def get_raw_metrics() -> dict:
    """Return a raw metrics dict from configured data source."""
    if CONFIG["DATA_SOURCE"] == "csv":
        if not _csv_reader_state["rows"]:
            _load_csv()
        row = _next_csv_row()
        if row:
            return row

    # Default: simulation
    def rnd(lo, hi):
        return round(random.uniform(lo, hi), 2)

    return {
        "cpu":     rnd(*CONFIG["CPU_RANGE"]),
        "jitter":  rnd(*CONFIG["JITTER_RANGE"]),
        "delay":   rnd(*CONFIG["DELAY_RANGE"]),
        "lambda_": rnd(*CONFIG["LAMBDA_RANGE"]),
    }


def build_snapshot() -> dict:
    """Assemble a complete timestamped snapshot."""
    raw = get_raw_metrics()
    qos    = calculate_qos(raw["delay"], raw["jitter"], raw["cpu"])
    wq     = calculate_mm1_wait(raw["lambda_"])
    status = classify_status(qos)
    ts     = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")
    return {**raw, "qos": qos, "wq": wq, "status": status, "ts": ts}

# ─────────────────────────────────────────────
# Background push thread
# ─────────────────────────────────────────────

_push_thread_running = False


def _push_loop():
    global _push_thread_running
    _push_thread_running = True
    while _push_thread_running:
        try:
            snap = build_snapshot()
            insert_metric(snap)
            socketio.emit("metric_update", snap)
        except Exception as e:
            print(f"[push_loop] error: {e}")
        time.sleep(CONFIG["PUSH_INTERVAL"])

# ─────────────────────────────────────────────
# Flask Routes
# ─────────────────────────────────────────────

@app.route("/")
def index():
    return render_template("index.html")


@app.route("/api/snapshot")
def api_snapshot():
    """Return a single live snapshot (for polling fallback)."""
    snap = build_snapshot()
    insert_metric(snap)
    return jsonify(snap)


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
    """GET returns current CONFIG; POST updates it (subset of keys allowed)."""
    UPDATABLE = {
        "MU", "PUSH_INTERVAL", "DATA_SOURCE",
        "CPU_RANGE", "JITTER_RANGE", "DELAY_RANGE", "LAMBDA_RANGE",
        "W_DELAY", "W_JITTER", "W_CPU",
    }
    if request.method == "POST":
        data = request.get_json(force=True)
        for k, v in data.items():
            if k in UPDATABLE:
                CONFIG[k] = v
        return jsonify({"ok": True, "config": {k: CONFIG[k] for k in UPDATABLE}})
    return jsonify({k: CONFIG[k] for k in UPDATABLE})

# ─────────────────────────────────────────────
# SocketIO events
# ─────────────────────────────────────────────

@socketio.on("connect")
def on_connect():
    history = fetch_history(30)
    emit("history", history)


@socketio.on("request_snapshot")
def on_request_snapshot():
    snap = build_snapshot()
    insert_metric(snap)
    emit("metric_update", snap)

# ─────────────────────────────────────────────
# Entry point
# ─────────────────────────────────────────────

if __name__ == "__main__":
    init_db()
    _load_csv()                          # pre-load CSV rows (no-op if file absent)
    t = threading.Thread(target=_push_loop, daemon=True)
    t.start()
    print("▶  GPON/IMS Monitor running on http://0.0.0.0:5000")
    socketio.run(app, host="0.0.0.0", port=5000, debug=False)