# ═══════════════════════════════════════════════════════════════════
# 1.  Imports & app bootstrap
# ═══════════════════════════════════════════════════════════════════

import csv
import math
import os
import random
import sqlite3
import time
from collections import deque
from datetime import datetime, timezone
from threading import Lock

import eventlet
eventlet.monkey_patch()

from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config["SECRET_KEY"] = "gpon-ims-poc-secret"

socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="eventlet",
    json=None,
    logger=False,
    engineio_logger=False,
    ping_interval=25,
    ping_timeout=120,
    allow_upgrades=True,
    binary=False,
)

# ═══════════════════════════════════════════════════════════════════
# 2.  CONFIG & constants
# ═══════════════════════════════════════════════════════════════════

CONFIG: dict = {
    # ── M/M/1 model ─────────────────────────────────────────────────
    "MU": 10.0,                     # service rate (requests / s)

    # ── Push cadence ────────────────────────────────────────────────
    "PUSH_INTERVAL": 2,             # seconds between SocketIO emits

    # ── Data source ─────────────────────────────────────────────────
    "DATA_SOURCE": "simulate",      # "simulate" | "csv"
    "CSV_PATH": "data.csv",

    # ── Simulation ranges (min, max) ────────────────────────────────
    "CPU_RANGE":         (8,  92),
    "JITTER_RANGE":      (2,  45),
    "DELAY_RANGE":       (5,  80),
    "LAMBDA_RANGE":      (1.5, 9.6),
    "IMS_SESSION_RANGE": (12, 84),

    # ── QoS formula weights  (W_DELAY*d + W_JITTER*j + W_CPU*c) ────
    "W_DELAY":  0.4,
    "W_JITTER": 0.3,
    "W_CPU":    0.3,

    # ── IMS service-mix (normalised internally) ──────────────────────
    "SERVICE_WEIGHTS": {"voip": 0.40, "video": 0.35, "web": 0.25},

    # ── Simulation physics ──────────────────────────────────────────
    # CHAOS     - magnitude of random shocks (0 = smooth, 1.5 = wild)
    # MOMENTUM  - how much previous velocity carries forward (0-0.97)
    "SIMULATION_CHAOS":    0.22,
    "SIMULATION_MOMENTUM": 0.78,

    # ── Alert thresholds ────────────────────────────────────────────
    "VOIP_JITTER_THRESHOLD":  20,   # ms above which VoIP SHI degrades
    "VIDEO_DELAY_THRESHOLD":  28,   # ms above which Video SHI degrades
    "CPU_WARNING_THRESHOLD":  85,   # % above which transport score rises
    "LAMBDA_WARNING_RATIO":   0.82, # lambda/mu ratio above which load alarm fires

    # ── Action-center mode ──────────────────────────────────────────
    "ACTION_MODE":   "auto",        # "auto" | "manual"
    "FORCED_ACTION": "observe",     # used only when ACTION_MODE = "manual"
    "FORCED_STATUS": "Normal",      # used only when ACTION_MODE = "manual"
}

# Keys whose POST values must be converted from list -> tuple
_RANGE_KEYS = frozenset({
    "CPU_RANGE", "JITTER_RANGE", "DELAY_RANGE",
    "LAMBDA_RANGE", "IMS_SESSION_RANGE",
})

# Keys that callers are allowed to update via POST /api/config
_UPDATABLE_KEYS = frozenset({
    "MU", "PUSH_INTERVAL", "DATA_SOURCE", "CSV_PATH",
    "CPU_RANGE", "JITTER_RANGE", "DELAY_RANGE",
    "LAMBDA_RANGE", "IMS_SESSION_RANGE",
    "W_DELAY", "W_JITTER", "W_CPU",
    "SERVICE_WEIGHTS",
    "SIMULATION_CHAOS", "SIMULATION_MOMENTUM",
    "VOIP_JITTER_THRESHOLD", "VIDEO_DELAY_THRESHOLD",
    "CPU_WARNING_THRESHOLD", "LAMBDA_WARNING_RATIO",
    "ACTION_MODE", "FORCED_ACTION", "FORCED_STATUS",
})

# Action library
ACTION_LIBRARY: dict = {
    "observe": {
        "service":  "Bütün xidmətlər",
        "priority": "AŞAĞI",
        "patch":    "Həll tələb olunmur. Əsas nəqliyyat siyasətini aktiv saxlayın.",
    },
    "rtp_priority_qos": {
        "service":  "VoIP",
        "priority": "YÜKSƏK",
        "patch":    "cli: qos policy update class VOIP set dscp ef queue strict-priority",
    },
    "increase_bandwidth_reservation": {
        "service":  "Video",
        "priority": "ORTA",
        "patch":    "config: ims.video.reservation=+15% and gpon.tcont.video.assured_bw=boost",
    },
    "load_balance_secondary": {
        "service":  "Nəqliyyat",
        "priority": "YÜKSƏK",
        "patch":    "cli: orchestrator rebalance --target secondary-vnf --drain best-effort 20%",
    },
    "preemptive_shaping": {
        "service":  "Nəqliyyat",
        "priority": "YÜKSƏK",
        "patch":    "cli: traffic-shaper apply profile preemptive_guard --window 30s",
    },
}

# Warm-boot reference values
_NORMAL_BOOTSTRAP = {
    "cpu":      22.0,
    "jitter":    6.0,
    "delay":    12.0,
    "lambda_":   3.2,
    "ims_total": 24,
}

DB_PATH = "network_metrics.db"

# ═══════════════════════════════════════════════════════════════════
# 3.  Database helpers
# ═══════════════════════════════════════════════════════════════════

def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id       INTEGER PRIMARY KEY AUTOINCREMENT,
                ts       TEXT NOT NULL,
                cpu      REAL NOT NULL,
                jitter   REAL NOT NULL,
                delay    REAL NOT NULL,
                lambda_  REAL NOT NULL,
                qos      REAL NOT NULL,
                wq       REAL NOT NULL,
                status   TEXT NOT NULL
            )
        """)
        conn.commit()


def insert_metric(snapshot: dict) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO metrics (ts, cpu, jitter, delay, lambda_, qos, wq, status)
            VALUES (:ts, :cpu, :jitter, :delay, :lambda_, :qos, :wq, :status)
        """, {
            "ts":      snapshot["ts"],
            "cpu":     snapshot["cpu"],
            "jitter":  snapshot["jitter"],
            "delay":   snapshot["delay"],
            "lambda_": snapshot["lambda_"],
            "qos":     snapshot["qos"],
            "wq":      snapshot["wq"],
            "status":  snapshot["status"],
        })
        conn.commit()


def fetch_history(limit: int = 60) -> list:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM metrics ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in reversed(rows)]


# ═══════════════════════════════════════════════════════════════════
# 4.  Math / QoS / M/M/1 models
# ═══════════════════════════════════════════════════════════════════

def calculate_qos(delay: float, jitter: float, cpu: float) -> float:
    """QoS = 100 - (W_d*delay + W_j*jitter + W_c*cpu), clamped [0, 100]."""
    score = (
        100.0
        - CONFIG["W_DELAY"]  * delay
        - CONFIG["W_JITTER"] * jitter
        - CONFIG["W_CPU"]    * cpu
    )
    return round(max(0.0, min(100.0, score)), 2)


def calculate_mm1_wait(lambda_: float, mu: float = None) -> float:
    """W = 1 / (mu - lambda).  Returns inf when lambda >= mu."""
    mu = mu if mu is not None else CONFIG["MU"]
    if lambda_ >= mu:
        return float("inf")
    return round(1.0 / (mu - lambda_), 4)


def classify_status(qos: float) -> str:
    if qos > 80:
        return "Normal"
    if qos > 50:
        return "Warning"
    return "Critical"


def mm1_curve(mu: float = None, steps: int = 50) -> list:
    """Return (lambda, W) pairs from lambda=0 up to 0.99*mu for plotting."""
    mu = mu if mu is not None else CONFIG["MU"]
    points = []
    for i in range(1, steps + 1):
        lam  = round(mu * i / (steps + 1), 4)
        wait = calculate_mm1_wait(lam, mu)
        points.append({"lambda": lam, "W": wait if math.isfinite(wait) else None})
    return points


def severity_rank(status: str) -> int:
    return {"Normal": 0, "Warning": 1, "Critical": 2}.get(status, 0)


# ═══════════════════════════════════════════════════════════════════
# 5.  Simulation engine
# ═══════════════════════════════════════════════════════════════════

_sim_state: dict = None


def _clamp(value: float, bounds: tuple) -> float:
    return max(bounds[0], min(bounds[1], value))


def _init_sim_state() -> dict:
    return {
        **_NORMAL_BOOTSTRAP,
        "lambda_velocity": 0.0,
        "shock":           0.0,
        "tick":            0,
        "warmup":          8,
    }


def _seed_sim_from_history(history: list) -> dict:
    if not history:
        return _init_sim_state()

    latest = history[-1]
    blended = {
        "cpu":      latest["cpu"]      * 0.25 + _NORMAL_BOOTSTRAP["cpu"]      * 0.75,
        "jitter":   latest["jitter"]   * 0.20 + _NORMAL_BOOTSTRAP["jitter"]   * 0.80,
        "delay":    latest["delay"]    * 0.20 + _NORMAL_BOOTSTRAP["delay"]    * 0.80,
        "lambda_":  latest["lambda_"]  * 0.35 + _NORMAL_BOOTSTRAP["lambda_"]  * 0.65,
        "ims_total": int(round(
            latest.get("ims_total", _NORMAL_BOOTSTRAP["ims_total"]) * 0.3
            + _NORMAL_BOOTSTRAP["ims_total"] * 0.7
        )),
    }

    # Force a clean start if the blended QoS is still degraded
    if calculate_qos(blended["delay"], blended["jitter"], blended["cpu"]) < 82:
        blended.update(cpu=min(blended["cpu"], 28.0),
                       jitter=min(blended["jitter"], 8.0),
                       delay=min(blended["delay"], 14.0))

    return {
        "cpu":             round(_clamp(blended["cpu"],     CONFIG["CPU_RANGE"]),     2),
        "jitter":          round(_clamp(blended["jitter"],  CONFIG["JITTER_RANGE"]),  2),
        "delay":           round(_clamp(blended["delay"],   CONFIG["DELAY_RANGE"]),   2),
        "lambda_":         round(_clamp(blended["lambda_"], CONFIG["LAMBDA_RANGE"]),  2),
        "ims_total":       int(_clamp(blended["ims_total"], CONFIG["IMS_SESSION_RANGE"])),
        "lambda_velocity": 0.0,
        "shock":           0.0,
        "tick":            0,
        "warmup":          8,
    }


def seed_runtime_state() -> None:
    global _sim_state
    history = fetch_history(12)

    for row in history:
        _add_telemetry_sample({
            "ts_epoch": _parse_ts_epoch(row["ts"]),
            "cpu":      float(row["cpu"]),
            "jitter":   float(row["jitter"]),
            "delay":    float(row["delay"]),
            "lambda_":  float(row["lambda_"]),
        })

    _sim_state = _seed_sim_from_history(history)
    _log("startup",
         f"sim seeded: cpu={_sim_state['cpu']:.1f} jitter={_sim_state['jitter']:.1f} "
         f"delay={_sim_state['delay']:.1f} lambda={_sim_state['lambda_']:.2f}")


def simulate_metrics() -> dict:
    """
    - lambda follows a mean-reverting random walk driven by a momentum term.
    - Random shocks inject occasional bursts (probability scales with CHAOS).
    - CPU, jitter, and delay are each modelled as exponential smoothing
      toward a target that is a function of lambda and the shock.
    - MOMENTUM controls how much the previous velocity carries forward
      (higher = smoother but slower to respond).
    - CHAOS scales shock magnitude and noise amplitude
      (0 = smooth/realistic, 1.5 = erratic/stress-test).
    - A warmup period damps everything to healthy values on first boot.
    """
    global _sim_state
    if _sim_state is None:
        _sim_state = _init_sim_state()

    chaos    = max(0.0, min(1.5,  float(CONFIG["SIMULATION_CHAOS"])))
    momentum = max(0.0, min(0.97, float(CONFIG["SIMULATION_MOMENTUM"])))
    mu       = max(0.001, float(CONFIG["MU"]))

    state  = _sim_state
    warmup = max(0, int(state.get("warmup", 0)))
    state["tick"] += 1

    # Shocks: probability of a new shock rises with chaos.
    if random.random() < 0.08 + chaos * 0.08:
        state["shock"] = random.uniform(-1.0, 1.0) * (0.18 + chaos * 0.95)
    else:
        state["shock"] *= 0.72

    effective_chaos = chaos * (0.35 if warmup > 0 else 1.0)
    if warmup > 0:
        state["shock"] *= 0.35

    # lambda random walk with mean-reversion
    lambda_center = (CONFIG["LAMBDA_RANGE"][0] + CONFIG["LAMBDA_RANGE"][1]) / 2.0
    lambda_drift  = (lambda_center - state["lambda_"]) * 0.08
    lambda_noise  = random.uniform(-0.28, 0.28) * (0.25 + effective_chaos)

    state["lambda_velocity"] = (
        state["lambda_velocity"] * momentum
        + lambda_drift  * 0.4
        + lambda_noise
        + state["shock"] * 0.22
    )
    state["lambda_"] = _clamp(
        state["lambda_"] + state["lambda_velocity"],
        CONFIG["LAMBDA_RANGE"],
    )

    # IMS sessions track lambda with lag
    lambda_ratio  = state["lambda_"] / mu
    session_range = CONFIG["IMS_SESSION_RANGE"]
    base_sessions = session_range[0] + (session_range[1] - session_range[0]) * min(1.0, lambda_ratio)
    session_noise = random.uniform(-2.5, 2.5) * (0.5 + effective_chaos)
    state["ims_total"] = int(round(_clamp(
        state["ims_total"] * 0.55 + base_sessions * 0.45 + session_noise,
        session_range,
    )))

    # CPU is a function of lambda-ratio, sessions, and shock
    cpu_target = (
        16.0
        + lambda_ratio       * 58.0
        + state["ims_total"] * 0.12
        + abs(state["shock"]) * 16.0
    )
    cpu_noise = random.uniform(-2.2, 2.2) * (0.55 + effective_chaos)
    state["cpu"] = _clamp(
        state["cpu"] * 0.72 + cpu_target * 0.28 + cpu_noise,
        CONFIG["CPU_RANGE"],
    )

    # Jitter rises with lambda-ratio and high CPU
    jitter_target = (
        4.0
        + lambda_ratio * 10.0
        + max(0.0, state["cpu"] - 55.0) * 0.12
        + abs(state["shock"]) * 7.0
    )
    jitter_noise = random.uniform(-1.3, 1.3) * (0.45 + effective_chaos)
    state["jitter"] = _clamp(
        state["jitter"] * 0.62 + jitter_target * 0.38 + jitter_noise,
        CONFIG["JITTER_RANGE"],
    )

    # Delay is correlated with jitter and CPU
    delay_target = (
        8.0
        + lambda_ratio    * 18.0
        + state["jitter"] * 0.85
        + max(0.0, state["cpu"] - 60.0) * 0.16
    )
    delay_noise = random.uniform(-1.8, 1.8) * (0.45 + effective_chaos)
    state["delay"] = _clamp(
        state["delay"] * 0.66 + delay_target * 0.34 + delay_noise,
        CONFIG["DELAY_RANGE"],
    )

    # Warmup caps: keep values in healthy territory for first N ticks
    if warmup > 0:
        state["lambda_"] = min(state["lambda_"], 4.2)
        state["cpu"]     = min(state["cpu"],     31.0)
        state["jitter"]  = min(state["jitter"],   8.5)
        state["delay"]   = min(state["delay"],   16.0)
        state["warmup"]  = warmup - 1

    return {
        "cpu":       round(state["cpu"],     2),
        "jitter":    round(state["jitter"],  2),
        "delay":     round(state["delay"],   2),
        "lambda_":   round(state["lambda_"], 2),
        "ims_total": int(state["ims_total"]),
    }


# CSV data source helpers

_csv_state: dict = {"rows": [], "index": 0}


def _load_csv() -> bool:
    path = CONFIG["CSV_PATH"]
    if not os.path.exists(path):
        return False
    with open(path, newline="") as f:
        _csv_state["rows"] = list(csv.DictReader(f))
    _csv_state["index"] = 0
    return bool(_csv_state["rows"])


def _next_csv_row() -> dict:
    rows = _csv_state["rows"]
    if not rows:
        return None
    row = rows[_csv_state["index"] % len(rows)]
    _csv_state["index"] += 1
    return {
        "cpu":       float(row["cpu"]),
        "jitter":    float(row["jitter"]),
        "delay":     float(row["delay"]),
        "lambda_":   float(row["lambda"]),
        "ims_total": int(float(row.get("ims_total", 0) or 0)),
    }


def get_raw_metrics() -> dict:
    if CONFIG["DATA_SOURCE"] == "csv":
        if not _csv_state["rows"]:
            _load_csv()
        row = _next_csv_row()
        if row:
            return row
    return simulate_metrics()


# ═══════════════════════════════════════════════════════════════════
# 6.  Telemetry window & gradient analytics
# ═══════════════════════════════════════════════════════════════════

# Rolling window of the last 120 samples for gradient computation
_telemetry_window: deque = deque(maxlen=120)


def _parse_ts_epoch(ts: str) -> float:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except Exception:
        return time.time()


def _add_telemetry_sample(sample: dict) -> None:
    _telemetry_window.append(sample)


def _interpolate_past(key: str, seconds_ago: float, current_ts: float, fallback: float) -> float:
    """Return the value of `key` from ~seconds_ago seconds in the past."""
    target_ts = current_ts - seconds_ago
    for point in reversed(_telemetry_window):
        if point["ts_epoch"] <= target_ts:
            return float(point[key])
    return float(_telemetry_window[0][key]) if _telemetry_window else fallback


def compute_gradients(raw: dict, now: float) -> dict:
    """Rate of change (units/s) over the last 10 s for each key metric."""
    window = 10.0
    return {
        key: round(
            (float(raw[key]) - _interpolate_past(key, window, now, float(raw[key]))) / window,
            4,
        )
        for key in ("jitter", "delay", "cpu", "lambda_")
    }


# ═══════════════════════════════════════════════════════════════════
# 7.  Service-health & queue-projection helpers
# ═══════════════════════════════════════════════════════════════════

def _normalise_weights(w: dict) -> dict:
    """Normalise service-weight dict so values sum to 1."""
    values = {k: max(0.0, float(w.get(k, 0))) for k in ("voip", "video", "web")}
    total  = sum(values.values()) or 1.0
    return {k: v / total for k, v in values.items()}


def split_sessions(total: int) -> dict:
    weights = _normalise_weights(CONFIG["SERVICE_WEIGHTS"])
    voip  = int(round(total * weights["voip"]))
    video = int(round(total * weights["video"]))
    web   = max(0, total - voip - video)
    return {"voip": voip, "video": video, "web": web}


def compute_service_health(raw: dict) -> dict:
    """Service Health Index (SHI) in [0, 100] per service type."""
    shi_voip  = max(0.0, min(100.0, 100.0 - (raw["jitter"] * 2.5 + raw["delay"] * 0.5)))
    shi_video = max(0.0, min(100.0, 100.0 - (raw["delay"]  * 1.5 + raw["cpu"]   * 0.2)))
    return {"voip": round(shi_voip, 2), "video": round(shi_video, 2)}


def projected_queue_metrics(raw: dict, gradients: dict) -> dict:
    """Project lambda and Wq 30 s into the future using current gradient."""
    proj_lambda = max(0.01, raw["lambda_"] + gradients["lambda_"] * 30.0)
    proj_wq     = calculate_mm1_wait(proj_lambda)

    if gradients["lambda_"] > 0 and raw["lambda_"] < CONFIG["MU"]:
        time_to_sat = max(0.0, (CONFIG["MU"] - raw["lambda_"]) / gradients["lambda_"])
    else:
        time_to_sat = float("inf")

    return {
        "projected_lambda_30s":  round(proj_lambda, 3),
        "projected_wq_30s":      proj_wq,
        "projected_wq_30s_ms":   round(proj_wq * 1000, 2) if math.isfinite(proj_wq) else float("inf"),
        "time_to_saturation_s":  round(time_to_sat, 2)    if math.isfinite(time_to_sat) else float("inf"),
    }


# ═══════════════════════════════════════════════════════════════════
# 8.  Decision / action-center engine
# ═══════════════════════════════════════════════════════════════════

_decision_state: dict = {
    "last_action": None,
    "last_result": "Hələ heç bir əməliyyat qiymətləndirilməyib.",
}


def _action_status_from_score(score: float) -> str:
    if score >= 2.25:
        return "Critical"
    if score >= 0.90:
        return "Warning"
    return "Normal"


def _build_candidate_scores(raw: dict, sessions: dict, gradients: dict,
                             shi: dict, queue_proj: dict) -> dict:
    lambda_ratio = raw["lambda_"] / max(CONFIG["MU"], 0.001)
    total        = max(sessions["voip"] + sessions["video"] + sessions["web"], 1)
    scores       = {"observe": 0.15}

    # VoIP / RTP priority
    if sessions["voip"] > 0:
        voip_load = sessions["voip"] / total
        scores["rtp_priority_qos"] = round(
            max(0.0, gradients["jitter"])
            + max(0.0, (CONFIG["VOIP_JITTER_THRESHOLD"] - shi["voip"]) / 12.0)
            + max(0.0, (raw["jitter"] - CONFIG["VOIP_JITTER_THRESHOLD"]) / 6.0)
            + voip_load * 1.2,
            4,
        )
    else:
        scores["rtp_priority_qos"] = 0.0

    # Video / bandwidth reservation
    if sessions["video"] > 0:
        video_load = sessions["video"] / total
        scores["increase_bandwidth_reservation"] = round(
            max(0.0, gradients["delay"])
            + max(0.0, (CONFIG["VIDEO_DELAY_THRESHOLD"] - shi["video"]) / 16.0)
            + max(0.0, (raw["delay"] - CONFIG["VIDEO_DELAY_THRESHOLD"]) / 9.0)
            + max(0.0, (raw["cpu"] - 70.0) / 18.0)
            + video_load,
            4,
        )
    else:
        scores["increase_bandwidth_reservation"] = 0.0

    # Transport load-balance
    scores["load_balance_secondary"] = round(
        max(0.0, lambda_ratio - CONFIG["LAMBDA_WARNING_RATIO"]) * 5.5
        + max(0.0, gradients["lambda_"]) * 1.3
        + max(0.0, (raw["cpu"] - CONFIG["CPU_WARNING_THRESHOLD"]) / 6.0)
        + (2.2 if queue_proj["time_to_saturation_s"] <= 30 else 0.0),
        4,
    )

    # Pre-emptive shaping
    scores["preemptive_shaping"] = round(
        max(0.0, gradients["lambda_"]) * 1.5
        + max(0.0, gradients["delay"]) * 0.8
        + (3.0 if queue_proj["projected_wq_30s_ms"] > 200 else 0.0)
        + (1.5 if queue_proj["time_to_saturation_s"] <= 30 else 0.0),
        4,
    )

    return scores


def _apply_hysteresis(candidate_key: str, candidate_status: str) -> tuple:
    """Suppress action flapping: hold the current action for 15 s unless
    a Critical event overrides it."""
    last = _decision_state["last_action"]
    if not last or (time.time() - last["applied_at"]) > 15:
        return candidate_key, candidate_status
    if candidate_key == last["scenario"]:
        return candidate_key, candidate_status
    if severity_rank(candidate_status) >= severity_rank("Critical"):
        return candidate_key, candidate_status
    return last["scenario"], last["status"]


def _assess_closed_loop(action_key: str, raw: dict, shi: dict, queue_proj: dict) -> str:
    """Compare current telemetry against the baseline recorded when the
    action was applied; return a human-readable outcome string."""
    last = _decision_state["last_action"]
    if not last or last["scenario"] != action_key:
        return _decision_state["last_result"]

    elapsed = time.time() - last["applied_at"]
    if elapsed < 6:
        return "Əməliyyat tətbiq edildi; dəyişiklikdən sonrakı telemetriya gözlənilir."

    baseline = last["baseline"]
    improved = 0
    checks   = 2

    if action_key == "rtp_priority_qos":
        improved += int(raw["jitter"] <= baseline["jitter"])
        improved += int(shi["voip"]   >= baseline["shi_voip"])
    elif action_key == "increase_bandwidth_reservation":
        improved += int(raw["delay"]  <= baseline["delay"])
        improved += int(shi["video"]  >= baseline["shi_video"])
    else:
        improved += int(raw["lambda_"] <= baseline["lambda_"])
        improved += int(queue_proj["projected_wq_30s_ms"] <= baseline["projected_wq_30s_ms"])

    result = (
        "Optimizasiya uğurla tamamlandı"
        if improved >= max(1, checks - 1)
        else "Əməliyyat təsirsiz oldu; eskalasiya edilir."
    )
    _decision_state["last_result"] = result
    return result


def _build_decision_object(action_key: str, status: str, mode: str,
                            raw: dict, sessions: dict, gradients: dict,
                            shi: dict, queue_proj: dict,
                            confidence: float, opt_result: str) -> dict:
    tmpl  = ACTION_LIBRARY[action_key]
    total = sessions["voip"] + sessions["video"] + sessions["web"]
    lambda_ratio = raw["lambda_"] / max(CONFIG["MU"], 0.001)

    diagnoses = {
        "rtp_priority_qos":               "Aktiv səs yükü zamanı IMS VoIP sessiyalarında artan jitter problemi müşahidə olunur.",
        "increase_bandwidth_reservation": "CPU yüklənməsi ve gecikmə artımı səbəbindən IMS video sessiyalarının keyfiyyəti zəifləyir.",
        "load_balance_secondary":         "Trafik intensivliyi xidmət tutumuna yaxınlaşdığı üçün nəqliyyat yükü artır.",
        "preemptive_shaping":             "Növbə artımı kritik limitlərə çatmadan əvvəl doyma vəziyyətinə yaxınlaşır.",
        "observe":                        "Cari telemetriya intervalında dominant xidmət problemi müşahidə olunmur.",
    }
    diagnosis = diagnoses.get(action_key, diagnoses["observe"])

    rationale_parts = [
        f"Son 10 s ərzində jitter {gradients['jitter']:.2f} ms/s, "
        f"gecikmə {gradients['delay']:.2f} ms/s, CPU {gradients['cpu']:.2f} %/s dəyişib.",
        f"VoIP SHI={shi['voip']:.1f}, Video SHI={shi['video']:.1f}.",
        f"lambda/mu = {lambda_ratio:.2f}.",
    ]
    if math.isfinite(queue_proj["projected_wq_30s_ms"]):
        rationale_parts.append(
            f"30 s sonrakı proqnoz Wq = {queue_proj['projected_wq_30s_ms']:.1f} ms."
        )
    if math.isfinite(queue_proj["time_to_saturation_s"]):
        rationale_parts.append(
            f"Doyma vəziyyəti ~{queue_proj['time_to_saturation_s']:.1f} s içindədir."
        )

    return {
        "mode":                action_key,
        "scenario":            action_key,
        "status":              status,
        "service":             tmpl["service"],
        "priority":            tmpl["priority"],
        "diagnosis":           diagnosis,
        "rationale":           " ".join(rationale_parts),
        "proposed_patch":      tmpl["patch"],
        "confidence_score":    round(confidence, 2),
        "optimization_result": opt_result,
        "analysis":            f"IMS sessiyaları cəmi={total}, VoIP={sessions['voip']}, Video={sessions['video']}.",
        "decision":            diagnosis,
        "optimization":        tmpl["patch"],
    }


def resolve_action(raw: dict, sessions: dict, gradients: dict,
                   shi: dict, queue_proj: dict, current_status: str) -> dict:
    """Top-level decision resolver. Returns a full decision object."""
    mode       = CONFIG["ACTION_MODE"]
    opt_result = _decision_state["last_result"]

    # Manual override
    if mode == "manual":
        return _build_decision_object(
            CONFIG["FORCED_ACTION"], CONFIG["FORCED_STATUS"],
            "manual", raw, sessions, gradients, shi, queue_proj,
            confidence=0.95, opt_result=opt_result,
        )

    # Automatic scoring
    scores     = _build_candidate_scores(raw, sessions, gradients, shi, queue_proj)
    action_key, top_score = max(scores.items(), key=lambda kv: kv[1])

    # Ignore low-confidence non-observe picks
    if action_key != "observe" and top_score < 0.75:
        action_key, top_score = "observe", scores["observe"]

    confidence = min(0.99, 0.35 + top_score / 5.0)
    status     = _action_status_from_score(top_score)

    if action_key == "observe":
        status = current_status if (current_status != "Normal" and top_score > 0.5) else "Normal"

    action_key, status = _apply_hysteresis(action_key, status)
    opt_result = _assess_closed_loop(action_key, raw, shi, queue_proj)

    decision = _build_decision_object(
        action_key, status, "auto",
        raw, sessions, gradients, shi, queue_proj,
        confidence=confidence, opt_result=opt_result,
    )

    # Record baseline for closed-loop assessment
    if action_key != "observe":
        last = _decision_state["last_action"]
        if last is None or last["scenario"] != action_key:
            _decision_state["last_action"] = {
                "scenario":   action_key,
                "status":     status,
                "applied_at": time.time(),
                "baseline": {
                    "jitter":              raw["jitter"],
                    "delay":               raw["delay"],
                    "lambda_":             raw["lambda_"],
                    "shi_voip":            shi["voip"],
                    "shi_video":           shi["video"],
                    "projected_wq_30s_ms": queue_proj["projected_wq_30s_ms"],
                },
            }
        else:
            last["status"] = status

    return decision


# ═══════════════════════════════════════════════════════════════════
# 9.  Snapshot assembly
# ═══════════════════════════════════════════════════════════════════

def build_snapshot() -> dict:
    raw       = get_raw_metrics()
    now_epoch = time.time()

    ims_total = int(raw.get("ims_total") or random.randint(*CONFIG["IMS_SESSION_RANGE"]))
    sessions  = split_sessions(ims_total)

    _add_telemetry_sample({
        "ts_epoch": now_epoch,
        "cpu":      raw["cpu"],
        "jitter":   raw["jitter"],
        "delay":    raw["delay"],
        "lambda_":  raw["lambda_"],
    })

    gradients  = compute_gradients(raw, now_epoch)
    shi        = compute_service_health(raw)
    queue_proj = projected_queue_metrics(raw, gradients)

    qos    = calculate_qos(raw["delay"], raw["jitter"], raw["cpu"])
    wq     = calculate_mm1_wait(raw["lambda_"])
    status = classify_status(qos)

    decision = resolve_action(raw, sessions, gradients, shi, queue_proj, status)

    # Action center may escalate status upward — never downgrade
    if severity_rank(decision["status"]) > severity_rank(status):
        status = decision["status"]

    return {
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
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


# ═══════════════════════════════════════════════════════════════════
# 10. Config normalisation helpers
# ═══════════════════════════════════════════════════════════════════

def _normalise_config_value(key: str, value):
    """Convert incoming JSON values to the correct Python types."""
    if key in _RANGE_KEYS and isinstance(value, (list, tuple)):
        lo, hi = float(value[0]), float(value[1])
        return (min(lo, hi), max(lo, hi))           # always (low, high)

    if key == "SERVICE_WEIGHTS" and isinstance(value, dict):
        return _normalise_weights(value)

    if key in ("MU", "W_DELAY", "W_JITTER", "W_CPU",
               "SIMULATION_CHAOS", "SIMULATION_MOMENTUM",
               "VOIP_JITTER_THRESHOLD", "VIDEO_DELAY_THRESHOLD",
               "CPU_WARNING_THRESHOLD", "LAMBDA_WARNING_RATIO"):
        return float(value)

    if key == "PUSH_INTERVAL":
        return max(1, int(value))                   # minimum 1 s

    return value


def _validate_config_value(key: str, value) -> tuple:
    """Return (is_valid, error_message). Empty string means no error."""
    try:
        if key == "SIMULATION_CHAOS":
            if not (0.0 <= float(value) <= 1.5):
                return False, "must be in [0.0, 1.5]"

        elif key == "SIMULATION_MOMENTUM":
            if not (0.0 <= float(value) <= 0.97):
                return False, "must be in [0.0, 0.97]"

        elif key == "MU":
            if float(value) <= 0:
                return False, "must be > 0"

        elif key in ("W_DELAY", "W_JITTER", "W_CPU"):
            if not (0.0 <= float(value) <= 1.0):
                return False, "must be in [0.0, 1.0]"

        elif key == "ACTION_MODE":
            if value not in ("auto", "manual"):
                return False, "must be 'auto' or 'manual'"

        elif key == "FORCED_ACTION":
            if value not in ACTION_LIBRARY:
                return False, f"must be one of {sorted(ACTION_LIBRARY)}"

        elif key in _RANGE_KEYS:
            if not (isinstance(value, (list, tuple)) and len(value) == 2):
                return False, "must be a two-element list [min, max]"
            if float(value[0]) >= float(value[1]):
                return False, "first element must be less than second"

        elif key == "PUSH_INTERVAL":
            if int(value) < 1:
                return False, "must be >= 1"

    except (TypeError, ValueError) as exc:
        return False, f"type error: {exc}"

    return True, ""


# ═══════════════════════════════════════════════════════════════════
# 11. SocketIO push-loop
# ═══════════════════════════════════════════════════════════════════

_push_lock:   Lock = Lock()
_push_thread        = None

# Track the last emitted state to build minimal deltas
_emit_state: dict = {"last_snapshot": None, "last_action_sig": None}


def _action_signature(action: dict) -> tuple:
    if not action:
        return None
    return (
        action.get("scenario"),
        action.get("status"),
        action.get("priority"),
        round(float(action.get("confidence_score", 0.0)), 2),
        action.get("optimization_result"),
    )


def _build_metric_delta(snapshot: dict) -> dict:
    """Return only the fields that changed since the last emit.
    Core telemetry is always included; optional fields are omitted if
    unchanged, reducing payload size and preventing spurious re-renders."""
    prev       = _emit_state["last_snapshot"]
    action_sig = _action_signature(snapshot.get("action_center"))
    prev_sig   = _emit_state["last_action_sig"]

    delta = {
        "ts":      snapshot["ts"],
        "cpu":     snapshot["cpu"],
        "jitter":  snapshot["jitter"],
        "delay":   snapshot["delay"],
        "lambda_": snapshot["lambda_"],
        "qos":     snapshot["qos"],
        "wq":      snapshot["wq"],
        "status":  snapshot["status"],
    }

    if prev is None or prev.get("ims_total") != snapshot.get("ims_total"):
        delta["ims_total"] = snapshot["ims_total"]

    if prev is None or prev.get("service_sessions") != snapshot.get("service_sessions"):
        delta["service_sessions"] = snapshot["service_sessions"]

    if prev is None or action_sig != prev_sig:
        delta["action_center"] = snapshot["action_center"]
        _log("decision",
             f"{snapshot['action_center']['scenario']}  status={snapshot['status']}  "
             f"confidence={snapshot['action_center']['confidence_score']:.2f}")

    _emit_state["last_snapshot"]  = snapshot
    _emit_state["last_action_sig"] = action_sig
    return delta


def _push_loop() -> None:
    while True:
        try:
            snapshot = build_snapshot()
            insert_metric(snapshot)
            socketio.emit("metric_update", _build_metric_delta(snapshot))
        except Exception as exc:
            _log("push_loop", f"error: {exc}")
        socketio.sleep(CONFIG["PUSH_INTERVAL"])


def _ensure_push_loop() -> None:
    global _push_thread
    with _push_lock:
        if _push_thread is None:
            _push_thread = socketio.start_background_task(_push_loop)


# ═══════════════════════════════════════════════════════════════════
# Shared utility
# ═══════════════════════════════════════════════════════════════════

def _log(channel: str, message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] [{channel}] {message}", flush=True)


# ═══════════════════════════════════════════════════════════════════
# 12. Flask routes
# ═══════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    _ensure_push_loop()
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
    """
    GET  - return current config + list of available action keys.

    POST - update any subset of allowed keys independently.
           Each key is validated and normalised before being applied.
           Unknown / disallowed keys are reported in `ignored` so
           callers can detect typos without the whole request failing.

    Examples:
        # Set only chaos — no other keys needed
        POST /api/config
        {"SIMULATION_CHAOS": 0.5}

        # Set multiple keys at once
        POST /api/config
        {"SIMULATION_CHAOS": 0.8, "MU": 12.0, "PUSH_INTERVAL": 3}

        # Switch to manual mode
        POST /api/config
        {"ACTION_MODE": "manual", "FORCED_ACTION": "load_balance_secondary"}
    """
    if request.method == "POST":
        data = request.get_json(force=True) or {}
        applied  = {}
        rejected = {}
        ignored  = []

        for key, raw_value in data.items():
            if key not in _UPDATABLE_KEYS:
                ignored.append(key)
                continue

            ok, err = _validate_config_value(key, raw_value)
            if not ok:
                rejected[key] = err
                continue

            CONFIG[key] = _normalise_config_value(key, raw_value)
            applied[key] = CONFIG[key]

        _log("config",
             f"applied={sorted(applied)} rejected={sorted(rejected)} ignored={ignored}")

        return jsonify({
            "ok":                len(rejected) == 0,
            "applied":           applied,
            "rejected":          rejected,
            "ignored":           ignored,
            "available_actions": sorted(ACTION_LIBRARY.keys()),
            "config":            {k: CONFIG[k] for k in sorted(_UPDATABLE_KEYS)},
        })

    # GET
    return jsonify({
        "config":            {k: CONFIG[k] for k in sorted(_UPDATABLE_KEYS)},
        "available_actions": sorted(ACTION_LIBRARY.keys()),
    })


# ═══════════════════════════════════════════════════════════════════
# 13. SocketIO event handlers
# ═══════════════════════════════════════════════════════════════════

@socketio.on("connect")
def on_connect():
    _ensure_push_loop()
    _log("socket", f"client connected  sid={request.sid}")
    snapshot = build_snapshot()
    emit("bootstrap_data", {
        "snapshot": snapshot,
        "history":  fetch_history(30),
    })


@socketio.on("disconnect")
def on_disconnect():
    _log("socket", f"client disconnected  sid={request.sid}")


# ═══════════════════════════════════════════════════════════════════
# 14. Entry point
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    init_db()
    _load_csv()
    seed_runtime_state()
    _ensure_push_loop()
    _log("startup",
         f"GPON/IMS Monitor  ->  http://0.0.0.0:5000  "
         f"(push_interval={CONFIG['PUSH_INTERVAL']}s)")
    socketio.run(
        app,
        host="0.0.0.0",
        port=5000,
        debug=False,
        allow_unsafe_werkzeug=True,
    )