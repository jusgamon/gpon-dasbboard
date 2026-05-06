"""
GPON/IMS Network Monitoring Dashboard backend.
"""

import csv
import math
import os
import random
import sqlite3
import time
from collections import deque
from datetime import datetime, timezone
from threading import Lock

from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO, emit

app = Flask(__name__)
app.config["SECRET_KEY"] = "gpon-ims-poc-secret"
socketio = SocketIO(
    app,
    cors_allowed_origins="*",
    async_mode="threading",
    logger=False,
    engineio_logger=False,
    ping_interval=20,
    ping_timeout=60,
)

CONFIG = {
    "MU": 10.0,
    "PUSH_INTERVAL": 2,
    "DATA_SOURCE": "simulate",
    "CSV_PATH": "data.csv",
    "CPU_RANGE": (8, 92),
    "JITTER_RANGE": (2, 45),
    "DELAY_RANGE": (5, 80),
    "LAMBDA_RANGE": (1.5, 9.6),
    "IMS_SESSION_RANGE": (12, 84),
    "MAX_HISTORY": 200,
    "W_DELAY": 0.4,
    "W_JITTER": 0.3,
    "W_CPU": 0.3,
    "SERVICE_WEIGHTS": {"voip": 0.4, "video": 0.35, "web": 0.25},
    "SIMULATION_CHAOS": 0.22,
    "SIMULATION_MOMENTUM": 0.78,
    "VOIP_JITTER_THRESHOLD": 20,
    "VIDEO_DELAY_THRESHOLD": 28,
    "CPU_WARNING_THRESHOLD": 85,
    "LAMBDA_WARNING_RATIO": 0.82,
    "ACTION_MODE": "auto",
    "FORCED_ACTION": "observe",
    "FORCED_STATUS": "Normal",
}

ACTION_LIBRARY = {
    "observe": {
        "service": "All Services",
        "priority": "LOW",
        "patch": "No patch required. Keep baseline transport policy active.",
    },
    "rtp_priority_qos": {
        "service": "VoIP",
        "priority": "HIGH",
        "patch": "cli: qos policy update class VOIP set dscp ef queue strict-priority",
    },
    "increase_bandwidth_reservation": {
        "service": "Video",
        "priority": "MEDIUM",
        "patch": "config: ims.video.reservation=+15% and gpon.tcont.video.assured_bw=boost",
    },
    "load_balance_secondary": {
        "service": "Transport",
        "priority": "HIGH",
        "patch": "cli: orchestrator rebalance --target secondary-vnf --drain best-effort 20%",
    },
    "preemptive_shaping": {
        "service": "Transport",
        "priority": "HIGH",
        "patch": "cli: traffic-shaper apply profile preemptive_guard --window 30s",
    },
}

UPDATABLE_CONFIG = {
    "MU",
    "PUSH_INTERVAL",
    "DATA_SOURCE",
    "CSV_PATH",
    "CPU_RANGE",
    "JITTER_RANGE",
    "DELAY_RANGE",
    "LAMBDA_RANGE",
    "IMS_SESSION_RANGE",
    "W_DELAY",
    "W_JITTER",
    "W_CPU",
    "SERVICE_WEIGHTS",
    "SIMULATION_CHAOS",
    "SIMULATION_MOMENTUM",
    "VOIP_JITTER_THRESHOLD",
    "VIDEO_DELAY_THRESHOLD",
    "CPU_WARNING_THRESHOLD",
    "LAMBDA_WARNING_RATIO",
    "ACTION_MODE",
    "FORCED_ACTION",
    "FORCED_STATUS",
}

RANGE_KEYS = {
    "CPU_RANGE",
    "JITTER_RANGE",
    "DELAY_RANGE",
    "LAMBDA_RANGE",
    "IMS_SESSION_RANGE",
}

DB_PATH = "network_metrics.db"

_csv_reader_state = {"rows": [], "index": 0}
_push_thread = None
_push_lock = Lock()
_telemetry_window = deque(maxlen=120)
_sim_state = None
_decision_state = {
    "last_action": None,
    "last_result": "No action evaluated yet.",
}
_emit_state = {
    "last_snapshot": None,
    "last_action_signature": None,
}

NORMAL_BOOTSTRAP = {
    "cpu": 22.0,
    "jitter": 6.0,
    "delay": 12.0,
    "lambda_": 3.2,
    "ims_total": 24,
}


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


def log_event(channel: str, message: str):
    timestamp = datetime.now().strftime("%H:%M:%S")
    print(f"[{timestamp}] [{channel}] {message}", flush=True)


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


def fetch_latest_metric():
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        row = conn.execute(
            "SELECT * FROM metrics ORDER BY id DESC LIMIT 1"
        ).fetchone()
    return dict(row) if row else None


def action_signature(action_center: dict | None):
    if not action_center:
        return None
    return (
        action_center.get("scenario"),
        action_center.get("status"),
        action_center.get("priority"),
        round(float(action_center.get("confidence_score", 0.0)), 2),
        action_center.get("optimization_result"),
    )


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


def clamp(value: float, bounds: tuple[float, float]) -> float:
    return max(bounds[0], min(bounds[1], value))


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


def parse_ts_epoch(ts_value: str) -> float:
    try:
        return datetime.fromisoformat(ts_value.replace("Z", "+00:00")).timestamp()
    except Exception:
        return time.time()


def seed_runtime_state():
    """Use DB history for continuity while bootstrapping the live simulation in a healthy state."""
    global _sim_state

    history = fetch_history(12)
    for row in history:
        _telemetry_window.append({
            "ts_epoch": parse_ts_epoch(row["ts"]),
            "cpu": float(row["cpu"]),
            "jitter": float(row["jitter"]),
            "delay": float(row["delay"]),
            "lambda_": float(row["lambda_"]),
        })

    latest = history[-1] if history else None
    if latest is None:
        _sim_state = {
            **NORMAL_BOOTSTRAP,
            "lambda_velocity": 0.0,
            "shock": 0.0,
            "tick": 0,
            "warmup": 8,
        }
        return

    blended = {
        "cpu": latest["cpu"] * 0.25 + NORMAL_BOOTSTRAP["cpu"] * 0.75,
        "jitter": latest["jitter"] * 0.20 + NORMAL_BOOTSTRAP["jitter"] * 0.80,
        "delay": latest["delay"] * 0.20 + NORMAL_BOOTSTRAP["delay"] * 0.80,
        "lambda_": latest["lambda_"] * 0.35 + NORMAL_BOOTSTRAP["lambda_"] * 0.65,
        "ims_total": int(round(latest.get("ims_total", NORMAL_BOOTSTRAP["ims_total"]) * 0.3 + NORMAL_BOOTSTRAP["ims_total"] * 0.7)),
    }

    bootstrap_qos = calculate_qos(blended["delay"], blended["jitter"], blended["cpu"])
    if bootstrap_qos < 82:
        blended["cpu"] = min(blended["cpu"], 28.0)
        blended["jitter"] = min(blended["jitter"], 8.0)
        blended["delay"] = min(blended["delay"], 14.0)

    _sim_state = {
        "cpu": round(clamp(blended["cpu"], CONFIG["CPU_RANGE"]), 2),
        "jitter": round(clamp(blended["jitter"], CONFIG["JITTER_RANGE"]), 2),
        "delay": round(clamp(blended["delay"], CONFIG["DELAY_RANGE"]), 2),
        "lambda_": round(clamp(blended["lambda_"], CONFIG["LAMBDA_RANGE"]), 2),
        "ims_total": int(clamp(blended["ims_total"], CONFIG["IMS_SESSION_RANGE"])),
        "lambda_velocity": 0.0,
        "shock": 0.0,
        "tick": 0,
        "warmup": 8,
    }
    log_event(
        "startup",
        f"seeded simulation cpu={_sim_state['cpu']:.1f} jitter={_sim_state['jitter']:.1f} "
        f"delay={_sim_state['delay']:.1f} lambda={_sim_state['lambda_']:.2f}"
    )


def init_sim_state():
    midpoint = lambda bounds: (bounds[0] + bounds[1]) / 2.0
    return {
        "cpu": NORMAL_BOOTSTRAP["cpu"],
        "jitter": NORMAL_BOOTSTRAP["jitter"],
        "delay": NORMAL_BOOTSTRAP["delay"],
        "lambda_": NORMAL_BOOTSTRAP["lambda_"],
        "ims_total": NORMAL_BOOTSTRAP["ims_total"],
        "lambda_velocity": 0.0,
        "shock": 0.0,
        "tick": 0,
        "warmup": 8,
    }


def simulate_metrics() -> dict:
    """Generate smoother, more realistic telemetry with configurable chaos."""
    global _sim_state
    if _sim_state is None:
        _sim_state = init_sim_state()

    chaos = max(0.0, min(1.5, float(CONFIG["SIMULATION_CHAOS"])))
    momentum = max(0.0, min(0.97, float(CONFIG["SIMULATION_MOMENTUM"])))

    state = _sim_state
    state["tick"] += 1
    warmup = max(0, int(state.get("warmup", 0)))

    if random.random() < 0.08 + chaos * 0.08:
        state["shock"] = random.uniform(-1.0, 1.0) * (0.18 + chaos * 0.95)
    else:
        state["shock"] *= 0.72

    if warmup > 0:
        chaos *= 0.35
        state["shock"] *= 0.35

    lambda_center = (CONFIG["LAMBDA_RANGE"][0] + CONFIG["LAMBDA_RANGE"][1]) / 2.0
    lambda_drift = (lambda_center - state["lambda_"]) * 0.08
    lambda_noise = random.uniform(-0.28, 0.28) * (0.25 + chaos)
    state["lambda_velocity"] = (
        state["lambda_velocity"] * momentum
        + lambda_drift * 0.4
        + lambda_noise
        + state["shock"] * 0.22
    )
    state["lambda_"] = clamp(
        state["lambda_"] + state["lambda_velocity"],
        CONFIG["LAMBDA_RANGE"],
    )

    lambda_ratio = state["lambda_"] / max(CONFIG["MU"], 0.001)
    base_sessions = (
        CONFIG["IMS_SESSION_RANGE"][0]
        + (CONFIG["IMS_SESSION_RANGE"][1] - CONFIG["IMS_SESSION_RANGE"][0]) * min(1.0, lambda_ratio)
    )
    session_noise = random.uniform(-2.5, 2.5) * (0.5 + chaos)
    state["ims_total"] = int(round(clamp(
        state["ims_total"] * 0.55 + base_sessions * 0.45 + session_noise,
        CONFIG["IMS_SESSION_RANGE"],
    )))

    cpu_target = 16 + lambda_ratio * 58 + state["ims_total"] * 0.12 + abs(state["shock"]) * 16
    cpu_noise = random.uniform(-2.2, 2.2) * (0.55 + chaos)
    state["cpu"] = clamp(
        state["cpu"] * 0.72 + cpu_target * 0.28 + cpu_noise,
        CONFIG["CPU_RANGE"],
    )

    jitter_target = 4 + lambda_ratio * 10 + max(0.0, state["cpu"] - 55) * 0.12 + abs(state["shock"]) * 7
    jitter_noise = random.uniform(-1.3, 1.3) * (0.45 + chaos)
    state["jitter"] = clamp(
        state["jitter"] * 0.62 + jitter_target * 0.38 + jitter_noise,
        CONFIG["JITTER_RANGE"],
    )

    delay_target = 8 + lambda_ratio * 18 + state["jitter"] * 0.85 + max(0.0, state["cpu"] - 60) * 0.16
    delay_noise = random.uniform(-1.8, 1.8) * (0.45 + chaos)
    state["delay"] = clamp(
        state["delay"] * 0.66 + delay_target * 0.34 + delay_noise,
        CONFIG["DELAY_RANGE"],
    )

    if warmup > 0:
        state["lambda_"] = min(state["lambda_"], 4.2)
        state["cpu"] = min(state["cpu"], 31.0)
        state["jitter"] = min(state["jitter"], 8.5)
        state["delay"] = min(state["delay"], 16.0)
        state["warmup"] = warmup - 1

    return {
        "cpu": round(state["cpu"], 2),
        "jitter": round(state["jitter"], 2),
        "delay": round(state["delay"], 2),
        "lambda_": round(state["lambda_"], 2),
        "ims_total": int(state["ims_total"]),
    }


def get_raw_metrics() -> dict:
    """Return a raw metric sample from the configured data source."""
    if CONFIG["DATA_SOURCE"] == "csv":
        if not _csv_reader_state["rows"]:
            _load_csv()
        row = _next_csv_row()
        if row:
            return row
    return simulate_metrics()


def add_window_sample(sample: dict):
    _telemetry_window.append(sample)


def interpolate_metric(seconds_ago: float, key: str, current_ts: float, current_value: float) -> float:
    target_ts = current_ts - seconds_ago
    if not _telemetry_window:
        return current_value

    previous = None
    for point in reversed(_telemetry_window):
        if point["ts_epoch"] <= target_ts:
            return float(point[key])
        previous = point

    return float(_telemetry_window[0][key]) if previous is not None else current_value


def compute_gradients(raw: dict, current_ts: float) -> dict:
    window_seconds = 10.0
    gradients = {}
    for key in ("jitter", "delay", "cpu", "lambda_"):
        past_value = interpolate_metric(window_seconds, key, current_ts, float(raw[key]))
        gradients[key] = round((float(raw[key]) - past_value) / window_seconds, 4)
    return gradients


def compute_service_health(raw: dict) -> dict:
    shi_voip = max(0.0, min(100.0, 100 - (raw["jitter"] * 2.5 + raw["delay"] * 0.5)))
    shi_video = max(0.0, min(100.0, 100 - (raw["delay"] * 1.5 + raw["cpu"] * 0.2)))
    return {"voip": round(shi_voip, 2), "video": round(shi_video, 2)}


def projected_queue_metrics(raw: dict, gradients: dict) -> dict:
    projected_lambda = max(0.01, raw["lambda_"] + gradients["lambda_"] * 30.0)
    projected_wait = calculate_mm1_wait(projected_lambda)
    if gradients["lambda_"] > 0 and raw["lambda_"] < CONFIG["MU"]:
        time_to_saturation = max(0.0, (CONFIG["MU"] - raw["lambda_"]) / gradients["lambda_"])
    else:
        time_to_saturation = float("inf")

    return {
        "projected_lambda_30s": round(projected_lambda, 3),
        "projected_wq_30s": projected_wait,
        "projected_wq_30s_ms": round(projected_wait * 1000, 2) if math.isfinite(projected_wait) else float("inf"),
        "time_to_saturation_s": round(time_to_saturation, 2) if math.isfinite(time_to_saturation) else float("inf"),
    }


def severity_rank(status: str) -> int:
    return {"Normal": 0, "Warning": 1, "Critical": 2}.get(status, 0)


def action_status_from_score(score: float) -> str:
    if score >= 2.25:
        return "Critical"
    if score >= 0.9:
        return "Warning"
    return "Normal"


def build_candidate_scores(raw: dict, sessions: dict, gradients: dict, shi: dict, queue_projection: dict) -> dict:
    lambda_ratio = raw["lambda_"] / max(CONFIG["MU"], 0.001)
    scores = {"observe": 0.15}

    voip_score = 0.0
    if sessions["voip"] > 0:
        load_factor = sessions["voip"] / max(sessions["voip"] + sessions["video"] + sessions["web"], 1)
        voip_score = (
            max(0.0, gradients["jitter"])
            + max(0.0, (CONFIG["VOIP_JITTER_THRESHOLD"] - shi["voip"]) / 12.0)
            + max(0.0, (raw["jitter"] - CONFIG["VOIP_JITTER_THRESHOLD"]) / 6.0)
            + load_factor * 1.2
        )
    scores["rtp_priority_qos"] = round(voip_score, 4)

    video_score = 0.0
    if sessions["video"] > 0:
        load_factor = sessions["video"] / max(sessions["voip"] + sessions["video"] + sessions["web"], 1)
        video_score = (
            max(0.0, gradients["delay"])
            + max(0.0, (CONFIG["VIDEO_DELAY_THRESHOLD"] - shi["video"]) / 16.0)
            + max(0.0, (raw["delay"] - CONFIG["VIDEO_DELAY_THRESHOLD"]) / 9.0)
            + max(0.0, (raw["cpu"] - 70.0) / 18.0)
            + load_factor
        )
    scores["increase_bandwidth_reservation"] = round(video_score, 4)

    transport_score = (
        max(0.0, lambda_ratio - CONFIG["LAMBDA_WARNING_RATIO"]) * 5.5
        + max(0.0, gradients["lambda_"]) * 1.3
        + max(0.0, (raw["cpu"] - CONFIG["CPU_WARNING_THRESHOLD"]) / 6.0)
        + (2.2 if queue_projection["time_to_saturation_s"] <= 30 else 0.0)
    )
    scores["load_balance_secondary"] = round(transport_score, 4)

    preemptive_score = (
        max(0.0, gradients["lambda_"]) * 1.5
        + max(0.0, gradients["delay"]) * 0.8
        + (3.0 if queue_projection["projected_wq_30s_ms"] > 200 else 0.0)
        + (1.5 if queue_projection["time_to_saturation_s"] <= 30 else 0.0)
    )
    scores["preemptive_shaping"] = round(preemptive_score, 4)
    return scores


def build_decision_object(
    action_key: str,
    status: str,
    mode: str,
    raw: dict,
    sessions: dict,
    gradients: dict,
    shi: dict,
    queue_projection: dict,
    confidence: float,
    optimization_result: str,
) -> dict:
    template = ACTION_LIBRARY[action_key]
    voip_load = sessions["voip"]
    video_load = sessions["video"]
    total_sessions = sessions["voip"] + sessions["video"] + sessions["web"]
    lambda_ratio = raw["lambda_"] / max(CONFIG["MU"], 0.001)

    if action_key == "rtp_priority_qos":
        diagnosis = "IMS VoIP sessions are suffering from rising jitter under active voice load."
    elif action_key == "increase_bandwidth_reservation":
        diagnosis = "IMS video sessions are degrading due to delay growth coupled with CPU pressure."
    elif action_key == "load_balance_secondary":
        diagnosis = "Transport stress is building as traffic intensity approaches service capacity."
    elif action_key == "preemptive_shaping":
        diagnosis = "Queue growth is projecting toward saturation before hard limits are reached."
    else:
        diagnosis = "No immediate service degradation is dominating the current telemetry window."

    rationale_sentences = [
        f"Jitter is changing by {gradients['jitter']:.2f} ms per second, delay by {gradients['delay']:.2f} ms per second, and CPU by {gradients['cpu']:.2f} percent per second over the last 10 seconds.",
        f"The VoIP Service Health Index is {shi['voip']:.1f} and the Video Service Health Index is {shi['video']:.1f}.",
        f"Traffic intensity is running at lambda over mu = {lambda_ratio:.2f}.",
    ]
    if math.isfinite(queue_projection["projected_wq_30s_ms"]):
        rationale_sentences.append(
            f"The projected queue wait in 30 seconds is {queue_projection['projected_wq_30s_ms']:.1f} ms."
        )
    if math.isfinite(queue_projection["time_to_saturation_s"]):
        rationale_sentences.append(
            f"At the current gradient, time to saturation is approximately {queue_projection['time_to_saturation_s']:.1f} seconds."
        )

    return {
        "mode": mode,
        "scenario": action_key,
        "status": status,
        "service": template["service"],
        "priority": template["priority"],
        "diagnosis": diagnosis,
        "rationale": " ".join(rationale_sentences),
        "proposed_patch": template["patch"],
        "confidence_score": round(confidence, 2),
        "optimization_result": optimization_result,
        "analysis": f"Total IMS sessions={total_sessions}, VoIP={voip_load}, Video={video_load}.",
        "decision": diagnosis,
        "optimization": template["patch"],
    }


def assess_closed_loop(action_key: str, raw: dict, gradients: dict, shi: dict, queue_projection: dict) -> str:
    last_action = _decision_state["last_action"]
    if not last_action or last_action["scenario"] != action_key:
        return _decision_state["last_result"]

    elapsed = time.time() - last_action["applied_at"]
    if elapsed < 6:
        return "Action applied; waiting for post-change telemetry."

    baseline = last_action["baseline"]
    improved = 0
    checks = 0

    if action_key == "rtp_priority_qos":
        checks += 2
        improved += raw["jitter"] <= baseline["jitter"]
        improved += shi["voip"] >= baseline["shi_voip"]
    elif action_key == "increase_bandwidth_reservation":
        checks += 2
        improved += raw["delay"] <= baseline["delay"]
        improved += shi["video"] >= baseline["shi_video"]
    else:
        checks += 2
        improved += raw["lambda_"] <= baseline["lambda_"]
        improved += queue_projection["projected_wq_30s_ms"] <= baseline["projected_wq_30s_ms"]

    result = "Optimization Successful" if improved >= max(1, checks - 1) else "Action Ineffective; Escalating."
    _decision_state["last_result"] = result
    return result


def apply_hysteresis(candidate_key: str, candidate_status: str) -> tuple[str, str]:
    last_action = _decision_state["last_action"]
    if not last_action:
        return candidate_key, candidate_status

    elapsed = time.time() - last_action["applied_at"]
    if elapsed > 15:
        return candidate_key, candidate_status

    if candidate_key == last_action["scenario"]:
        return candidate_key, candidate_status

    if severity_rank(candidate_status) < severity_rank("Critical"):
        return last_action["scenario"], last_action["status"]

    return candidate_key, candidate_status


def resolve_action(raw: dict, sessions: dict, gradients: dict, shi: dict, queue_projection: dict, current_status: str) -> dict:
    mode = CONFIG["ACTION_MODE"]
    optimization_result = _decision_state["last_result"]

    if mode == "manual":
        action_key = CONFIG["FORCED_ACTION"]
        status = CONFIG["FORCED_STATUS"]
        return build_decision_object(
            action_key,
            status,
            "manual",
            raw,
            sessions,
            gradients,
            shi,
            queue_projection,
            confidence=0.95,
            optimization_result=optimization_result,
        )

    scores = build_candidate_scores(raw, sessions, gradients, shi, queue_projection)
    action_key, top_score = max(scores.items(), key=lambda item: item[1])
    if action_key != "observe" and top_score < 0.75:
        action_key = "observe"
        top_score = scores["observe"]
    confidence = min(0.99, 0.35 + top_score / 5.0)
    status = action_status_from_score(top_score)

    if action_key == "observe":
        status = current_status if current_status != "Normal" and top_score > 0.5 else "Normal"

    action_key, status = apply_hysteresis(action_key, status)
    optimization_result = assess_closed_loop(action_key, raw, gradients, shi, queue_projection)

    decision = build_decision_object(
        action_key,
        status,
        "auto",
        raw,
        sessions,
        gradients,
        shi,
        queue_projection,
        confidence=confidence,
        optimization_result=optimization_result,
    )

    if action_key != "observe":
        last_action = _decision_state["last_action"]
        if last_action is None or last_action["scenario"] != action_key:
            _decision_state["last_action"] = {
                "scenario": action_key,
                "status": status,
                "applied_at": time.time(),
                "baseline": {
                    "jitter": raw["jitter"],
                    "delay": raw["delay"],
                    "lambda_": raw["lambda_"],
                    "shi_voip": shi["voip"],
                    "shi_video": shi["video"],
                    "projected_wq_30s_ms": queue_projection["projected_wq_30s_ms"],
                },
            }
        else:
            last_action["status"] = status

    return decision


def build_snapshot() -> dict:
    raw = get_raw_metrics()
    now_epoch = time.time()
    ims_total = int(raw.get("ims_total") or random.randint(*CONFIG["IMS_SESSION_RANGE"]))
    sessions = split_sessions(ims_total)

    add_window_sample({
        "ts_epoch": now_epoch,
        "cpu": raw["cpu"],
        "jitter": raw["jitter"],
        "delay": raw["delay"],
        "lambda_": raw["lambda_"],
    })

    gradients = compute_gradients(raw, now_epoch)
    shi = compute_service_health(raw)
    queue_projection = projected_queue_metrics(raw, gradients)

    qos = calculate_qos(raw["delay"], raw["jitter"], raw["cpu"])
    wait = calculate_mm1_wait(raw["lambda_"])
    status = classify_status(qos)

    decision = resolve_action(raw, sessions, gradients, shi, queue_projection, status)
    if severity_rank(decision["status"]) > severity_rank(status):
        status = decision["status"]

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
        "analytics": {
            "gradients": gradients,
            "shi": shi,
            "queue_projection": queue_projection,
        },
        "action_center": {**decision, "status": status},
        "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }


def normalize_config_value(key: str, value):
    if key in RANGE_KEYS and isinstance(value, list):
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


def build_bootstrap_payload():
    snapshot = build_snapshot()
    return {
        "snapshot": snapshot,
        "history": fetch_history(30),
    }


def build_metric_delta(snapshot: dict):
    previous = _emit_state["last_snapshot"]
    action_sig = action_signature(snapshot.get("action_center"))
    previous_sig = _emit_state["last_action_signature"]

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

    if previous is None or previous.get("ims_total") != snapshot.get("ims_total"):
        delta["ims_total"] = snapshot["ims_total"]

    if previous is None or previous.get("service_sessions") != snapshot.get("service_sessions"):
        delta["service_sessions"] = snapshot["service_sessions"]

    if previous is None or action_sig != previous_sig:
        delta["action_center"] = snapshot["action_center"]
        log_event(
            "decision",
            f"{snapshot['action_center']['scenario']} status={snapshot['status']} "
            f"confidence={snapshot['action_center']['confidence_score']:.2f}"
        )

    _emit_state["last_snapshot"] = snapshot
    _emit_state["last_action_signature"] = action_sig
    return delta


def _push_loop():
    while True:
        try:
            snapshot = build_snapshot()
            insert_metric(snapshot)
            socketio.emit("metric_update", build_metric_delta(snapshot))
        except Exception as exc:
            log_event("push_loop", f"error: {exc}")
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
    if request.method == "POST":
        data = request.get_json(force=True)
        for key, value in data.items():
            if key in UPDATABLE_CONFIG:
                CONFIG[key] = normalize_config_value(key, value)
        log_event("config", f"updated keys={sorted(data.keys())}")
        return jsonify({
            "ok": True,
            "config": {key: CONFIG[key] for key in sorted(UPDATABLE_CONFIG)},
            "available_actions": sorted(ACTION_LIBRARY.keys()),
        })

    return jsonify({
        "config": {key: CONFIG[key] for key in sorted(UPDATABLE_CONFIG)},
        "available_actions": sorted(ACTION_LIBRARY.keys()),
    })


@socketio.on("connect")
def on_connect():
    ensure_push_loop()
    log_event("socket", f"client connected sid={request.sid}")
    emit("bootstrap_data", build_bootstrap_payload())


@socketio.on("disconnect")
def on_disconnect():
    log_event("socket", f"client disconnected sid={request.sid}")


if __name__ == "__main__":
    init_db()
    _load_csv()
    seed_runtime_state()
    ensure_push_loop()
    log_event("startup", f"GPON/IMS Monitor running on http://0.0.0.0:5000 with push_interval={CONFIG['PUSH_INTERVAL']}s")
    socketio.run(
        app,
        host="0.0.0.0",
        port=5000,
        debug=False,
        allow_unsafe_werkzeug=True,
    )
