# ═══════════════════════════════════════════════════════════════════
# 1.  Imports & application bootstrap
# ═══════════════════════════════════════════════════════════════════

import csv
import math
import os
import random
import sqlite3
import time
from collections import deque
from datetime import datetime, timezone
from threading import Lock, Thread

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
    ping_interval=25,
    ping_timeout=120,
)

# ═══════════════════════════════════════════════════════════════════
# 2.  Configuration
# ═══════════════════════════════════════════════════════════════════

CONFIG: dict = {
    # M/M/1 service rate (requests/s — conceptual unit matching lambda)
    "MU": 10.0,

    # Seconds between SocketIO push emissions
    "PUSH_INTERVAL": 2,

    # "simulate" uses the built-in physics engine; "csv" replays data.csv
    "DATA_SOURCE": "simulate",
    "CSV_PATH": "data.csv",

    # Physical ranges for the simulation (min, max)
    "CPU_RANGE":         (8.0,  92.0),
    "JITTER_RANGE":      (2.0,  45.0),
    "DELAY_RANGE":       (5.0,  80.0),
    "LAMBDA_RANGE":      (0.5,   7.5),   # max λ/μ = 0.75 → healthy ceiling
    "IMS_SESSION_RANGE": (8,    60),

    # QoS formula:  score = 100 − (W_DELAY·d + W_JITTER·j + W_CPU·c)
    "W_DELAY":  0.4,
    "W_JITTER": 0.3,
    "W_CPU":    0.3,

    # IMS service-mix proportions (normalised before use)
    "SERVICE_WEIGHTS": {"voip": 0.40, "video": 0.35, "web": 0.25},

    # CHAOS    – shock magnitude [0 = smooth, 1.5 = stress-test]
    # MOMENTUM – velocity carry-over [0 = memoryless, 0.97 = very sticky]
    "SIMULATION_CHAOS":    0.22,
    "SIMULATION_MOMENTUM": 0.78,

    # Per-service degradation thresholds used by the scoring functions
    "VOIP_JITTER_THRESHOLD":  20.0,   # ms
    "VIDEO_DELAY_THRESHOLD":  28.0,   # ms
    "CPU_WARNING_THRESHOLD":  85.0,   # %
    "LAMBDA_WARNING_RATIO":   0.75,   # λ/μ

    # "auto" runs the scoring engine; "manual" pins to FORCED_ACTION/STATUS
    "ACTION_MODE":   "auto",
    "FORCED_ACTION": "observe",
    "FORCED_STATUS": "Normal",
}

# Keys accepted by POST /api/config
_UPDATABLE_KEYS = frozenset(CONFIG.keys())

# Range keys arrive as JSON arrays → must be converted to tuples
_RANGE_KEYS = frozenset({
    "CPU_RANGE", "JITTER_RANGE", "DELAY_RANGE",
    "LAMBDA_RANGE", "IMS_SESSION_RANGE",
})

# Warm-boot target — simulation starts here and slowly drifts
# These values match the mean of the supplied CSV data set.
_BOOTSTRAP = {
    "cpu":      23.0,
    "jitter":    5.7,
    "delay":    11.5,
    "lambda_":   2.5,
    "ims_total": 18,
}

DB_PATH = "network_metrics.db"


# ═══════════════════════════════════════════════════════════════════
# 3.  Action registry
# ═══════════════════════════════════════════════════════════════════

def _score_observe(raw, sessions, gradients, shi, qp, cfg):
    return 0.15   # always a weak baseline candidate


def _score_rtp_priority(raw, sessions, gradients, shi, qp, cfg):
    """Fires when VoIP sessions exist and jitter is rising or already high."""
    if sessions["voip"] == 0:
        return 0.0
    total      = max(sum(sessions.values()), 1)
    voip_load  = sessions["voip"] / total
    # Stress factor: 0 when SHI is healthy (≥85), rises as SHI degrades
    shi_stress = max(0.0, (85.0 - shi["voip"]) / 85.0)
    return (
        max(0.0, gradients["jitter"])
        + max(0.0, (cfg["VOIP_JITTER_THRESHOLD"] - shi["voip"]) / 12.0)
        + max(0.0, (raw["jitter"] - cfg["VOIP_JITTER_THRESHOLD"]) / 6.0)
        + voip_load * shi_stress * 1.8
    )


def _score_bandwidth_reservation(raw, sessions, gradients, shi, qp, cfg):
    """Fires when video sessions exist and delay or CPU pressure is building."""
    if sessions["video"] == 0:
        return 0.0
    total      = max(sum(sessions.values()), 1)
    video_load = sessions["video"] / total
    shi_stress = max(0.0, (85.0 - shi["video"]) / 85.0)
    return (
        max(0.0, gradients["delay"])
        + max(0.0, (cfg["VIDEO_DELAY_THRESHOLD"] - shi["video"]) / 16.0)
        + max(0.0, (raw["delay"] - cfg["VIDEO_DELAY_THRESHOLD"]) / 9.0)
        + max(0.0, (raw["cpu"] - 70.0) / 18.0)
        + video_load * shi_stress * 1.5
    )


def _score_load_balance(raw, sessions, gradients, shi, qp, cfg):
    """Fires when λ/μ or CPU are approaching their warning thresholds."""
    lambda_ratio = raw["lambda_"] / max(cfg["MU"], 0.001)
    return (
        max(0.0, lambda_ratio - cfg["LAMBDA_WARNING_RATIO"]) * 5.5
        + max(0.0, gradients["lambda_"]) * 1.3
        + max(0.0, (raw["cpu"] - cfg["CPU_WARNING_THRESHOLD"]) / 6.0)
        + (2.2 if qp["time_to_saturation_s"] <= 20 else 0.0)
    )


def _score_preemptive_shaping(raw, sessions, gradients, shi, qp, cfg):
    """Fires when the queue is trending toward saturation.
    Uses a graduated load bonus (rises linearly above 60 % utilisation)
    instead of a flat threshold on raw Wq ms, which fired constantly."""
    lambda_ratio = raw["lambda_"] / max(cfg["MU"], 0.001)
    load_bonus   = max(0.0, lambda_ratio - 0.60) * 4.0   # 0 below 60 %
    return (
        max(0.0, gradients["lambda_"]) * 1.5
        + max(0.0, gradients["delay"]) * 0.8
        + load_bonus
        + (1.5 if qp["time_to_saturation_s"] <= 30 else 0.0)
    )

def _score_cpu_shedding(raw, sessions, gradients, shi, qp, cfg):
    """
    Fires when CPU pressure is persistently high and rising.
    Simulates shedding low-priority/background traffic.
    """
    cpu_pressure = max(0.0, (raw["cpu"] - cfg["CPU_WARNING_THRESHOLD"]) / 10.0)

    return (
        cpu_pressure * 2.2
        + max(0.0, gradients["cpu"]) * 1.5
        + max(0.0, gradients["lambda_"]) * 0.8
        + (1.5 if raw["cpu"] >= 92.0 else 0.0)
    )


def _score_session_guard(raw, sessions, gradients, shi, qp, cfg):
    """
    Fires when IMS session count becomes abnormally high
    relative to current transport conditions.
    """
    max_sessions = max(cfg["IMS_SESSION_RANGE"][1], 1)

    load_factor = raw["ims_total"] / max_sessions
    lambda_ratio = raw["lambda_"] / max(cfg["MU"], 0.001)

    return (
        max(0.0, load_factor - 0.70) * 4.0
        + max(0.0, lambda_ratio - 0.65) * 3.5
        + max(0.0, gradients["lambda_"]) * 1.2
        + (1.8 if qp["time_to_saturation_s"] <= 25 else 0.0)
    )


def _score_transport_retransmission(raw, sessions, gradients, shi, qp, cfg):
    """
    Fires when delay rises faster than jitter,
    indicating possible transport inefficiency or congestion.
    """
    delay_pressure = max(
        0.0,
        (raw["delay"] - cfg["VIDEO_DELAY_THRESHOLD"]) / 8.0
    )

    delay_dominance = max(
        0.0,
        gradients["delay"] - gradients["jitter"]
    )

    return (
        delay_pressure * 1.8
        + delay_dominance * 2.5
        + max(0.0, gradients["lambda_"]) * 0.9
        + (1.2 if raw["delay"] >= 50.0 else 0.0)
    )


# Registry — the single source of truth for all actions.
# Keys become scenario identifiers throughout the rest of the code.
ACTION_REGISTRY: dict = {
    "observe": {
        "service":   "Bütün xidmətlər",
        "priority":  "AŞAĞI",
        "patch":     "Həll tələb olunmur. Əsas nəqliyyat siyasətini aktiv saxlayın.",
        "diagnosis": "Cari telemetriya intervalında dominant xidmət problemi müşahidə olunmur.",
        "score_fn":  _score_observe,
    },
    "rtp_priority_qos": {
        "service":   "VoIP",
        "priority":  "YÜKSƏK",
        "patch":     "cli: qos policy update class VOIP set dscp ef queue strict-priority",
        "diagnosis": "Aktiv səs yükü zamanı IMS VoIP sessiyalarında artan jitter problemi müşahidə olunur.",
        "score_fn":  _score_rtp_priority,
    },
    "increase_bandwidth_reservation": {
        "service":   "Video",
        "priority":  "ORTA",
        "patch":     "config: ims.video.reservation=+15% and gpon.tcont.video.assured_bw=boost",
        "diagnosis": "CPU yüklənməsi və gecikmə artımı səbəbindən IMS video sessiyalarının keyfiyyəti zəifləyir.",
        "score_fn":  _score_bandwidth_reservation,
    },
    "load_balance_secondary": {
        "service":   "Nəqliyyat",
        "priority":  "YÜKSƏK",
        "patch":     "cli: orchestrator rebalance --target secondary-vnf --drain best-effort 20%",
        "diagnosis": "Trafik intensivliyi xidmət tutumuna yaxınlaşdığı üçün nəqliyyat yükü artır.",
        "score_fn":  _score_load_balance,
    },
    "preemptive_shaping": {
        "service":   "Nəqliyyat",
        "priority":  "YÜKSƏK",
        "patch":     "cli: traffic-shaper apply profile preemptive_guard --window 30s",
        "diagnosis": "Növbə artımı kritik limitlərə çatmadan əvvəl doyma vəziyyətinə yaxınlaşır.",
        "score_fn":  _score_preemptive_shaping,
    },
    "cpu_traffic_shedding": {
        "service":   "Compute",
        "priority":  "YÜKSƏK",
        "patch":     "cli: traffic-policy apply low-priority-shedding --threshold cpu>85",
        "diagnosis": "CPU resursları kritik həddə yaxınlaşdığı üçün aşağı prioritet trafik məhdudlaşdırılır.",
        "score_fn":  _score_cpu_shedding,
    },
    "ims_session_guard": {
        "service":   "IMS Core",
        "priority":  "ORTA",
        "patch":     "cli: ims session-guard enable --limit adaptive",
        "diagnosis": "IMS sessiya sayı nəqliyyat və xidmət tutumuna yaxınlaşır.",
        "score_fn":  _score_session_guard,
    },

    "transport_retransmission_optimization": {
        "service":   "Transport",
        "priority":  "ORTA",
        "patch":     "cli: transport optimize retransmission-window --adaptive",
        "diagnosis": "Artan gecikmə nəqliyyat səviyyəsində retransmissiya və congestion problemlərinə işarə edir.",
        "score_fn":  _score_transport_retransmission,
    },
}


# ═══════════════════════════════════════════════════════════════════
# 4.  Database helpers
# ═══════════════════════════════════════════════════════════════════

def init_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS metrics (
                id      INTEGER PRIMARY KEY AUTOINCREMENT,
                ts      TEXT NOT NULL,
                cpu     REAL NOT NULL,
                jitter  REAL NOT NULL,
                delay   REAL NOT NULL,
                lambda_ REAL NOT NULL,
                qos     REAL NOT NULL,
                wq      REAL NOT NULL,
                status  TEXT NOT NULL
            )
        """)
        conn.commit()


def insert_metric(snapshot: dict) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("""
            INSERT INTO metrics (ts, cpu, jitter, delay, lambda_, qos, wq, status)
            VALUES (:ts, :cpu, :jitter, :delay, :lambda_, :qos, :wq, :status)
        """, {k: snapshot[k] for k in ("ts", "cpu", "jitter", "delay", "lambda_", "qos", "wq", "status")})
        conn.commit()


def fetch_history(limit: int = 60) -> list:
    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(
            "SELECT * FROM metrics ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
    return [dict(r) for r in reversed(rows)]


# ═══════════════════════════════════════════════════════════════════
# 5.  QoS & M/M/1 models
# ═══════════════════════════════════════════════════════════════════

def calculate_qos(delay: float, jitter: float, cpu: float) -> float:
    """QoS = 100 − (W_d·delay + W_j·jitter + W_c·cpu), clamped to [0, 100]."""
    raw = 100.0 - CONFIG["W_DELAY"] * delay - CONFIG["W_JITTER"] * jitter - CONFIG["W_CPU"] * cpu
    return round(max(0.0, min(100.0, raw)), 2)


def calculate_mm1_wait(lambda_: float, mu: float = None) -> float:
    """M/M/1 mean waiting time: W = 1/(μ − λ).  Returns inf when λ ≥ μ."""
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


def severity_rank(status: str) -> int:
    return {"Normal": 0, "Warning": 1, "Critical": 2}.get(status, 0)


def mm1_curve(mu: float = None, steps: int = 50) -> list:
    """(λ, W) pairs from λ = 0 up to 0.99·μ — used by the M/M/1 chart."""
    mu = mu if mu is not None else CONFIG["MU"]
    result = []
    for i in range(1, steps + 1):
        lam  = round(mu * i / (steps + 1), 4)
        wait = calculate_mm1_wait(lam, mu)
        result.append({"lambda": lam, "W": wait if math.isfinite(wait) else None})
    return result


# ═══════════════════════════════════════════════════════════════════
# 6.  Simulation engine
# ═══════════════════════════════════════════════════════════════════

_sim_state: dict = None   # initialised by seed_runtime_state()


def _clamp(value: float, bounds: tuple) -> float:
    return max(bounds[0], min(bounds[1], value))


def _build_initial_sim_state(seed: dict = None) -> dict:
    """Return a sim state dict, optionally pre-seeded from historical data."""
    base = seed if seed else _BOOTSTRAP.copy()
    return {
        **base,
        "lambda_velocity": 0.0,
        "shock":           0.0,
        "tick":            0,
        "warmup":          8,   # first 8 ticks are capped to healthy values
    }


def seed_runtime_state() -> None:
    """Warm the simulation from the last DB rows so a restart feels continuous.
    Blends toward _BOOTSTRAP so a previously-degraded run starts cleanly."""
    global _sim_state
    history = fetch_history(12)

    # Pre-populate the telemetry window for gradient computation
    for row in history:
        _add_telemetry_sample({
            "ts_epoch": _parse_ts_epoch(row["ts"]),
            "cpu":      float(row["cpu"]),
            "jitter":   float(row["jitter"]),
            "delay":    float(row["delay"]),
            "lambda_":  float(row["lambda_"]),
        })

    if not history:
        _sim_state = _build_initial_sim_state()
        return

    last = history[-1]
    # Blend 25 % of the last DB row with 75 % of the healthy bootstrap.
    # This prevents a degraded run from immediately re-entering degraded state.
    blended = {
        "cpu":      last["cpu"]     * 0.25 + _BOOTSTRAP["cpu"]     * 0.75,
        "jitter":   last["jitter"]  * 0.20 + _BOOTSTRAP["jitter"]  * 0.80,
        "delay":    last["delay"]   * 0.20 + _BOOTSTRAP["delay"]   * 0.80,
        "lambda_":  last["lambda_"] * 0.35 + _BOOTSTRAP["lambda_"] * 0.65,
        "ims_total": int(round(
            last.get("ims_total", _BOOTSTRAP["ims_total"]) * 0.3
            + _BOOTSTRAP["ims_total"] * 0.7
        )),
    }
    # Hard-cap to healthy values if QoS is still poor after blending
    if calculate_qos(blended["delay"], blended["jitter"], blended["cpu"]) < 82:
        blended.update(cpu=min(blended["cpu"], 28.0),
                       jitter=min(blended["jitter"], 8.0),
                       delay=min(blended["delay"], 14.0))

    seed = {
        "cpu":      round(_clamp(blended["cpu"],      CONFIG["CPU_RANGE"]),     2),
        "jitter":   round(_clamp(blended["jitter"],   CONFIG["JITTER_RANGE"]),  2),
        "delay":    round(_clamp(blended["delay"],    CONFIG["DELAY_RANGE"]),   2),
        "lambda_":  round(_clamp(blended["lambda_"],  CONFIG["LAMBDA_RANGE"]),  2),
        "ims_total": int(_clamp(blended["ims_total"], CONFIG["IMS_SESSION_RANGE"])),
    }
    _sim_state = _build_initial_sim_state(seed)
    _log("startup",
         f"sim seeded: cpu={seed['cpu']:.1f} jitter={seed['jitter']:.1f} "
         f"delay={seed['delay']:.1f} lambda={seed['lambda_']:.2f}")


def simulate_metrics() -> dict:
    """Advance the simulation by one tick.

    Physics model
    ─────────────
    λ  – mean-reverting random walk with momentum and occasional shocks.
         Reverts toward 30 % of LAMBDA_RANGE (healthy operating point),
         NOT the midpoint, so normal operation stays in a low-utilisation zone.
    CPU, jitter, delay – each smoothed toward a target derived from λ-ratio
         and the current shock amplitude.
    CHAOS    – scales shock probability and noise amplitude.
    MOMENTUM – controls how much of the previous λ-velocity carries forward.
    """
    global _sim_state
    if _sim_state is None:
        _sim_state = _build_initial_sim_state()

    chaos    = max(0.0, min(1.5,  float(CONFIG["SIMULATION_CHAOS"])))
    momentum = max(0.0, min(0.97, float(CONFIG["SIMULATION_MOMENTUM"])))
    mu       = max(0.001, float(CONFIG["MU"]))
    s        = _sim_state
    warmup   = max(0, int(s.get("warmup", 0)))
    s["tick"] += 1

    # ── Shocks ──────────────────────────────────────────────────────
    # Shock decays every tick; a new one is injected with p ∝ chaos.
    if random.random() < 0.08 + chaos * 0.08:
        s["shock"] = random.uniform(-1.0, 1.0) * (0.18 + chaos * 0.95)
    else:
        s["shock"] *= 0.72
    effective_chaos = chaos * (0.35 if warmup > 0 else 1.0)
    if warmup > 0:
        s["shock"] *= 0.35

    # ── Traffic rate λ (mean-reverting toward healthy operating point) ──
    lam_lo, lam_hi = CONFIG["LAMBDA_RANGE"]
    # Target is 30 % into the range — normal ops zone
    lambda_normal = lam_lo + (lam_hi - lam_lo) * 0.30
    lambda_drift  = (lambda_normal - s["lambda_"]) * 0.08
    lambda_noise  = random.uniform(-0.28, 0.28) * (0.25 + effective_chaos)
    s["lambda_velocity"] = (
        s["lambda_velocity"] * momentum
        + lambda_drift * 0.4
        + lambda_noise
        + s["shock"] * 0.22
    )
    s["lambda_"] = _clamp(s["lambda_"] + s["lambda_velocity"], CONFIG["LAMBDA_RANGE"])

    # ── IMS session count (lags λ) ───────────────────────────────────
    lambda_ratio  = s["lambda_"] / mu
    sr            = CONFIG["IMS_SESSION_RANGE"]
    base_sessions = sr[0] + (sr[1] - sr[0]) * min(1.0, lambda_ratio)
    s["ims_total"] = int(round(_clamp(
        s["ims_total"] * 0.55 + base_sessions * 0.45
        + random.uniform(-2.5, 2.5) * (0.5 + effective_chaos),
        sr,
    )))

    # ── CPU (driven by λ-ratio, session count, and shock) ────────────
    cpu_target = (
        16.0 + lambda_ratio * 58.0
        + s["ims_total"] * 0.12
        + abs(s["shock"]) * 16.0
    )
    s["cpu"] = _clamp(
        s["cpu"] * 0.72 + cpu_target * 0.28
        + random.uniform(-2.2, 2.2) * (0.55 + effective_chaos),
        CONFIG["CPU_RANGE"],
    )

    # ── Jitter (rises with λ-ratio and high CPU) ─────────────────────
    jitter_target = (
        4.0 + lambda_ratio * 10.0
        + max(0.0, s["cpu"] - 55.0) * 0.12
        + abs(s["shock"]) * 7.0
    )
    s["jitter"] = _clamp(
        s["jitter"] * 0.62 + jitter_target * 0.38
        + random.uniform(-1.3, 1.3) * (0.45 + effective_chaos),
        CONFIG["JITTER_RANGE"],
    )

    # ── Delay (correlated with jitter and CPU) ───────────────────────
    delay_target = (
        8.0 + lambda_ratio * 18.0
        + s["jitter"] * 0.85
        + max(0.0, s["cpu"] - 60.0) * 0.16
    )
    s["delay"] = _clamp(
        s["delay"] * 0.66 + delay_target * 0.34
        + random.uniform(-1.8, 1.8) * (0.45 + effective_chaos),
        CONFIG["DELAY_RANGE"],
    )

    # ── Warmup cap: hold everything in a healthy zone for the first ticks ──
    if warmup > 0:
        cap_lambda = lam_lo + (lam_hi - lam_lo) * 0.35
        s["lambda_"] = min(s["lambda_"], cap_lambda)
        s["cpu"]     = min(s["cpu"],     31.0)
        s["jitter"]  = min(s["jitter"],   8.5)
        s["delay"]   = min(s["delay"],   16.0)
        s["warmup"]  = warmup - 1

    return {
        "cpu":       round(s["cpu"],     2),
        "jitter":    round(s["jitter"],  2),
        "delay":     round(s["delay"],   2),
        "lambda_":   round(s["lambda_"], 2),
        "ims_total": int(s["ims_total"]),
    }


# ── CSV replay ───────────────────────────────────────────────────────

_csv_state: dict = {"rows": [], "index": 0}


def _load_csv() -> bool:
    path = CONFIG["CSV_PATH"]
    if not os.path.exists(path):
        return False
    with open(path, newline="") as f:
        _csv_state["rows"] = list(csv.DictReader(f))
    _csv_state["index"] = 0
    return bool(_csv_state["rows"])


def get_raw_metrics() -> dict:
    """Return one raw metric dict from the configured data source."""
    if CONFIG["DATA_SOURCE"] == "csv":
        if not _csv_state["rows"]:
            _load_csv()
        rows = _csv_state["rows"]
        if rows:
            row = rows[_csv_state["index"] % len(rows)]
            _csv_state["index"] += 1
            return {
                "cpu":       float(row["cpu"]),
                "jitter":    float(row["jitter"]),
                "delay":     float(row["delay"]),
                "lambda_":   float(row["lambda"]),
                "ims_total": int(float(row.get("ims_total", 0) or 0)),
            }
    return simulate_metrics()


# ═══════════════════════════════════════════════════════════════════
# 7.  Telemetry window & gradient analytics
# ═══════════════════════════════════════════════════════════════════

# Bounded rolling window — no memory leak regardless of uptime
_telemetry_window: deque = deque(maxlen=120)


def _parse_ts_epoch(ts: str) -> float:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except Exception:
        return time.time()


def _add_telemetry_sample(sample: dict) -> None:
    _telemetry_window.append(sample)


def compute_gradients(raw: dict, now: float) -> dict:
    """Rate of change (units/s) over the last 10 s for each metric."""
    window = 10.0
    result = {}
    for key in ("jitter", "delay", "cpu", "lambda_"):
        target_ts = now - window
        past_val  = float(raw[key])   # fallback: no change
        for point in reversed(_telemetry_window):
            if point["ts_epoch"] <= target_ts:
                past_val = float(point[key])
                break
        result[key] = round((float(raw[key]) - past_val) / window, 4)
    return result


# ═══════════════════════════════════════════════════════════════════
# 8.  Service-health & queue-projection helpers
# ═══════════════════════════════════════════════════════════════════

def _normalise_weights(w: dict) -> dict:
    values = {k: max(0.0, float(w.get(k, 0))) for k in ("voip", "video", "web")}
    total  = sum(values.values()) or 1.0
    return {k: v / total for k, v in values.items()}


def split_sessions(total: int) -> dict:
    w     = _normalise_weights(CONFIG["SERVICE_WEIGHTS"])
    voip  = int(round(total * w["voip"]))
    video = int(round(total * w["video"]))
    return {"voip": voip, "video": video, "web": max(0, total - voip - video)}


def compute_service_health(raw: dict) -> dict:
    """Service Health Index (SHI) in [0, 100] per service type."""
    return {
        "voip":  round(max(0.0, min(100.0, 100.0 - raw["jitter"] * 2.5 - raw["delay"] * 0.5)), 2),
        "video": round(max(0.0, min(100.0, 100.0 - raw["delay"]  * 1.5 - raw["cpu"]   * 0.2)), 2),
    }


def projected_queue_metrics(raw: dict, gradients: dict) -> dict:
    """Project λ and Wq 30 s ahead using the current λ gradient."""
    proj_lambda = max(0.01, raw["lambda_"] + gradients["lambda_"] * 30.0)
    proj_wq     = calculate_mm1_wait(proj_lambda)
    mu          = CONFIG["MU"]

    if gradients["lambda_"] > 0 and raw["lambda_"] < mu:
        time_to_sat = max(0.0, (mu - raw["lambda_"]) / gradients["lambda_"])
    else:
        time_to_sat = float("inf")

    return {
        "projected_lambda_30s": round(proj_lambda, 3),
        "projected_wq_30s":     proj_wq,
        "projected_wq_30s_ms":  round(proj_wq * 1000, 2) if math.isfinite(proj_wq) else float("inf"),
        "time_to_saturation_s": round(time_to_sat, 2)    if math.isfinite(time_to_sat) else float("inf"),
    }


# ═══════════════════════════════════════════════════════════════════
# 9.  Decision / action-center engine
# ═══════════════════════════════════════════════════════════════════

_decision_state: dict = {
    "last_action": None,
    "last_result": "Hələ heç bir əməliyyat qiymətləndirilməyib.",
}


def _score_to_status(score: float) -> str:
    if score >= 1.80:
        return "Critical"
    if score >= 0.75:
        return "Warning"
    return "Normal"


def _compute_all_scores(raw, sessions, gradients, shi, queue_proj) -> dict:
    """Invoke each action's score_fn and return a {key: score} dict."""
    return {
        key: round(action["score_fn"](raw, sessions, gradients, shi, queue_proj, CONFIG), 4)
        for key, action in ACTION_REGISTRY.items()
    }


def _apply_hysteresis(candidate: str, candidate_status: str) -> tuple:
    """Hold the current action for 15 s to prevent rapid flapping.
    A Critical candidate always overrides immediately."""
    last = _decision_state["last_action"]
    if not last or (time.time() - last["applied_at"]) > 15:
        return candidate, candidate_status
    if candidate == last["scenario"] or severity_rank(candidate_status) >= severity_rank("Critical"):
        return candidate, candidate_status
    return last["scenario"], last["status"]


def _assess_closed_loop(action_key: str, raw: dict, shi: dict, queue_proj: dict) -> str:
    """Compare current telemetry to the baseline captured when the action
    was applied; return a human-readable outcome."""
    last = _decision_state["last_action"]
    if not last or last["scenario"] != action_key:
        return _decision_state["last_result"]

    if (time.time() - last["applied_at"]) < 6:
        return "Əməliyyat tətbiq edildi; dəyişiklikdən sonrakı telemetriya gözlənilir."

    b        = last["baseline"]
    improved = 0

    if action_key == "rtp_priority_qos":
        improved += int(raw["jitter"] <= b["jitter"])
        improved += int(shi["voip"]   >= b["shi_voip"])
    elif action_key == "increase_bandwidth_reservation":
        improved += int(raw["delay"]  <= b["delay"])
        improved += int(shi["video"]  >= b["shi_video"])
    else:
        improved += int(raw["lambda_"]              <= b["lambda_"])
        improved += int(queue_proj["projected_wq_30s_ms"] <= b["projected_wq_30s_ms"])

    result = (
        "Optimizasiya uğurla tamamlandı"
        if improved >= 1
        else "Əməliyyat təsirsiz oldu; eskalasiya edilir."
    )
    _decision_state["last_result"] = result
    return result


def _build_decision(action_key: str, status: str, mode: str,
                    raw: dict, sessions: dict, gradients: dict,
                    shi: dict, queue_proj: dict,
                    confidence: float, opt_result: str) -> dict:
    action       = ACTION_REGISTRY[action_key]
    total        = sum(sessions.values())
    lambda_ratio = raw["lambda_"] / max(CONFIG["MU"], 0.001)

    rationale = (
        f"Son 10 s ərzində jitter {gradients['jitter']:.2f} ms/s, "
        f"gecikmə {gradients['delay']:.2f} ms/s, CPU {gradients['cpu']:.2f} %/s dəyişib. "
        f"VoIP SHI={shi['voip']:.1f}, Video SHI={shi['video']:.1f}. "
        f"λ/μ={lambda_ratio:.2f}."
    )
    if math.isfinite(queue_proj["projected_wq_30s_ms"]):
        rationale += f" 30 s proqnoz Wq={queue_proj['projected_wq_30s_ms']:.1f} ms."
    if math.isfinite(queue_proj["time_to_saturation_s"]):
        rationale += f" Doyma ~{queue_proj['time_to_saturation_s']:.1f} s."

    return {
        "mode":                mode,
        "scenario":            action_key,
        "status":              status,
        "service":             action["service"],
        "priority":            action["priority"],
        "diagnosis":           action["diagnosis"],
        "rationale":           rationale,
        "proposed_patch":      action["patch"],
        "confidence_score":    round(confidence, 2),
        "optimization_result": opt_result,
        "analysis":            f"IMS sessiyaları cəmi={total}, VoIP={sessions['voip']}, Video={sessions['video']}.",
        "decision":            action["diagnosis"],
        "optimization":        action["patch"],
    }


def resolve_action(raw: dict, sessions: dict, gradients: dict,
                   shi: dict, queue_proj: dict, current_status: str) -> dict:
    """Select and return the best action for the current network state."""
    opt_result = _decision_state["last_result"]

    # Manual override: use whatever was configured via the API
    if CONFIG["ACTION_MODE"] == "manual":
        return _build_decision(
            CONFIG["FORCED_ACTION"], CONFIG["FORCED_STATUS"], "manual",
            raw, sessions, gradients, shi, queue_proj,
            confidence=0.95, opt_result=opt_result,
        )

    # Score every registered action
    scores = _compute_all_scores(raw, sessions, gradients, shi, queue_proj)
    action_key, top_score = max(scores.items(), key=lambda kv: kv[1])

    # Suppress weak non-observe picks
    if action_key != "observe" and top_score < 0.75:
        action_key, top_score = "observe", scores["observe"]

    confidence = min(0.99, 0.35 + top_score / 5.0)
    status     = _score_to_status(top_score)
    if action_key == "observe":
        status = current_status if (current_status != "Normal" and top_score > 0.5) else "Normal"

    action_key, status = _apply_hysteresis(action_key, status)
    opt_result = _assess_closed_loop(action_key, raw, shi, queue_proj)

    # Record baseline for closed-loop comparison on subsequent ticks
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

    return _build_decision(
        action_key, status, "auto",
        raw, sessions, gradients, shi, queue_proj,
        confidence=confidence, opt_result=opt_result,
    )


# ═══════════════════════════════════════════════════════════════════
# 10. Snapshot assembly
# ═══════════════════════════════════════════════════════════════════

def build_snapshot() -> dict:
    """Assemble one complete snapshot: raw metrics → analytics → decision."""
    raw       = get_raw_metrics()
    now_epoch = time.time()
    ims_total = int(raw.get("ims_total") or random.randint(*CONFIG["IMS_SESSION_RANGE"]))
    sessions  = split_sessions(ims_total)

    _add_telemetry_sample({
        "ts_epoch": now_epoch,
        "cpu": raw["cpu"], "jitter": raw["jitter"],
        "delay": raw["delay"], "lambda_": raw["lambda_"],
    })

    gradients  = compute_gradients(raw, now_epoch)
    shi        = compute_service_health(raw)
    queue_proj = projected_queue_metrics(raw, gradients)

    qos    = calculate_qos(raw["delay"], raw["jitter"], raw["cpu"])
    wq     = calculate_mm1_wait(raw["lambda_"])
    status = classify_status(qos)

    decision = resolve_action(raw, sessions, gradients, shi, queue_proj, status)

    # The action center may escalate status upward but never downgrade it
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


# ═══════════════════════════════════════════════════════════════════
# 11. Config helpers
# ═══════════════════════════════════════════════════════════════════

def _coerce_config_value(key: str, value):
    """Convert an incoming JSON value to the Python type CONFIG expects."""
    if key in _RANGE_KEYS:
        lo, hi = float(value[0]), float(value[1])
        return (min(lo, hi), max(lo, hi))
    if key == "SERVICE_WEIGHTS":
        return _normalise_weights(value)
    if key in ("MU", "W_DELAY", "W_JITTER", "W_CPU",
               "SIMULATION_CHAOS", "SIMULATION_MOMENTUM",
               "VOIP_JITTER_THRESHOLD", "VIDEO_DELAY_THRESHOLD",
               "CPU_WARNING_THRESHOLD", "LAMBDA_WARNING_RATIO"):
        return float(value)
    if key == "PUSH_INTERVAL":
        return max(1, int(value))
    return value


def _validate_config_value(key: str, value) -> tuple:
    """Return (ok: bool, error: str)."""
    try:
        rules = {
            "SIMULATION_CHAOS":    lambda v: 0.0 <= float(v) <= 1.5   or "must be in [0.0, 1.5]",
            "SIMULATION_MOMENTUM": lambda v: 0.0 <= float(v) <= 0.97  or "must be in [0.0, 0.97]",
            "MU":                  lambda v: float(v) > 0              or "must be > 0",
            "PUSH_INTERVAL":       lambda v: int(v) >= 1               or "must be >= 1",
            "ACTION_MODE":         lambda v: v in ("auto", "manual")   or "must be 'auto' or 'manual'",
            "FORCED_ACTION":       lambda v: v in ACTION_REGISTRY      or f"must be one of {sorted(ACTION_REGISTRY)}",
        }
        for wk in ("W_DELAY", "W_JITTER", "W_CPU"):
            rules[wk] = lambda v: 0.0 <= float(v) <= 1.0 or "must be in [0.0, 1.0]"

        if key in rules:
            result = rules[key](value)
            if result is not True:
                return False, result

        if key in _RANGE_KEYS:
            if not (isinstance(value, (list, tuple)) and len(value) == 2):
                return False, "must be a two-element list [min, max]"
            if float(value[0]) >= float(value[1]):
                return False, "min must be less than max"

    except (TypeError, ValueError) as exc:
        return False, f"type error: {exc}"

    return True, ""


# ═══════════════════════════════════════════════════════════════════
# 12. Push-loop (background thread)
# ═══════════════════════════════════════════════════════════════════

_push_lock:   Lock = Lock()
_push_thread        = None

# Tracks the last emitted state so we send only changed optional fields.
_last_emit: dict = {"action_sig": None, "ims_total": None, "service_sessions": None}


def _action_signature(action: dict) -> tuple:
    """Stable tuple identifying the current action decision for change detection."""
    if not action:
        return None
    return (
        action.get("scenario"),
        action.get("status"),
        round(float(action.get("confidence_score", 0.0)), 2),
        action.get("optimization_result"),
    )


def _build_delta(snapshot: dict) -> dict:
    """Core telemetry fields are always included.
    Optional fields are only included when they change,
    keeping the per-tick payload small."""
    action_sig = _action_signature(snapshot.get("action_center"))

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

    # Update only the small tracking fields — not the full snapshot
    _last_emit["action_sig"]       = action_sig
    _last_emit["ims_total"]        = snapshot.get("ims_total")
    _last_emit["service_sessions"] = snapshot.get("service_sessions")

    return delta


def _push_loop() -> None:
    """Runs in a single background thread for the lifetime of the process.
    Builds one snapshot per PUSH_INTERVAL, persists it, and broadcasts the
    minimal delta to all connected clients."""
    while True:
        try:
            snapshot = build_snapshot()
            insert_metric(snapshot)
            socketio.emit("metric_update", _build_delta(snapshot))
        except Exception as exc:
            _log("push_loop", f"error: {exc}")
        time.sleep(CONFIG["PUSH_INTERVAL"])


def _ensure_push_loop() -> None:
    """Start the push-loop thread exactly once (safe to call from multiple contexts)."""
    global _push_thread
    with _push_lock:
        if _push_thread is None:
            _push_thread = Thread(target=_push_loop, daemon=True, name="push-loop")
            _push_thread.start()


# ═══════════════════════════════════════════════════════════════════
# Shared utility
# ═══════════════════════════════════════════════════════════════════

def _log(channel: str, message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] [{channel}] {message}", flush=True)


# ═══════════════════════════════════════════════════════════════════
# 13. Flask routes
# ═══════════════════════════════════════════════════════════════════

@app.route("/")
def index():
    _ensure_push_loop()
    return render_template("index.html")


@app.route("/api/snapshot")
def api_snapshot():
    snap = build_snapshot()
    insert_metric(snap)
    return jsonify(snap)


@app.route("/api/history")
def api_history():
    return jsonify(fetch_history(int(request.args.get("limit", 60))))


@app.route("/api/mm1_curve")
def api_mm1_curve():
    mu = float(request.args.get("mu", CONFIG["MU"]))
    return jsonify({"mu": mu, "curve": mm1_curve(mu)})


@app.route("/api/config", methods=["GET", "POST"])
def api_config():
    """
    GET  – current configuration and available action keys.
    POST – partial update: send only the keys you want to change.

    Example — change only chaos without touching anything else:
        POST /api/config   {"SIMULATION_CHAOS": 0.8}
    """
    if request.method == "POST":
        data     = request.get_json(force=True) or {}
        applied  = {}
        rejected = {}
        ignored  = [k for k in data if k not in _UPDATABLE_KEYS]

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


# ═══════════════════════════════════════════════════════════════════
# 14. SocketIO event handlers
# ═══════════════════════════════════════════════════════════════════

@socketio.on("connect")
def on_connect():
    """Send a bootstrap payload containing the current snapshot and recent
    history so the client can populate charts before the first push tick."""
    _ensure_push_loop()
    ns = request.namespace or "/"
    room_count = len(socketio.server.manager.rooms.get(ns, {}))
    _log("socket", f"connected sid={request.sid}  active={room_count}")

    snapshot = build_snapshot()
    emit("bootstrap_data", {
        "snapshot": snapshot,
        "history":  fetch_history(30),
    })
    # snapshot is a local variable — released after this function returns.
    # No module-level reference is kept, preventing per-connection retention.


@socketio.on("disconnect")
def on_disconnect():
    ns = request.namespace or "/"
    room_count = len(socketio.server.manager.rooms.get(ns, {}))
    _log("socket", f"disconnected sid={request.sid}  active={room_count}")


# ═══════════════════════════════════════════════════════════════════
# 15. Entry point
# ═══════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    init_db()
    _load_csv()
    seed_runtime_state()
    _ensure_push_loop()
    _log("startup", f"GPON/IMS Monitor  →  http://0.0.0.0:5000  (push_interval={CONFIG['PUSH_INTERVAL']}s)")
    _log("startup", "waitress not found — falling back to Werkzeug (dev only)")
    socketio.run(app, host="0.0.0.0", port=5000, debug=False,
                     allow_unsafe_werkzeug=True)