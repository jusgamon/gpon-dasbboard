# core/simulation.py
import csv
import os
import random
from core.config import CONFIG, _BOOTSTRAP
from core.database import fetch_history
from core.analytics import add_telemetry_sample, _parse_ts_epoch
from core.models import calculate_qos
from core.utils import _log

_sim_state: dict = None
_csv_state: dict = {"rows": [], "index": 0}

def _clamp(value: float, bounds: tuple) -> float:
    return max(bounds[0], min(bounds[1], value))

def _build_initial_sim_state(seed: dict = None) -> dict:
    base = seed if seed else _BOOTSTRAP.copy()
    return {
        **base,
        "lambda_velocity": 0.0,
        "shock": 0.0,
        "tick": 0,
        "warmup": 8,
    }

def seed_runtime_state() -> None:
    global _sim_state
    history = fetch_history(12)
    for row in history:
        add_telemetry_sample({
            "ts_epoch": _parse_ts_epoch(row["ts"]),
            "cpu": float(row["cpu"]),
            "jitter": float(row["jitter"]),
            "delay": float(row["delay"]),
            "lambda_": float(row["lambda_"]),
        })
    if not history:
        _sim_state = _build_initial_sim_state()
        return
    last = history[-1]
    blended = {
        "cpu": last["cpu"] * 0.25 + _BOOTSTRAP["cpu"] * 0.75,
        "jitter": last["jitter"] * 0.20 + _BOOTSTRAP["jitter"] * 0.80,
        "delay": last["delay"] * 0.20 + _BOOTSTRAP["delay"] * 0.80,
        "lambda_": last["lambda_"] * 0.35 + _BOOTSTRAP["lambda_"] * 0.65,
        "ims_total": int(round(
            last.get("ims_total", _BOOTSTRAP["ims_total"]) * 0.3
            + _BOOTSTRAP["ims_total"] * 0.7
        )),
    }
    if calculate_qos(blended["delay"], blended["jitter"], blended["cpu"]) < 82:
        blended.update(cpu=min(blended["cpu"], 28.0),
                       jitter=min(blended["jitter"], 8.0),
                       delay=min(blended["delay"], 14.0))
    seed = {
        "cpu": round(_clamp(blended["cpu"], CONFIG["CPU_RANGE"]), 2),
        "jitter": round(_clamp(blended["jitter"], CONFIG["JITTER_RANGE"]), 2),
        "delay": round(_clamp(blended["delay"], CONFIG["DELAY_RANGE"]), 2),
        "lambda_": round(_clamp(blended["lambda_"], CONFIG["LAMBDA_RANGE"]), 2),
        "ims_total": int(_clamp(blended["ims_total"], CONFIG["IMS_SESSION_RANGE"])),
    }
    _sim_state = _build_initial_sim_state(seed)
    _log("startup",
         f"sim seeded: cpu={seed['cpu']:.1f} jitter={seed['jitter']:.1f} "
         f"delay={seed['delay']:.1f} lambda={seed['lambda_']:.2f}")

def reset_simulation() -> None:
    """Restart simulation from initial state, clear all runtime memory."""
    global _sim_state, _csv_state
    _sim_state = None
    _csv_state = {"rows": [], "index": 0}

def simulate_metrics() -> dict:
    global _sim_state
    if _sim_state is None:
        _sim_state = _build_initial_sim_state()

    chaos = max(0.0, min(1.5, float(CONFIG["SIMULATION_CHAOS"])))
    momentum = max(0.0, min(0.97, float(CONFIG["SIMULATION_MOMENTUM"])))
    mu = max(0.001, float(CONFIG["MU"]))
    s = _sim_state
    warmup = max(0, int(s.get("warmup", 0)))
    s["tick"] += 1
    intensity = chaos / 1.5

    lam_lo, lam_hi = CONFIG["LAMBDA_RANGE"]
    if chaos > 0.5:
        extra = (chaos - 0.5) * 4.0
        lam_hi_eff = min(lam_hi + extra, mu - 0.05)
    else:
        lam_hi_eff = lam_hi

    target_ratio = 0.30 + intensity * 0.50
    lambda_normal = lam_lo + (lam_hi_eff - lam_lo) * target_ratio

    shock_prob = 0.05 + chaos * 0.15
    shock_mag = 0.2 + chaos * 1.2
    if random.random() < shock_prob:
        s["shock"] = random.uniform(-1.0, 1.0) * shock_mag
    else:
        s["shock"] *= 0.72

    effective_chaos = chaos * (0.35 if warmup > 0 else 1.0)
    if warmup > 0:
        s["shock"] *= 0.35

    lambda_drift = (lambda_normal - s["lambda_"]) * 0.08
    lambda_noise = random.uniform(-0.5, 0.5) * (1.0 + intensity * 2.0)
    s["lambda_velocity"] = (
        s["lambda_velocity"] * momentum
        + lambda_drift * 0.4
        + lambda_noise
        + s["shock"] * 0.22
    )
    s["lambda_"] = _clamp(s["lambda_"] + s["lambda_velocity"], (lam_lo, lam_hi_eff))

    lambda_ratio = s["lambda_"] / mu
    sr = CONFIG["IMS_SESSION_RANGE"]
    base_sessions = sr[0] + (sr[1] - sr[0]) * min(1.0, lambda_ratio)
    s["ims_total"] = int(round(_clamp(
        s["ims_total"] * 0.55 + base_sessions * 0.45
        + random.uniform(-2.5, 2.5) * (0.5 + effective_chaos),
        sr,
    )))

    base_alpha = 0.28
    alpha = base_alpha + intensity * 0.4
    noise_scale = 1.0 + intensity * 1.5

    cpu_target = (
        16.0 + lambda_ratio * 58.0
        + s["ims_total"] * 0.12
        + abs(s["shock"]) * 16.0
    )
    s["cpu"] = _clamp(
        s["cpu"] * (1.0 - alpha) + cpu_target * alpha
        + random.uniform(-2.2, 2.2) * (0.55 + effective_chaos) * noise_scale,
        CONFIG["CPU_RANGE"],
    )

    jitter_target = (
        4.0 + lambda_ratio * 10.0
        + max(0.0, s["cpu"] - 55.0) * 0.12
        + abs(s["shock"]) * 7.0
        + chaos * 12.0
    )
    s["jitter"] = _clamp(
        s["jitter"] * (1.0 - alpha) + jitter_target * alpha
        + random.uniform(-1.3, 1.3) * (0.45 + effective_chaos) * noise_scale,
        CONFIG["JITTER_RANGE"],
    )

    delay_target = (
        8.0 + lambda_ratio * 18.0
        + s["jitter"] * 0.85
        + max(0.0, s["cpu"] - 60.0) * 0.16
        + chaos * 15.0
    )
    s["delay"] = _clamp(
        s["delay"] * (1.0 - alpha) + delay_target * alpha
        + random.uniform(-1.8, 1.8) * (0.45 + effective_chaos) * noise_scale,
        CONFIG["DELAY_RANGE"],
    )

    if warmup > 0:
        cap_lambda = lam_lo + (lam_hi - lam_lo) * 0.35
        s["lambda_"] = min(s["lambda_"], cap_lambda)
        s["cpu"] = min(s["cpu"], 31.0)
        s["jitter"] = min(s["jitter"], 8.5)
        s["delay"] = min(s["delay"], 16.0)
        s["warmup"] = warmup - 1

    return {
        "cpu": round(s["cpu"], 2),
        "jitter": round(s["jitter"], 2),
        "delay": round(s["delay"], 2),
        "lambda_": round(s["lambda_"], 2),
        "ims_total": int(s["ims_total"]),
    }

def _load_csv() -> bool:
    path = CONFIG["CSV_PATH"]
    if not os.path.exists(path):
        return False
    with open(path, newline="") as f:
        _csv_state["rows"] = list(csv.DictReader(f))
    _csv_state["index"] = 0
    return bool(_csv_state["rows"])

def get_raw_metrics() -> dict:
    if CONFIG["DATA_SOURCE"] == "csv":
        if not _csv_state["rows"]:
            _load_csv()
        rows = _csv_state["rows"]
        if rows:
            row = rows[_csv_state["index"] % len(rows)]
            _csv_state["index"] += 1
            return {
                "cpu": float(row["cpu"]),
                "jitter": float(row["jitter"]),
                "delay": float(row["delay"]),
                "lambda_": float(row["lambda"]),
                "ims_total": int(float(row.get("ims_total", 0) or 0)),
            }
    return simulate_metrics()