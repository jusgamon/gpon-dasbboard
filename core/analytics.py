# core/analytics.py
import time
import math
from collections import deque
from datetime import datetime
from core.config import CONFIG
from core.models import calculate_mm1_wait

_telemetry_window: deque = deque(maxlen=120)

def _parse_ts_epoch(ts: str) -> float:
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).timestamp()
    except Exception:
        return time.time()

def add_telemetry_sample(sample: dict) -> None:
    _telemetry_window.append(sample)

def clear_telemetry() -> None:
    _telemetry_window.clear()

def compute_gradients(raw: dict, now: float) -> dict:
    window = 10.0
    result = {}
    for key in ("jitter", "delay", "cpu", "lambda_"):
        target_ts = now - window
        past_val = float(raw[key])
        for point in reversed(_telemetry_window):
            if point["ts_epoch"] <= target_ts:
                past_val = float(point[key])
                break
        result[key] = round((float(raw[key]) - past_val) / window, 4)
    return result

def compute_gradients_from_history(history: list, raw: dict, now: float) -> dict:
    window = 10.0
    result = {}
    for key in ("jitter", "delay", "cpu", "lambda_"):
        target_ts = now - window
        past_val = float(raw[key])
        for point in reversed(history):
            if point.get("ts_epoch", 0) <= target_ts:
                past_val = float(point[key])
                break
        result[key] = round((float(raw[key]) - past_val) / window, 4)
    return result

def _normalise_weights(w: dict) -> dict:
    values = {k: max(0.0, float(w.get(k, 0))) for k in ("voip", "video", "web")}
    total = sum(values.values()) or 1.0
    return {k: v / total for k, v in values.items()}

def split_sessions(total: int, config: dict = CONFIG) -> dict:
    w = _normalise_weights(config["SERVICE_WEIGHTS"])
    voip = int(round(total * w["voip"]))
    video = int(round(total * w["video"]))
    return {"voip": voip, "video": video, "web": max(0, total - voip - video)}

def compute_service_health(raw: dict) -> dict:
    return {
        "voip": round(max(0.0, min(100.0, 100.0 - raw["jitter"] * 2.5 - raw["delay"] * 0.5)), 2),
        "video": round(max(0.0, min(100.0, 100.0 - raw["delay"] * 1.5 - raw["cpu"] * 0.2)), 2),
    }

def projected_queue_metrics(raw: dict, gradients: dict, config: dict = CONFIG) -> dict:
    mu = config["MU"]
    proj_lambda = max(0.01, raw["lambda_"] + gradients["lambda_"] * 30.0)
    proj_wq = calculate_mm1_wait(proj_lambda, mu)
    if gradients["lambda_"] > 0 and raw["lambda_"] < mu:
        time_to_sat = max(0.0, (mu - raw["lambda_"]) / gradients["lambda_"])
    else:
        time_to_sat = float("inf")
    return {
        "projected_lambda_30s": round(proj_lambda, 3),
        "projected_wq_30s": proj_wq,
        "projected_wq_30s_ms": round(proj_wq * 1000, 2) if math.isfinite(proj_wq) else float("inf"),
        "time_to_saturation_s": round(time_to_sat, 2) if math.isfinite(time_to_sat) else float("inf"),
    }