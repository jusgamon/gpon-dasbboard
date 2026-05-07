# utils.py
from datetime import datetime
from core.actions import ACTION_REGISTRY
from core.config import _RANGE_KEYS

def _log(channel: str, message: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] [{channel}] {message}", flush=True)

def _normalise_weights(w: dict) -> dict:
    values = {k: max(0.0, float(w.get(k, 0))) for k in ("voip", "video", "web")}
    total = sum(values.values()) or 1.0
    return {k: v / total for k, v in values.items()}

def _coerce_config_value(key: str, value):
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
    try:
        rules = {
            "SIMULATION_CHAOS": lambda v: 0.0 <= float(v) <= 1.5 or "must be in [0.0, 1.5]",
            "SIMULATION_MOMENTUM": lambda v: 0.0 <= float(v) <= 0.97 or "must be in [0.0, 0.97]",
            "MU": lambda v: float(v) > 0 or "must be > 0",
            "PUSH_INTERVAL": lambda v: int(v) >= 1 or "must be >= 1",
            "ACTION_MODE": lambda v: v in ("auto", "manual") or "must be 'auto' or 'manual'",
            "FORCED_ACTION": lambda v: v in ACTION_REGISTRY or f"must be one of {sorted(ACTION_REGISTRY)}",
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