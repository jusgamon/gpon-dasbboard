# core/logic.py
from typing import List, Optional, Dict, Any
import time

from core.simulation import simulate_metrics
from core.analytics import (
    compute_gradients_from_history,
    split_sessions,
    compute_service_health,
    projected_queue_metrics
)
from core.actions import evaluate_logic
from core.models import calculate_qos, calculate_mm1_wait, classify_status
from core.config import CONFIG

def execute_logic(raw: Dict[str, Any],
                  history: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """
    Run analysis, prediction, and action against a given metric snapshot.

    Args:
        raw: dict with keys cpu, jitter, delay, lambda_, ims_total.
        history: optional list of past raw metrics (with ts_epoch) for gradients.

    Returns:
        Dict with analytics, decision, QoS, status, etc.
    """
    cfg = CONFIG
    ims_total = int(raw.get("ims_total", 0))
    sessions = split_sessions(ims_total, cfg)
    now_epoch = time.time()
    if history:
        gradients = compute_gradients_from_history(history, raw, now_epoch)
    else:
        gradients = {k: 0.0 for k in ("jitter", "delay", "cpu", "lambda_")}
    shi = compute_service_health(raw)
    queue_proj = projected_queue_metrics(raw, gradients, cfg)
    qos = calculate_qos(raw["delay"], raw["jitter"], raw["cpu"])
    wq = calculate_mm1_wait(raw["lambda_"], cfg["MU"])
    status = classify_status(qos)
    decision = evaluate_logic(raw, sessions, gradients, shi, queue_proj, cfg)
    return {
        "analytics": {
            "gradients": gradients,
            "shi": shi,
            "queue_projection": queue_proj,
        },
        "action_center": decision,
        "qos": qos,
        "wq": wq,
        "status": status,
        "service_sessions": sessions,
    }

def run_simulation() -> Dict[str, Any]:
    """Advance the simulation by one step and return raw metrics."""
    return simulate_metrics()