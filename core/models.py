# core/models.py
import math
from core.config import CONFIG

def calculate_qos(delay: float, jitter: float, cpu: float) -> float:
    raw = 100.0 - CONFIG["W_DELAY"] * delay - CONFIG["W_JITTER"] * jitter - CONFIG["W_CPU"] * cpu
    return round(max(0.0, min(100.0, raw)), 2)

def calculate_mm1_wait(lambda_: float, mu: float = None) -> float:
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
    mu = mu if mu is not None else CONFIG["MU"]
    result = []
    for i in range(1, steps + 1):
        lam = round(mu * i / (steps + 1), 4)
        wait = calculate_mm1_wait(lam, mu)
        result.append({"lambda": lam, "W": wait if math.isfinite(wait) else None})
    return result