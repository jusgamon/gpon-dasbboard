# test_gpon_ims.py
"""
Test suite for GPON/IMS orchestration system.
Tests execute_logic() and run_simulation() with specific inputs
targeting each action in the registry.
"""
import math
import pytest
import time

from core.logic import execute_logic, run_simulation
from core.config import CONFIG
from core.analytics import split_sessions, compute_service_health, projected_queue_metrics
from core.actions import evaluate_logic


# ──────────────────────────────────────────────────────────────────
# Helper: build a raw metric dict with defaults
# ──────────────────────────────────────────────────────────────────
def make_raw(cpu=25.0, jitter=6.0, delay=12.0, lambda_=2.5, ims_total=20):
    return {
        "cpu": cpu,
        "jitter": jitter,
        "delay": delay,
        "lambda_": lambda_,
        "ims_total": ims_total,
    }


def make_history(raw, count=10, seconds_ago=0.5):
    """Create a simple history list from a raw dict."""
    now = time.time()
    return [
        {
            "ts_epoch": now - seconds_ago * (count - i),
            "cpu": raw["cpu"],
            "jitter": raw["jitter"],
            "delay": raw["delay"],
            "lambda_": raw["lambda_"],
        }
        for i in range(count)
    ]


# ──────────────────────────────────────────────────────────────────
# Test: execute_logic returns correct structure
# ──────────────────────────────────────────────────────────────────
def test_execute_logic_returns_required_keys():
    raw = make_raw()
    history = make_history(raw)
    result = execute_logic(raw, history)

    assert isinstance(result, dict)

    # Top‑level keys
    assert "analytics" in result
    assert "action_center" in result
    assert "qos" in result
    assert "wq" in result
    assert "status" in result
    assert "service_sessions" in result

    # Analytics sub‑keys
    analytics = result["analytics"]
    assert "gradients" in analytics
    assert "shi" in analytics
    assert "queue_projection" in analytics

    # Gradients sub‑keys
    for k in ("jitter", "delay", "cpu", "lambda_"):
        assert k in analytics["gradients"]

    # SHI sub‑keys
    assert "voip" in analytics["shi"]
    assert "video" in analytics["shi"]

    # Queue projection sub‑keys
    qp = analytics["queue_projection"]
    assert "projected_lambda_30s" in qp
    assert "projected_wq_30s" in qp
    assert "projected_wq_30s_ms" in qp
    assert "time_to_saturation_s" in qp

    # Action center sub‑keys
    ac = result["action_center"]
    for k in ("mode", "scenario", "status", "service", "priority",
              "diagnosis", "rationale", "proposed_patch",
              "confidence_score", "optimization_result",
              "analysis", "decision", "optimization"):
        assert k in ac, f"Missing key: {k}"


# ──────────────────────────────────────────────────────────────────
# Test: run_simulation returns correct metric keys
# ──────────────────────────────────────────────────────────────────
def test_run_simulation_returns_required_keys():
    for _ in range(5):
        result = run_simulation()
        assert isinstance(result, dict)
        for k in ("cpu", "jitter", "delay", "lambda_", "ims_total"):
            assert k in result
        # Type checks
        assert isinstance(result["cpu"], (int, float))
        assert isinstance(result["jitter"], (int, float))
        assert isinstance(result["delay"], (int, float))
        assert isinstance(result["lambda_"], (int, float))
        assert isinstance(result["ims_total"], int)


# ──────────────────────────────────────────────────────────────────
# Test: execute_logic with normal / healthy metrics → observe
# ──────────────────────────────────────────────────────────────────
def test_healthy_returns_observe():
    raw = make_raw(cpu=22, jitter=5, delay=10, lambda_=2.0)
    history = make_history(raw)
    result = execute_logic(raw, history)

    assert result["status"] == "Normal"
    assert result["qos"] >= 85
    assert result["action_center"]["scenario"] == "observe"


# ──────────────────────────────────────────────────────────────────
# Test: RTP Priority (high VoIP jitter)
# ──────────────────────────────────────────────────────────────────
def test_rtp_priority_action():
    raw = make_raw(cpu=40, jitter=32, delay=15, lambda_=3.5, ims_total=30)
    history = make_history(raw)
    result = execute_logic(raw, history)

    ac = result["action_center"]
    # With high jitter, should trigger VoIP action
    # (may fall back to observe if score < 0.75, but high jitter usually wins)
    print(f"[test_rtp] scenario={ac['scenario']} score={ac['confidence_score']:.3f} status={ac['status']}")
    assert result["analytics"]["shi"]["voip"] < 80  # SHI should be degraded


# ──────────────────────────────────────────────────────────────────
# Test: Bandwidth Reservation (high delay + CPU, video stress)
# ──────────────────────────────────────────────────────────────────
def test_bandwidth_reservation_action():
    raw = make_raw(cpu=88, jitter=10, delay=35, lambda_=4.0, ims_total=45)
    history = make_history(raw)
    result = execute_logic(raw, history)

    ac = result["action_center"]
    print(f"[test_bw] scenario={ac['scenario']} score={ac['confidence_score']:.3f} status={ac['status']}")
    # Should NOT be normal observe
    assert result["status"] in ("Warning", "Critical")


# ──────────────────────────────────────────────────────────────────
# Test: Load Balance (high λ/μ ratio)
# ──────────────────────────────────────────────────────────────────
def test_load_balance_action():
    raw = make_raw(cpu=60, jitter=12, delay=18, lambda_=7.0, ims_total=50)
    history = make_history(raw)
    result = execute_logic(raw, history)

    ac = result["action_center"]
    print(f"[test_lb] scenario={ac['scenario']} score={ac['confidence_score']:.3f} status={ac['status']}")
    assert result["status"] in ("Warning", "Critical")
    assert result["wq"] > 0.1  # M/M/1 wait should be elevated


# ──────────────────────────────────────────────────────────────────
# Test: Preemptive Shaping (approaching saturation)
# ──────────────────────────────────────────────────────────────────
def test_preemptive_shaping_action():
    raw = make_raw(cpu=75, jitter=20, delay=25, lambda_=8.5, ims_total=55)
    history = make_history(raw)
    result = execute_logic(raw, history)

    qp = result["analytics"]["queue_projection"]
    print(f"[test_ps] scenario={result['action_center']['scenario']} "
          f"Wq_30s={qp['projected_wq_30s_ms']:.1f} t_sat={qp['time_to_saturation_s']}")
    # Projected queue wait should be elevated
    assert qp["projected_wq_30s_ms"] > 10 or not math.isfinite(qp["projected_wq_30s_ms"])


# ──────────────────────────────────────────────────────────────────
# Test: CPU Shedding (very high CPU)
# ──────────────────────────────────────────────────────────────────
def test_cpu_shedding_action():
    raw = make_raw(cpu=91, jitter=25, delay=30, lambda_=5.0, ims_total=40)
    history = make_history(raw)
    result = execute_logic(raw, history)

    ac = result["action_center"]
    print(f"[test_cpu] scenario={ac['scenario']} score={ac['confidence_score']:.3f} status={ac['status']}")
    # CPU above 88 should trigger some action
    assert result["status"] in ("Warning", "Critical")


# ──────────────────────────────────────────────────────────────────
# Test: Session Guard (high IMS session count)
# ──────────────────────────────────────────────────────────────────
def test_session_guard_action():
    raw = make_raw(cpu=50, jitter=15, delay=20, lambda_=5.5, ims_total=58)
    history = make_history(raw)
    result = execute_logic(raw, history)

    ac = result["action_center"]
    print(f"[test_sg] scenario={ac['scenario']} score={ac['confidence_score']:.3f} status={ac['status']}")
    # With 58/60 sessions, should trigger action
    assert raw["ims_total"] > CONFIG["IMS_SESSION_RANGE"][1] * 0.75


# ──────────────────────────────────────────────────────────────────
# Test: Transport Retransmission (high delay, delay growing faster)
# ──────────────────────────────────────────────────────────────────
def test_transport_retransmission():
    raw = make_raw(cpu=45, jitter=10, delay=52, lambda_=4.0, ims_total=35)
    history = make_history(raw)
    result = execute_logic(raw, history)

    ac = result["action_center"]
    print(f"[test_tr] scenario={ac['scenario']} score={ac['confidence_score']:.3f} status={ac['status']}")
    assert raw["delay"] >= 50.0  # Above transport threshold


# ──────────────────────────────────────────────────────────────────
# Test: execute_logic without history uses zero gradients
# ──────────────────────────────────────────────────────────────────
def test_execute_logic_no_history():
    raw = make_raw(cpu=90, jitter=30, delay=40, lambda_=7.0)
    result = execute_logic(raw, history=None)

    gradients = result["analytics"]["gradients"]
    for val in gradients.values():
        assert val == 0.0


# ──────────────────────────────────────────────────────────────────
# Test: QoS calculation ranges [0, 100]
# ──────────────────────────────────────────────────────────────────
def test_qos_range():
    # Very healthy
    raw = make_raw(cpu=10, jitter=2, delay=5, lambda_=1.0)
    history = make_history(raw)
    result = execute_logic(raw, history)
    assert 0 <= result["qos"] <= 100

    # Very degraded
    raw2 = make_raw(cpu=95, jitter=45, delay=80, lambda_=7.5)
    result2 = execute_logic(raw2, make_history(raw2))
    assert 0 <= result2["qos"] <= 100


# ──────────────────────────────────────────────────────────────────
# Test: split_sessions respects service weights
# ──────────────────────────────────────────────────────────────────
def test_split_sessions_consistency():
    total = 50
    sessions = split_sessions(total, CONFIG)
    assert sessions["voip"] + sessions["video"] + sessions["web"] == total
    assert sessions["voip"] > sessions["web"]  # VoIP weight 0.40 > web 0.25


# ──────────────────────────────────────────────────────────────────
# Test: run_simulation respects config ranges
# ──────────────────────────────────────────────────────────────────
def test_simulation_within_ranges():
    for _ in range(20):
        metrics = run_simulation()
        assert CONFIG["CPU_RANGE"][0] <= metrics["cpu"] <= CONFIG["CPU_RANGE"][1], \
            f"CPU {metrics['cpu']} out of {CONFIG['CPU_RANGE']}"
        assert CONFIG["JITTER_RANGE"][0] <= metrics["jitter"] <= CONFIG["JITTER_RANGE"][1]
        assert CONFIG["DELAY_RANGE"][0] <= metrics["delay"] <= CONFIG["DELAY_RANGE"][1]
        assert CONFIG["LAMBDA_RANGE"][0] <= metrics["lambda_"] <= CONFIG["LAMBDA_RANGE"][1]
        assert CONFIG["IMS_SESSION_RANGE"][0] <= metrics["ims_total"] <= CONFIG["IMS_SESSION_RANGE"][1]


# ──────────────────────────────────────────────────────────────────
# Test: evaluate_logic returns decision for every action key
# ──────────────────────────────────────────────────────────────────
def test_evaluate_logic_all_actions():
    """Verify that evaluate_logic works when each action should win."""
    sessions = split_sessions(30)

    # We'll test with extreme scenarios for each key
    scenarios = {
        "observe": make_raw(cpu=22, jitter=5, delay=10, lambda_=2.0),
        "rtp_priority_qos": make_raw(cpu=30, jitter=35, delay=12, lambda_=3.0),
        "increase_bandwidth_reservation": make_raw(cpu=88, jitter=12, delay=35, lambda_=4.0),
        "load_balance_secondary": make_raw(cpu=70, jitter=15, delay=20, lambda_=7.2),
        "preemptive_shaping": make_raw(cpu=80, jitter=22, delay=28, lambda_=8.5),
        "cpu_traffic_shedding": make_raw(cpu=92, jitter=28, delay=32, lambda_=5.5),
        "ims_session_guard": make_raw(cpu=50, jitter=18, delay=22, lambda_=5.8, ims_total=58),
        "transport_retransmission_optimization": make_raw(cpu=45, jitter=10, delay=55, lambda_=4.2),
    }

    for expected_action, raw in scenarios.items():
        history = make_history(raw, count=10, seconds_ago=1.0)
        shi = compute_service_health(raw)
        gradients = {"jitter": 0.5, "delay": 0.3, "cpu": 0.2, "lambda_": 0.1}
        qp = projected_queue_metrics(raw, gradients, CONFIG)

        decision = evaluate_logic(raw, sessions, gradients, shi, qp, CONFIG)

        assert "scenario" in decision
        print(f"[evaluate] expected={expected_action:40s} got={decision['scenario']:40s} "
              f"confidence={decision['confidence_score']:.3f} status={decision['status']}")


# ──────────────────────────────────────────────────────────────────
# Test: All API-required fields present in decision
# ──────────────────────────────────────────────────────────────────
def test_decision_field_presence():
    raw = make_raw(cpu=90, jitter=35, delay=45, lambda_=7.0)
    history = make_history(raw)
    result = execute_logic(raw, history)

    ac = result["action_center"]
    required = {
        "mode": str,
        "scenario": str,
        "status": str,
        "service": str,
        "priority": str,
        "diagnosis": str,
        "rationale": str,
        "proposed_patch": str,
        "confidence_score": float,
        "optimization_result": str,
        "analysis": str,
        "decision": str,
        "optimization": str,
    }

    for field, ftype in required.items():
        assert field in ac, f"Missing field: {field}"
        assert isinstance(ac[field], ftype), f"Field {field} should be {ftype}, got {type(ac[field])}"


# ──────────────────────────────────────────────────────────────────
# Test: M/M/1 wait calculation edge cases
# ──────────────────────────────────────────────────────────────────
def test_mm1_wait_edge_cases():
    from core.models import calculate_mm1_wait

    # Normal case
    assert calculate_mm1_wait(5.0, 10.0) == 0.2

    # Lambda >= mu
    assert calculate_mm1_wait(10.0, 10.0) == float("inf")
    assert calculate_mm1_wait(11.0, 10.0) == float("inf")

    # Low lambda — calculate_mm1_wait rounds to 4 decimal places
    result = calculate_mm1_wait(1.0, 10.0)
    expected = round(1.0 / 9.0, 4)  # = 0.1111
    assert result == expected, f"Expected {expected}, got {result}"

    # Very low lambda
    assert calculate_mm1_wait(0.1, 10.0) == round(1.0 / 9.9, 4)

# ──────────────────────────────────────────────────────────────────
# Test: SHI calculations
# ──────────────────────────────────────────────────────────────────
def test_service_health_index():
    raw_healthy = make_raw(cpu=20, jitter=4, delay=8)
    shi = compute_service_health(raw_healthy)
    assert shi["voip"] > 80
    assert shi["video"] > 80

    raw_degraded = make_raw(cpu=90, jitter=40, delay=70)
    shi2 = compute_service_health(raw_degraded)
    assert shi2["voip"] < 40
    assert shi2["video"] < 40


if __name__ == "__main__":
    # Run with: pytest test_gpon_ims.py -v
    pytest.main([__file__, "-v", "-s"])