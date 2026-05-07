# core/actions.py
import math
import time
from core.config import CONFIG
from core.models import classify_status, severity_rank, calculate_qos

def _score_observe(raw, sessions, gradients, shi, qp, cfg):
    return 0.15

def _score_rtp_priority(raw, sessions, gradients, shi, qp, cfg):
    if sessions["voip"] == 0:
        return 0.0
    total = max(sum(sessions.values()), 1)
    voip_load = sessions["voip"] / total
    threshold = cfg["VOIP_JITTER_THRESHOLD"]
    if raw["jitter"] <= threshold and shi["voip"] >= 60.0:
        return 0.0
    jitter_ex = max(0.0, raw["jitter"] - threshold)
    shi_stress = max(0.0, 60.0 - shi["voip"]) / 40.0
    return (
        jitter_ex * 0.25
        + max(0.0, gradients["jitter"]) * 1.2
        + shi_stress * 1.8
        + voip_load * min(1.0, jitter_ex / 12.0) * 1.5
    )

def _score_bandwidth_reservation(raw, sessions, gradients, shi, qp, cfg):
    if sessions["video"] == 0:
        return 0.0
    total = max(sum(sessions.values()), 1)
    video_load = sessions["video"] / total
    delay_ex = max(0.0, raw["delay"] - cfg["VIDEO_DELAY_THRESHOLD"])
    cpu_ex = max(0.0, raw["cpu"] - cfg["CPU_WARNING_THRESHOLD"])
    shi_stress = max(0.0, 70.0 - shi["video"]) / 30.0
    degraded = raw["delay"] > cfg["VIDEO_DELAY_THRESHOLD"] or raw["cpu"] > cfg["CPU_WARNING_THRESHOLD"] or shi["video"] < 70.0
    if not degraded:
        return 0.0
    return (
        delay_ex * 0.4
        + cpu_ex * 0.3
        + max(0.0, gradients["delay"]) * 1.0
        + shi_stress * 1.5
        + video_load * 0.8
    )

def _score_load_balance(raw, sessions, gradients, shi, qp, cfg):
    lambda_ratio = raw["lambda_"] / max(cfg["MU"], 0.001)
    cpu_ex = max(0.0, raw["cpu"] - cfg["CPU_WARNING_THRESHOLD"])
    lambda_ex = max(0.0, lambda_ratio - cfg["LAMBDA_WARNING_RATIO"])
    if cpu_ex == 0.0 and lambda_ex == 0.0:
        return 0.0
    return (
        cpu_ex * 0.18
        + lambda_ex * 8.0
        + max(0.0, gradients["lambda_"]) * 1.5
        + (2.5 if qp["time_to_saturation_s"] <= 30 and raw["lambda_"] < cfg["MU"] else 0.0)
    )

def _score_preemptive_shaping(raw, sessions, gradients, shi, qp, cfg):
    projected_wq_ms = qp["projected_wq_30s_ms"]
    cpu_ex = max(0.0, raw["cpu"] - cfg["CPU_WARNING_THRESHOLD"])
    if not math.isfinite(projected_wq_ms) or projected_wq_ms < 30.0:
        if cpu_ex == 0.0:
            return 0.0
    wq_score = 0.0
    if math.isfinite(projected_wq_ms) and projected_wq_ms >= 30.0:
        wq_score = (projected_wq_ms - 30.0) / 100.0
    return (
        wq_score * 1.5
        + cpu_ex * 0.2
        + max(0.0, gradients["lambda_"]) * 1.2
        + (1.0 if qp["time_to_saturation_s"] <= 30 and raw["lambda_"] < cfg["MU"] else 0.0)
    )

def _score_cpu_shedding(raw, sessions, gradients, shi, qp, cfg):
    cpu_ex = max(0.0, raw["cpu"] - cfg["CPU_WARNING_THRESHOLD"])
    if raw["cpu"] < 88.0 or (raw["cpu"] < cfg["CPU_WARNING_THRESHOLD"] and gradients["cpu"] <= 0.0):
        return 0.0
    return (
        cpu_ex * 0.15
        + max(0.0, gradients["cpu"]) * 2.0
        + max(0.0, gradients["lambda_"]) * 1.2
        + (2.0 if raw["cpu"] >= 92.0 else 0.0)
    )

def _score_session_guard(raw, sessions, gradients, shi, qp, cfg):
    max_sessions = max(cfg["IMS_SESSION_RANGE"][1], 1)
    load_factor = raw["ims_total"] / max_sessions
    if load_factor < 0.75:
        return 0.0
    lambda_ratio = raw["lambda_"] / max(cfg["MU"], 0.001)
    return (
        (load_factor - 0.70) * 5.0
        + max(0.0, lambda_ratio - 0.65) * 4.0
        + max(0.0, gradients["lambda_"]) * 1.2
        + (2.0 if qp["time_to_saturation_s"] <= 25 else 0.0)
    )

def _score_transport_retransmission(raw, sessions, gradients, shi, qp, cfg):
    delay_threshold = cfg["VIDEO_DELAY_THRESHOLD"]
    if raw["delay"] <= delay_threshold:
        return 0.0
    delay_ex = raw["delay"] - delay_threshold
    delay_dominance = max(0.0, gradients["delay"] - gradients["jitter"])
    return (
        delay_ex * 0.3
        + delay_dominance * 2.0
        + max(0.0, gradients["lambda_"]) * 0.9
        + (1.0 if raw["delay"] >= 50.0 else 0.0)
    )

ACTION_REGISTRY: dict = {
    "observe": {
        "service": "Bütün xidmətlər",
        "priority": "AŞAĞI",
        "patch": "Həll tələb olunmur. Əsas nəqliyyat siyasətini aktiv saxlayın.",
        "diagnosis": "Cari telemetriya intervalında dominant xidmət problemi müşahidə olunmur.",
        "score_fn": _score_observe,
    },
    "rtp_priority_qos": {
        "service": "VoIP",
        "priority": "YÜKSƏK",
        "patch": "cli: qos policy update class VOIP set dscp ef queue strict-priority",
        "diagnosis": "Aktiv səs yükü zamanı IMS VoIP sessiyalarında artan jitter problemi müşahidə olunur.",
        "score_fn": _score_rtp_priority,
    },
    "increase_bandwidth_reservation": {
        "service": "Video",
        "priority": "ORTA",
        "patch": "config: ims.video.reservation=+15% and gpon.tcont.video.assured_bw=boost",
        "diagnosis": "CPU yüklənməsi və gecikmə artımı səbəbindən IMS video sessiyalarının keyfiyyəti zəifləyir.",
        "score_fn": _score_bandwidth_reservation,
    },
    "load_balance_secondary": {
        "service": "Nəqliyyat",
        "priority": "YÜKSƏK",
        "patch": "cli: orchestrator rebalance --target secondary-vnf --drain best-effort 20%",
        "diagnosis": "Trafik intensivliyi xidmət tutumuna yaxınlaşdığı üçün nəqliyyat yükü artır.",
        "score_fn": _score_load_balance,
    },
    "preemptive_shaping": {
        "service": "Nəqliyyat",
        "priority": "YÜKSƏK",
        "patch": "cli: traffic-shaper apply profile preemptive_guard --window 30s",
        "diagnosis": "Növbə artımı kritik limitlərə çatmadan əvvəl doyma vəziyyətinə yaxınlaşır.",
        "score_fn": _score_preemptive_shaping,
    },
    "cpu_traffic_shedding": {
        "service": "Compute",
        "priority": "YÜKSƏK",
        "patch": "cli: traffic-policy apply low-priority-shedding --threshold cpu>85",
        "diagnosis": "CPU resursları kritik həddə yaxınlaşdığı üçün aşağı prioritet trafik məhdudlaşdırılır.",
        "score_fn": _score_cpu_shedding,
    },
    "ims_session_guard": {
        "service": "IMS Core",
        "priority": "ORTA",
        "patch": "cli: ims session-guard enable --limit adaptive",
        "diagnosis": "IMS sessiya sayı nəqliyyat və xidmət tutumuna yaxınlaşır.",
        "score_fn": _score_session_guard,
    },
    "transport_retransmission_optimization": {
        "service": "Transport",
        "priority": "ORTA",
        "patch": "cli: transport optimize retransmission-window --adaptive",
        "diagnosis": "Artan gecikmə nəqliyyat səviyyəsində retransmissiya və congestion problemlərinə işarə edir.",
        "score_fn": _score_transport_retransmission,
    },
}

_decision_state: dict = {
    "last_action": None,
    "last_result": "Hələ heç bir əməliyyat qiymətləndirilməyib.",
}

def reset_decision_state() -> None:
    _decision_state["last_action"] = None
    _decision_state["last_result"] = "Hələ heç bir əməliyyat qiymətləndirilməyib."

def _score_to_status(score: float) -> str:
    if score >= 1.80:
        return "Critical"
    if score >= 0.75:
        return "Warning"
    return "Normal"

def _compute_all_scores(raw, sessions, gradients, shi, queue_proj, cfg):
    return {
        key: round(action["score_fn"](raw, sessions, gradients, shi, queue_proj, cfg), 4)
        for key, action in ACTION_REGISTRY.items()
    }

def _apply_hysteresis(candidate: str, candidate_status: str) -> tuple:
    last = _decision_state["last_action"]
    if not last or (time.time() - last["applied_at"]) > 15:
        return candidate, candidate_status
    if candidate == last["scenario"] or severity_rank(candidate_status) >= severity_rank("Critical"):
        return candidate, candidate_status
    return last["scenario"], last["status"]

def _assess_closed_loop(action_key: str, raw: dict, shi: dict, queue_proj: dict) -> str:
    last = _decision_state["last_action"]
    if not last or last["scenario"] != action_key:
        return _decision_state["last_result"]
    if (time.time() - last["applied_at"]) < 6:
        return "Əməliyyat tətbiq edildi; dəyişiklikdən sonrakı telemetriya gözlənilir."
    b = last["baseline"]
    improved = 0
    if action_key == "rtp_priority_qos":
        improved += int(raw["jitter"] <= b["jitter"])
        improved += int(shi["voip"] >= b["shi_voip"])
    elif action_key == "increase_bandwidth_reservation":
        improved += int(raw["delay"] <= b["delay"])
        improved += int(shi["video"] >= b["shi_video"])
    else:
        improved += int(raw["lambda_"] <= b["lambda_"])
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
                    confidence: float, opt_result: str, cfg: dict) -> dict:
    action = ACTION_REGISTRY[action_key]
    total = sum(sessions.values())
    lambda_ratio = raw["lambda_"] / max(cfg["MU"], 0.001)
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
        "mode": mode,
        "scenario": action_key,
        "status": status,
        "service": action["service"],
        "priority": action["priority"],
        "diagnosis": action["diagnosis"],
        "rationale": rationale,
        "proposed_patch": action["patch"],
        "confidence_score": round(confidence, 2),
        "optimization_result": opt_result,
        "analysis": f"IMS sessiyaları cəmi={total}, VoIP={sessions['voip']}, Video={sessions['video']}.",
        "decision": action["diagnosis"],
        "optimization": action["patch"],
    }

def resolve_action(raw: dict, sessions: dict, gradients: dict,
                   shi: dict, queue_proj: dict, current_status: str,
                   cfg: dict = CONFIG) -> dict:
    opt_result = _decision_state["last_result"]
    if cfg["ACTION_MODE"] == "manual":
        return _build_decision(
            cfg["FORCED_ACTION"], cfg["FORCED_STATUS"], "manual",
            raw, sessions, gradients, shi, queue_proj,
            confidence=0.95, opt_result=opt_result, cfg=cfg,
        )
    scores = _compute_all_scores(raw, sessions, gradients, shi, queue_proj, cfg)
    qos = calculate_qos(raw["delay"], raw["jitter"], raw["cpu"])
    if qos >= 85 and current_status == "Normal":
        for key in scores:
            if key != "observe":
                scores[key] = 0.0
    action_key, top_score = max(scores.items(), key=lambda kv: kv[1])
    if action_key != "observe" and top_score < 0.75:
        action_key, top_score = "observe", scores["observe"]
    confidence = min(0.99, 0.35 + top_score / 5.0)
    status = _score_to_status(top_score)
    if action_key == "observe":
        status = current_status if (current_status != "Normal" and top_score > 0.5) else "Normal"
    action_key, status = _apply_hysteresis(action_key, status)
    opt_result = _assess_closed_loop(action_key, raw, shi, queue_proj)
    if action_key != "observe":
        last = _decision_state["last_action"]
        if last is None or last["scenario"] != action_key:
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
                    "projected_wq_30s_ms": queue_proj["projected_wq_30s_ms"],
                },
            }
        else:
            last["status"] = status
    return _build_decision(
        action_key, status, "auto",
        raw, sessions, gradients, shi, queue_proj,
        confidence=confidence, opt_result=opt_result, cfg=cfg,
    )

def evaluate_logic(raw: dict, sessions: dict, gradients: dict,
                   shi: dict, queue_proj: dict, cfg: dict = CONFIG) -> dict:
    scores = _compute_all_scores(raw, sessions, gradients, shi, queue_proj, cfg)
    qos = calculate_qos(raw["delay"], raw["jitter"], raw["cpu"])
    current_status = classify_status(qos)
    if qos >= 85 and current_status == "Normal":
        for key in scores:
            if key != "observe":
                scores[key] = 0.0
    action_key, top_score = max(scores.items(), key=lambda kv: kv[1])
    if action_key != "observe" and top_score < 0.75:
        action_key = "observe"
    confidence = min(0.99, 0.35 + top_score / 5.0)
    status = _score_to_status(top_score)
    if action_key == "observe":
        status = current_status
    opt_result = "Statik analiz – real vaxt qapalı dövrə yoxdur."
    return _build_decision(
        action_key, status, "external",
        raw, sessions, gradients, shi, queue_proj,
        confidence=confidence, opt_result=opt_result, cfg=cfg,
    )