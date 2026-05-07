# core/config.py
INITIAL_CONFIG: dict = {
    "MU": 10.0,
    "PUSH_INTERVAL": 2,
    "DATA_SOURCE": "simulate",
    "CSV_PATH": "data.csv",
    "CPU_RANGE": (8.0, 92.0),
    "JITTER_RANGE": (2.0, 45.0),
    "DELAY_RANGE": (5.0, 80.0),
    "LAMBDA_RANGE": (0.5, 7.5),
    "IMS_SESSION_RANGE": (8, 60),
    "W_DELAY": 0.4,
    "W_JITTER": 0.3,
    "W_CPU": 0.3,
    "SERVICE_WEIGHTS": {"voip": 0.40, "video": 0.35, "web": 0.25},
    "SIMULATION_CHAOS": 0.22,
    "SIMULATION_MOMENTUM": 0.78,
    "VOIP_JITTER_THRESHOLD": 20.0,
    "VIDEO_DELAY_THRESHOLD": 28.0,
    "CPU_WARNING_THRESHOLD": 85.0,
    "LAMBDA_WARNING_RATIO": 0.70,
    "ACTION_MODE": "auto",
    "FORCED_ACTION": "observe",
    "FORCED_STATUS": "Normal",
}

CONFIG: dict = INITIAL_CONFIG.copy();

_UPDATABLE_KEYS = frozenset(CONFIG.keys())
_RANGE_KEYS = frozenset({
    "CPU_RANGE", "JITTER_RANGE", "DELAY_RANGE",
    "LAMBDA_RANGE", "IMS_SESSION_RANGE",
})

_BOOTSTRAP = {
    "cpu": 23.0,
    "jitter": 5.7,
    "delay": 11.5,
    "lambda_": 2.5,
    "ims_total": 18,
}

DB_PATH = "network_metrics.db"

def reset_config ():
    global CONFIG

    CONFIG.clear()
    CONFIG.update(INITIAL_CONFIG)