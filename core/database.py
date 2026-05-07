# core/database.py
import sqlite3
from core.config import DB_PATH

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

def clear_db() -> None:
    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("DELETE FROM metrics")
        conn.commit()

    with sqlite3.connect(DB_PATH) as conn:
        conn.execute("VACUUM")
        conn.commit()