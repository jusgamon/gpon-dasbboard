import threading
import time
from typing import Optional
from core.utils import _log

_ROM_TTL_SECONDS: int = 300

class RomStore:
    def __init__(self, ttl: int = _ROM_TTL_SECONDS) -> None:
        self._lock = threading.Lock()
        self._summary: Optional[dict] = None
        self._written_at: float = 0.0
        self._ttl = ttl
        self._start_ttl_watcher()

    # ------------------------------------------------------------------
    def set(self, summary: dict) -> None:
        with self._lock:
            self._summary = summary
            self._written_at = time.monotonic()

    def get(self) -> Optional[dict]:
        with self._lock:
            return dict(self._summary) if self._summary else None

    def clear(self) -> None:
        with self._lock:
            self._summary = None
            self._written_at = 0.0

    # ------------------------------------------------------------------
    def _start_ttl_watcher(self) -> None:
        def _watcher() -> None:
            while True:
                time.sleep(self._ttl)
                with self._lock:
                    age = time.monotonic() - self._written_at
                    if self._summary and age >= self._ttl:
                        self._summary = None
                        self._written_at = 0.0
                        _log("rom_store", "TTL expired – summary cleared")

        t = threading.Thread(target=_watcher, daemon=True)
        t.start()
