"""Runtime configuration, overridable via environment variables."""
from __future__ import annotations

import os
from dataclasses import dataclass


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    return raw.strip().lower() in ("1", "true", "yes", "on")


@dataclass(frozen=True)
class Settings:
    # SQLite database file location.
    db_path: str = "/data/telemetry.db"
    # Sliding window size per device: sequences in
    # [next_expected, next_expected + window_size) are accepted/buffered.
    window_size: int = 1000
    # How long (seconds) a gap may stay open before it is declared 断档.
    gap_timeout_seconds: float = 30.0
    # Background sweeper interval (seconds) that declares expired gaps
    # even when no traffic arrives.
    sweep_interval_seconds: float = 5.0
    # Set to False to disable the background sweeper (tests).
    sweep_enabled: bool = True

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            db_path=os.getenv("TELEMETRY_DB_PATH", "/data/telemetry.db"),
            window_size=int(os.getenv("TELEMETRY_WINDOW_SIZE", "1000")),
            gap_timeout_seconds=float(os.getenv("TELEMETRY_GAP_TIMEOUT_SECONDS", "30")),
            sweep_interval_seconds=float(os.getenv("TELEMETRY_SWEEP_INTERVAL_SECONDS", "5")),
            sweep_enabled=_env_bool("TELEMETRY_SWEEP_ENABLED", True),
        )


# Process-wide default, used by `uvicorn app.main:app`.
settings = Settings.from_env()
