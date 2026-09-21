"""FastAPI application: device telemetry ingest with per-device sliding windows.

Concurrency model: endpoints are plain `def` (run in a threadpool); each
request gets its own SQLite connection and mutates state inside a
BEGIN IMMEDIATE transaction, so concurrent batches serialize cleanly.
A background sweeper declares expired gaps (断档) even when no traffic arrives.
"""
from __future__ import annotations

import asyncio
import json
import logging
import time
from contextlib import asynccontextmanager

from fastapi import Depends, FastAPI, HTTPException, Query

from .config import Settings
from .db import connect, init_db, write_txn
from .models import BatchIn, BatchResponse, RecordResult, WindowSummary
from .window import WindowEngine

logger = logging.getLogger("telemetry")

API_PREFIX = "/api/v1"


def create_app(settings: Settings) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        init_db(settings.db_path)
        stop = asyncio.Event()
        sweeper = None
        if settings.sweep_enabled:
            sweeper = asyncio.create_task(_sweeper_loop(stop))
        logger.info(
            "telemetry API ready: db=%s window_size=%d gap_timeout=%ss",
            settings.db_path, settings.window_size, settings.gap_timeout_seconds,
        )
        yield
        stop.set()
        if sweeper is not None:
            await sweeper

    app = FastAPI(
        title="Device Telemetry Ingest API",
        version="1.0.0",
        description="Local telemetry receiver with per-device sliding-window "
        "ordering, idempotent dedup, gap (断档) detection and replay.",
        lifespan=lifespan,
    )
    app.state.settings = settings

    # -------------------------------------------------------------- #
    # helpers
    # -------------------------------------------------------------- #
    def get_conn():
        conn = connect(settings.db_path)
        try:
            yield conn
        finally:
            conn.close()

    def engine(conn) -> WindowEngine:
        return WindowEngine(conn, settings.window_size, settings.gap_timeout_seconds)

    async def _sweeper_loop(stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                await asyncio.to_thread(_sweep_all)
            except Exception:  # pragma: no cover - defensive
                logger.exception("background sweep failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=settings.sweep_interval_seconds)
            except asyncio.TimeoutError:
                pass

    def _sweep_all() -> None:
        conn = connect(settings.db_path)
        try:
            with write_txn(conn):
                engine(conn).sweep_all_devices()
        finally:
            conn.close()

    # -------------------------------------------------------------- #
    # meta endpoints
    # -------------------------------------------------------------- #
    @app.get("/health")
    def health():
        return {"status": "ok", "time": time.time()}

    @app.get("/")
    def root():
        return {
            "service": "device-telemetry-ingest",
            "docs": "/docs",
            "api": API_PREFIX,
            "config": {
                "window_size": settings.window_size,
                "gap_timeout_seconds": settings.gap_timeout_seconds,
                "sweep_interval_seconds": settings.sweep_interval_seconds,
            },
        }

    # -------------------------------------------------------------- #
    # batch submission
    # -------------------------------------------------------------- #
    @app.post(f"{API_PREFIX}/batches", response_model=BatchResponse)
    def submit_batch(batch: BatchIn, conn=Depends(get_conn)):
        eng = engine(conn)
        items = [(r.seq, json.dumps(r.payload, ensure_ascii=False)) for r in batch.records]
        with write_txn(conn):
            results = eng.process_batch(batch.device_id, items)
            dev = conn.execute(
                "SELECT next_expected FROM devices WHERE device_id = ?", (batch.device_id,)
            ).fetchone()
            buffered = conn.execute(
                "SELECT COUNT(*) AS c FROM records WHERE device_id = ? AND status = 'buffered'",
                (batch.device_id,),
            ).fetchone()["c"]
        accepted = sum(1 for r in results if r["status"] in ("committed", "buffered", "gap_filled"))
        return BatchResponse(
            device_id=batch.device_id,
            batch_id=batch.batch_id,
            accepted=accepted,
            results=[RecordResult(**r) for r in results],
            window=WindowSummary(next_expected=dev["next_expected"], buffered_count=buffered),
        )

    # -------------------------------------------------------------- #
    # window state
    # -------------------------------------------------------------- #
    @app.get(f"{API_PREFIX}/devices")
    def list_devices(conn=Depends(get_conn)):
        rows = conn.execute(
            "SELECT device_id, next_expected, gap_since, duplicates_count, updated_at "
            "FROM devices ORDER BY device_id"
        ).fetchall()
        return {"devices": [dict(r) for r in rows]}

    @app.get(f"{API_PREFIX}/devices/{{device_id}}/window")
    def window_status(device_id: str, conn=Depends(get_conn)):
        eng = engine(conn)
        with write_txn(conn):  # read may lazily declare an expired gap
            status = eng.window_status(device_id)
        if status is None:
            raise HTTPException(status_code=404, detail=f"unknown device: {device_id}")
        return status

    # -------------------------------------------------------------- #
    # gaps & replay
    # -------------------------------------------------------------- #
    @app.get(f"{API_PREFIX}/devices/{{device_id}}/gaps")
    def list_gaps(
        device_id: str,
        status: str | None = Query(default=None, pattern="^(open|replaying|recovered)$"),
        conn=Depends(get_conn),
    ):
        eng = engine(conn)
        with write_txn(conn):
            dev = eng.get_device(device_id)
            if dev is None:
                raise HTTPException(status_code=404, detail=f"unknown device: {device_id}")
            eng.sweep_device(dev)
            return {"device_id": device_id, "gaps": eng.list_gaps(device_id, status)}

    @app.post(f"{API_PREFIX}/devices/{{device_id}}/gaps/{{gap_id}}/replay")
    def replay_gap(device_id: str, gap_id: int, conn=Depends(get_conn)):
        eng = engine(conn)
        with write_txn(conn):
            result = eng.request_replay(device_id, gap_id)
        if result is None:
            raise HTTPException(status_code=404, detail=f"gap {gap_id} not found for {device_id}")
        return result

    return app


# Default app instance for `uvicorn app.main:app`.
from . import config  # noqa: E402

app = create_app(config.settings)
