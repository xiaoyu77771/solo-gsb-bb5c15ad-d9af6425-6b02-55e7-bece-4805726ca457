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
from .models import BatchIn, BatchResponse, RecordOut, RecordPage, RecordResult, WindowSummary
from .pagination import (
    Cursor,
    CursorError,
    decode_cursor,
    encode_cursor,
    fetch_page,
    high_water_mark,
)
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
    # consistent records pagination
    # -------------------------------------------------------------- #
    @app.get(f"{API_PREFIX}/devices/{{device_id}}/records", response_model=RecordPage)
    def list_records(
        device_id: str,
        seq_from: int | None = Query(default=None, ge=0),
        seq_to: int | None = Query(default=None, ge=0),
        limit: int = Query(default=100, ge=1, le=1000),
        cursor: str | None = Query(default=None),
        conn=Depends(get_conn),
    ):
        dev = conn.execute(
            "SELECT 1 FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()
        if dev is None:
            raise HTTPException(status_code=404, detail=f"unknown device: {device_id}")

        if cursor is not None:
            try:
                cur = decode_cursor(cursor, settings.cursor_secret)
            except CursorError as exc:
                raise HTTPException(status_code=400, detail=str(exc)) from exc
            if cur.device_id != device_id:
                raise HTTPException(status_code=400,
                                    detail="cursor was issued for a different device")
            if seq_from is not None and seq_from != cur.seq_from:
                raise HTTPException(status_code=400,
                                    detail="seq_from conflicts with the cursor's range")
            if seq_to is not None and seq_to != cur.seq_to:
                raise HTTPException(status_code=400,
                                    detail="seq_to conflicts with the cursor's range")
            range_from, range_to = cur.seq_from, cur.seq_to
            high_water, last_seq = cur.high_water, cur.last_seq
        else:
            range_from = seq_from if seq_from is not None else 0
            range_to = seq_to
            if range_to is not None and range_from > range_to:
                raise HTTPException(status_code=400, detail="seq_from must be <= seq_to")
            # Freeze the snapshot *before* reading the page: rows inserted
            # afterwards get higher rowids and stay outside this session.
            high_water = high_water_mark(conn, device_id)
            last_seq = range_from - 1

        rows, has_more = fetch_page(conn, device_id, range_to, high_water, last_seq, limit)
        next_cursor = None
        if has_more:
            next_cursor = encode_cursor(
                Cursor(device_id=device_id, seq_from=range_from, seq_to=range_to,
                       high_water=high_water, last_seq=rows[-1]["seq"]),
                settings.cursor_secret,
            )
        return RecordPage(
            device_id=device_id,
            seq_from=range_from,
            seq_to=range_to,
            limit=limit,
            records=[
                RecordOut(seq=r["seq"], payload=json.loads(r["payload"]),
                          status=r["status"], received_at=r["received_at"])
                for r in rows
            ],
            next_cursor=next_cursor,
        )

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
