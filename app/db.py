"""SQLite access layer.

One connection per request (created by the FastAPI dependency in main.py).
WAL mode + busy_timeout allow concurrent readers and serialized writers;
all mutations happen inside BEGIN IMMEDIATE transactions (see write_txn),
which keeps per-device window state consistent under concurrent requests.
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS devices (
    device_id        TEXT PRIMARY KEY,
    next_expected    INTEGER NOT NULL,          -- next contiguous sequence expected
    gap_since        REAL,                      -- when the current pending gap was first seen
    duplicates_count INTEGER NOT NULL DEFAULT 0,
    created_at       REAL NOT NULL,
    updated_at       REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS records (
    device_id   TEXT NOT NULL,
    seq         INTEGER NOT NULL,
    payload     TEXT NOT NULL,                  -- JSON payload as received
    status      TEXT NOT NULL CHECK (status IN ('committed', 'buffered', 'gap_filled')),
    received_at REAL NOT NULL,
    PRIMARY KEY (device_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_records_device_status ON records(device_id, status);

CREATE TABLE IF NOT EXISTS gaps (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id    TEXT NOT NULL,
    start_seq    INTEGER NOT NULL,
    end_seq      INTEGER NOT NULL,
    status       TEXT NOT NULL CHECK (status IN ('open', 'replaying', 'recovered')),
    replay_count INTEGER NOT NULL DEFAULT 0,
    detected_at  REAL NOT NULL,                 -- when the gap was first observed
    updated_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_gaps_device ON gaps(device_id, status);
"""


def connect(db_path: str) -> sqlite3.Connection:
    # check_same_thread=False: FastAPI may run a request's dependency setup,
    # endpoint and teardown on different threadpool threads. Each connection
    # is still confined to a single request, and BEGIN IMMEDIATE serializes
    # writers, so this stays safe.
    conn = sqlite3.connect(db_path, timeout=10.0, isolation_level=None,
                           check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=10000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db(db_path: str) -> None:
    parent = os.path.dirname(db_path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def write_txn(conn: sqlite3.Connection):
    """Exclusive write transaction; serializes writers at the SQLite level."""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")
