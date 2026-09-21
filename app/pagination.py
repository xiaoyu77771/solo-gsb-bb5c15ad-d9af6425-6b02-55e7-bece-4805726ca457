"""Consistent keyset pagination over the append-only records table.

Consistency model
-----------------
The first page of a query freezes a high-water mark: the current maximum
SQLite rowid among the device's records. Every page of the same query is
then filtered with ``rowid <= high_water``, so records inserted *after*
the first page was served — new batches as well as gap backfills
(断档补录) — are never mixed into this pagination session, while the
frozen set is read with no duplicates and no misses. Records are
append-only (never deleted, never re-keyed; status flips like
buffered -> committed keep the rowid), which keeps the snapshot stable.

Pages are keyset-paginated by ascending ``seq`` (unique per device via
the PRIMARY KEY), so the ``seq > last_seq`` predicate resumes exactly
where the previous page stopped, regardless of concurrent inserts.

The opaque cursor carries the query range, the high-water mark and the
keyset position, HMAC-SHA256 signed with the configured secret
(TELEMETRY_CURSOR_SECRET). Tampering, use on another device, or reuse
with a changed range is rejected with HTTP 400 by the endpoint.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Optional


class CursorError(ValueError):
    """Raised when a cursor is malformed or its signature does not verify."""


@dataclass(frozen=True)
class Cursor:
    device_id: str
    seq_from: int
    seq_to: Optional[int]
    high_water: int  # rowid upper bound frozen by the first page
    last_seq: int    # keyset position: last seq returned to the client


# --------------------------------------------------------------------- #
# opaque cursor codec (HMAC-SHA256 signed)
# --------------------------------------------------------------------- #

def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64decode(data: str) -> bytes:
    return base64.urlsafe_b64decode(data + "=" * (-len(data) % 4))


def _signature(body: str, secret: str) -> bytes:
    return hmac.new(secret.encode("utf-8"), body.encode("ascii"), hashlib.sha256).digest()


def encode_cursor(cursor: Cursor, secret: str) -> str:
    payload = json.dumps(
        {
            "v": 1,
            "dev": cursor.device_id,
            "sf": cursor.seq_from,
            "st": cursor.seq_to,
            "hw": cursor.high_water,
            "ls": cursor.last_seq,
        },
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    body = _b64encode(payload)
    return f"v1.{body}.{_b64encode(_signature(body, secret))}"


def decode_cursor(token: str, secret: str) -> Cursor:
    parts = token.split(".")
    if len(parts) != 3 or parts[0] != "v1":
        raise CursorError("malformed cursor")
    _, body, sig = parts
    try:
        provided = _b64decode(sig)
    except Exception as exc:
        raise CursorError("malformed cursor") from exc
    if not hmac.compare_digest(_signature(body, secret), provided):
        raise CursorError("cursor signature mismatch")
    try:
        data = json.loads(_b64decode(body))
    except Exception as exc:
        raise CursorError("malformed cursor payload") from exc
    if not isinstance(data, dict):
        raise CursorError("malformed cursor payload")
    try:
        cursor = Cursor(
            device_id=data["dev"],
            seq_from=data["sf"],
            seq_to=data["st"],
            high_water=data["hw"],
            last_seq=data["ls"],
        )
    except KeyError as exc:
        raise CursorError("malformed cursor payload") from exc
    if (
        not isinstance(cursor.device_id, str)
        or not _is_int(cursor.seq_from)
        or not (cursor.seq_to is None or _is_int(cursor.seq_to))
        or not _is_int(cursor.high_water)
        or not _is_int(cursor.last_seq)
    ):
        raise CursorError("malformed cursor payload")
    return cursor


def _is_int(value) -> bool:
    # bool is a subclass of int; it is never a valid cursor field.
    return isinstance(value, int) and not isinstance(value, bool)


# --------------------------------------------------------------------- #
# page fetch
# --------------------------------------------------------------------- #

def high_water_mark(conn, device_id: str) -> int:
    """Current max rowid among the device's records (0 when none exist)."""
    row = conn.execute(
        "SELECT COALESCE(MAX(rowid), 0) AS hw FROM records WHERE device_id = ?",
        (device_id,),
    ).fetchone()
    return row["hw"]


def fetch_page(conn, device_id: str, seq_to: Optional[int], high_water: int,
               last_seq: int, limit: int):
    """Return (rows, has_more): up to `limit` rows strictly after `last_seq`.

    `limit + 1` rows are read so a short read unambiguously means the
    frozen set is exhausted and the next cursor must be null.
    """
    sql = (
        "SELECT seq, payload, status, received_at FROM records "
        "WHERE device_id = ? AND rowid <= ? AND seq > ?"
    )
    params: list = [device_id, high_water, last_seq]
    if seq_to is not None:
        sql += " AND seq <= ?"
        params.append(seq_to)
    sql += " ORDER BY seq ASC LIMIT ?"
    params.append(limit + 1)
    rows = conn.execute(sql, params).fetchall()
    has_more = len(rows) > limit
    return rows[:limit], has_more
