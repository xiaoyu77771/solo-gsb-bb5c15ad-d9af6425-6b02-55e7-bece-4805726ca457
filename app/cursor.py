"""Opaque, HMAC-signed cursors for consistent keyset pagination.

A cursor captures the full pagination session established by the first
request: the device, the [seq_from, seq_to] range, the rowid high-water
mark (snapshot boundary) and the keyset position (last returned seq).
The payload is JSON, base64url-encoded, and carries an HMAC-SHA256
signature keyed by TELEMETRY_CURSOR_SECRET, so clients cannot forge or
alter a cursor; the server also rejects cursors replayed against a
different device or range.
"""
from __future__ import annotations

import base64
import hashlib
import hmac
import json
from dataclasses import dataclass
from typing import Optional

CURSOR_VERSION = 1


class CursorError(ValueError):
    """Raised when a cursor is malformed or its signature does not verify."""


@dataclass(frozen=True)
class Cursor:
    device_id: str
    seq_from: Optional[int]
    seq_to: Optional[int]
    high_water: int  # rowid cap fixed by the first page; later inserts stay invisible
    last_seq: Optional[int]  # keyset position; None = nothing returned yet
    limit: int


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64decode(text: str) -> bytes:
    padding = "=" * (-len(text) % 4)
    return base64.urlsafe_b64decode(text + padding)


def _sign(secret: str, payload_b64: str) -> str:
    digest = hmac.new(
        secret.encode("utf-8"), payload_b64.encode("ascii"), hashlib.sha256
    ).digest()
    return _b64encode(digest)


def encode_cursor(cursor: Cursor, secret: str) -> str:
    payload = {
        "v": CURSOR_VERSION,
        "dev": cursor.device_id,
        "sf": cursor.seq_from,
        "st": cursor.seq_to,
        "hw": cursor.high_water,
        "ls": cursor.last_seq,
        "lim": cursor.limit,
    }
    payload_b64 = _b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    return f"{payload_b64}.{_sign(secret, payload_b64)}"


def decode_cursor(token: str, secret: str) -> Cursor:
    try:
        payload_b64, signature = token.split(".", 1)
    except ValueError:
        raise CursorError("malformed cursor") from None
    if not hmac.compare_digest(_sign(secret, payload_b64), signature):
        raise CursorError("invalid cursor signature")
    try:
        data = json.loads(_b64decode(payload_b64))
    except (ValueError, UnicodeDecodeError):
        raise CursorError("malformed cursor payload") from None
    try:
        if data["v"] != CURSOR_VERSION:
            raise CursorError("unsupported cursor version")
        device_id = data["dev"]
        seq_from, seq_to, last_seq = data["sf"], data["st"], data["ls"]
        high_water, limit = data["hw"], data["lim"]
        if not isinstance(device_id, str):
            raise TypeError
        for value in (seq_from, seq_to, last_seq):
            if value is not None and not isinstance(value, int):
                raise TypeError
        if not isinstance(high_water, int) or not isinstance(limit, int):
            raise TypeError
    except (KeyError, TypeError) as exc:
        raise CursorError("malformed cursor payload") from exc
    return Cursor(
        device_id=device_id,
        seq_from=seq_from,
        seq_to=seq_to,
        high_water=high_water,
        last_seq=last_seq,
        limit=limit,
    )
