"""Per-device sliding-window ingest engine.

Semantics
---------
* Each device has `next_expected`: the next sequence needed for contiguous
  (in-order) delivery. The acceptance window is
  [next_expected, next_expected + window_size).
* seq == next_expected            -> committed, window advances, buffered
                                     run of consecutive sequences drains.
* next_expected < seq < window end -> buffered (暂存) waiting for the gap
                                     ahead of it to fill.
* seq < next_expected              -> duplicate (idempotent discard) if the
                                     record already exists; gap_filled if it
                                     belongs to a declared gap being replayed;
                                     rebase (see below) or stale otherwise.
* seq >= window end                -> out_of_window, rejected; client retries
                                     after the window advances.
* Rebase: if a never-seen sequence lands below next_expected within
  window_size distance and no gap was ever declared for the device, the
  window start was anchored too high (concurrent first batches raced) and
  next_expected is moved down to it. Once a 断档 exists the start is frozen.
* A pending gap (buffered records exist ahead of next_expected) that survives
  longer than gap_timeout_seconds is declared 断档: a gap row is recorded,
  next_expected jumps past the hole, and buffered records drain.

All public methods must be called inside a write transaction (db.write_txn),
so concurrent batches for the same device serialize through SQLite.
"""
from __future__ import annotations

import time
from typing import Iterable, Optional

# Per-record ingest outcomes returned to the caller.
COMMITTED = "committed"
BUFFERED = "buffered"
DUPLICATE = "duplicate"
GAP_FILLED = "gap_filled"
OUT_OF_WINDOW = "out_of_window"
STALE = "stale"


def _to_ranges(seqs: Iterable[int]) -> list[list[int]]:
    """Compress a sorted set of sequences into [[start, end], ...] ranges."""
    seqs = sorted(seqs)
    if not seqs:
        return []
    ranges: list[list[int]] = []
    start = prev = seqs[0]
    for s in seqs[1:]:
        if s == prev + 1:
            prev = s
            continue
        ranges.append([start, prev])
        start = prev = s
    ranges.append([start, prev])
    return ranges


class WindowEngine:
    def __init__(self, conn, window_size: int, gap_timeout_seconds: float):
        self.conn = conn
        self.window_size = window_size
        self.gap_timeout = gap_timeout_seconds

    # ------------------------------------------------------------------ #
    # device row helpers
    # ------------------------------------------------------------------ #
    def get_device(self, device_id: str):
        """Public read of the device row (None if unknown)."""
        return self._get_device(device_id)

    def _get_device(self, device_id: str):
        return self.conn.execute(
            "SELECT * FROM devices WHERE device_id = ?", (device_id,)
        ).fetchone()

    def _create_device(self, device_id: str, start_seq: int, now: float):
        # Bootstrap: the first sequence ever seen defines the window start,
        # so devices are not forced to begin at any fixed number.
        self.conn.execute(
            "INSERT INTO devices (device_id, next_expected, gap_since, "
            "duplicates_count, created_at, updated_at) VALUES (?, ?, NULL, 0, ?, ?)",
            (device_id, start_seq, now, now),
        )
        return self._get_device(device_id)

    def _bump_duplicates(self, device_id: str, now: float) -> None:
        self.conn.execute(
            "UPDATE devices SET duplicates_count = duplicates_count + 1, "
            "updated_at = ? WHERE device_id = ?",
            (now, device_id),
        )

    # ------------------------------------------------------------------ #
    # batch ingest
    # ------------------------------------------------------------------ #
    def process_batch(self, device_id: str, items: list[tuple[int, str]]) -> list[dict]:
        """Ingest (seq, payload_json) pairs; returns per-record outcomes."""
        now = time.time()
        items = sorted(items, key=lambda x: x[0])
        dev = self._get_device(device_id)
        if dev is None:
            dev = self._create_device(device_id, items[0][0], now)
        # Declare any gap whose wait time expired before applying new data.
        self.sweep_device(dev, now)

        results = []
        for seq, payload_json in items:
            dev = self._get_device(device_id)  # state may move every record
            results.append({"seq": seq, "status": self._ingest(dev, seq, payload_json, now)})
        return results

    def _ingest(self, dev, seq: int, payload_json: str, now: float) -> str:
        device_id = dev["device_id"]
        next_expected = dev["next_expected"]

        if seq < next_expected:
            return self._ingest_behind_window(device_id, seq, payload_json, now)
        if seq >= next_expected + self.window_size:
            return OUT_OF_WINDOW

        existing = self.conn.execute(
            "SELECT 1 FROM records WHERE device_id = ? AND seq = ?",
            (device_id, seq),
        ).fetchone()
        if existing is not None:
            self._bump_duplicates(device_id, now)
            return DUPLICATE

        if seq == next_expected:
            self._insert_record(device_id, seq, payload_json, "committed", now)
            self._advance(device_id, now, start=seq + 1)
            return COMMITTED

        self._insert_record(device_id, seq, payload_json, "buffered", now)
        # A gap is now pending; remember when we first saw it.
        self.conn.execute(
            "UPDATE devices SET gap_since = COALESCE(gap_since, ?), updated_at = ? "
            "WHERE device_id = ?",
            (now, now, device_id),
        )
        return BUFFERED

    def _ingest_behind_window(self, device_id: str, seq: int, payload_json: str, now: float) -> str:
        existing = self.conn.execute(
            "SELECT 1 FROM records WHERE device_id = ? AND seq = ?",
            (device_id, seq),
        ).fetchone()
        if existing is not None:
            self._bump_duplicates(device_id, now)
            return DUPLICATE

        gap = self.conn.execute(
            "SELECT * FROM gaps WHERE device_id = ? AND status IN ('open', 'replaying') "
            "AND start_seq <= ? AND end_seq >= ?",
            (device_id, seq, seq),
        ).fetchone()
        if gap is None:
            # Below the window, never recorded, not part of any declared gap.
            if self._can_rebase(device_id, seq):
                # Window was anchored too high (racing first batches); pull
                # the start down and ingest normally.
                self.conn.execute(
                    "UPDATE devices SET next_expected = ?, updated_at = ? WHERE device_id = ?",
                    (seq, now, device_id),
                )
                return self._ingest(self._get_device(device_id), seq, payload_json, now)
            self._bump_duplicates(device_id, now)
            return STALE

        self._insert_record(device_id, seq, payload_json, "gap_filled", now)
        self._maybe_close_gap(gap, now)
        return GAP_FILLED

    def _can_rebase(self, device_id: str, seq: int) -> bool:
        dev = self._get_device(device_id)
        if seq >= dev["next_expected"]:
            return False
        if dev["next_expected"] - seq > self.window_size:
            return False
        any_gap = self.conn.execute(
            "SELECT 1 FROM gaps WHERE device_id = ? LIMIT 1", (device_id,)
        ).fetchone()
        return any_gap is None

    def _insert_record(self, device_id: str, seq: int, payload_json: str, status: str, now: float) -> None:
        self.conn.execute(
            "INSERT INTO records (device_id, seq, payload, status, received_at) "
            "VALUES (?, ?, ?, ?, ?)",
            (device_id, seq, payload_json, status, now),
        )

    def _advance(self, device_id: str, now: float, start: Optional[int] = None) -> None:
        """Move next_expected forward over buffered contiguous records.

        `start` overrides the scan origin: after committing seq N the scan
        begins at N+1; after a gap jump it begins at the new next_expected.
        """
        dev = self._get_device(device_id)
        ne = start if start is not None else dev["next_expected"]
        while True:
            row = self.conn.execute(
                "SELECT status FROM records WHERE device_id = ? AND seq = ?",
                (device_id, ne),
            ).fetchone()
            if row is None:
                break
            if row["status"] == "buffered":
                self.conn.execute(
                    "UPDATE records SET status = 'committed' WHERE device_id = ? AND seq = ?",
                    (device_id, ne),
                )
                ne += 1
            elif row["status"] in ("committed", "gap_filled"):
                ne += 1  # already delivered (possible after a rebase)
            else:  # pragma: no cover - unknown status
                break

        nxt = self.conn.execute(
            "SELECT seq, received_at FROM records WHERE device_id = ? AND status = 'buffered' "
            "ORDER BY seq LIMIT 1",
            (device_id,),
        ).fetchone()
        if nxt is None:
            gap_since = None
        else:
            # The same gap is still pending; keep its original timestamp. If
            # unknown (e.g. right after a gap jump), derive it from the arrival
            # time of the earliest buffered record beyond the hole.
            gap_since = dev["gap_since"] if dev["gap_since"] is not None else nxt["received_at"]
        self.conn.execute(
            "UPDATE devices SET next_expected = ?, gap_since = ?, updated_at = ? "
            "WHERE device_id = ?",
            (ne, gap_since, now, device_id),
        )

    # ------------------------------------------------------------------ #
    # gap timeout sweep (断档 detection)
    # ------------------------------------------------------------------ #
    def sweep_device(self, dev, now: Optional[float] = None) -> None:
        """Declare gaps whose wait time expired. Safe to call any time."""
        now = now if now is not None else time.time()
        device_id = dev["device_id"]
        while True:
            row = self.conn.execute(
                "SELECT seq, received_at FROM records "
                "WHERE device_id = ? AND status = 'buffered' ORDER BY seq LIMIT 1",
                (device_id,),
            ).fetchone()
            if row is None:
                if dev["gap_since"] is not None:
                    self.conn.execute(
                        "UPDATE devices SET gap_since = NULL, updated_at = ? WHERE device_id = ?",
                        (now, device_id),
                    )
                return

            gap_since = dev["gap_since"] if dev["gap_since"] is not None else row["received_at"]
            if now - gap_since < self.gap_timeout:
                if dev["gap_since"] is None:
                    self.conn.execute(
                        "UPDATE devices SET gap_since = ?, updated_at = ? WHERE device_id = ?",
                        (gap_since, now, device_id),
                    )
                return

            # Timeout exceeded -> declare 断档 for [next_expected, min_buffered - 1].
            self.conn.execute(
                "INSERT INTO gaps (device_id, start_seq, end_seq, status, replay_count, "
                "detected_at, updated_at) VALUES (?, ?, ?, 'open', 0, ?, ?)",
                (device_id, dev["next_expected"], row["seq"] - 1, gap_since, now),
            )
            self.conn.execute(
                "UPDATE devices SET next_expected = ?, gap_since = NULL, updated_at = ? "
                "WHERE device_id = ?",
                (row["seq"], now, device_id),
            )
            self._advance(device_id, now)
            dev = self._get_device(device_id)  # loop: a further gap may also be expired

    def sweep_all_devices(self) -> int:
        now = time.time()
        devices = self.conn.execute("SELECT * FROM devices").fetchall()
        for dev in devices:
            self.sweep_device(dev, now)
        return len(devices)

    # ------------------------------------------------------------------ #
    # queries
    # ------------------------------------------------------------------ #
    def window_status(self, device_id: str) -> Optional[dict]:
        now = time.time()
        dev = self._get_device(device_id)
        if dev is None:
            return None
        self.sweep_device(dev, now)  # lazy 断档 detection on read
        dev = self._get_device(device_id)

        buffered = [
            r["seq"]
            for r in self.conn.execute(
                "SELECT seq FROM records WHERE device_id = ? AND status = 'buffered' ORDER BY seq",
                (device_id,),
            )
        ]
        pending_gap = None
        if buffered and buffered[0] > dev["next_expected"]:
            gap_since = dev["gap_since"] if dev["gap_since"] is not None else now
            age = now - gap_since
            pending_gap = {
                "start_seq": dev["next_expected"],
                "end_seq": buffered[0] - 1,
                "age_seconds": round(age, 3),
                "declares_in_seconds": round(max(0.0, self.gap_timeout - age), 3),
            }

        counts = {
            row["status"]: row["c"]
            for row in self.conn.execute(
                "SELECT status, COUNT(*) AS c FROM records WHERE device_id = ? GROUP BY status",
                (device_id,),
            )
        }
        gaps = self.list_gaps(device_id)
        return {
            "device_id": device_id,
            "next_expected": dev["next_expected"],
            "window": {
                "start": dev["next_expected"],
                "end": dev["next_expected"] + self.window_size - 1,
                "size": self.window_size,
            },
            "buffered_sequences": buffered,
            "buffered_count": len(buffered),
            "pending_gap": pending_gap,
            "gaps": gaps,
            "stats": {
                "committed": counts.get("committed", 0),
                "buffered": counts.get("buffered", 0),
                "gap_filled": counts.get("gap_filled", 0),
                "duplicates": dev["duplicates_count"],
            },
        }

    def list_gaps(self, device_id: str, status: Optional[str] = None) -> list[dict]:
        if status:
            rows = self.conn.execute(
                "SELECT * FROM gaps WHERE device_id = ? AND status = ? ORDER BY id",
                (device_id, status),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM gaps WHERE device_id = ? ORDER BY id", (device_id,)
            ).fetchall()
        return [self._gap_dict(r) for r in rows]

    @staticmethod
    def _gap_dict(row) -> dict:
        return {
            "gap_id": row["id"],
            "device_id": row["device_id"],
            "start_seq": row["start_seq"],
            "end_seq": row["end_seq"],
            "missing_count": row["end_seq"] - row["start_seq"] + 1,
            "status": row["status"],
            "replay_count": row["replay_count"],
            "detected_at": row["detected_at"],
            "updated_at": row["updated_at"],
        }

    # ------------------------------------------------------------------ #
    # replay (断档重放)
    # ------------------------------------------------------------------ #
    def request_replay(self, device_id: str, gap_id: int) -> Optional[dict]:
        """Mark a gap as replaying and return the sequences still missing.

        The device then resubmits those sequences through the normal batch
        endpoint; each one lands as gap_filled and the gap auto-closes to
        'recovered' once every missing sequence has arrived.
        """
        now = time.time()
        gap = self.conn.execute(
            "SELECT * FROM gaps WHERE id = ? AND device_id = ?", (gap_id, device_id)
        ).fetchone()
        if gap is None:
            return None
        if gap["status"] == "recovered":
            return {"gap": self._gap_dict(gap), "missing_ranges": [], "missing_count": 0}

        self.conn.execute(
            "UPDATE gaps SET status = 'replaying', replay_count = replay_count + 1, "
            "updated_at = ? WHERE id = ?",
            (now, gap_id),
        )
        filled = {
            r["seq"]
            for r in self.conn.execute(
                "SELECT seq FROM records WHERE device_id = ? AND status = 'gap_filled' "
                "AND seq BETWEEN ? AND ?",
                (device_id, gap["start_seq"], gap["end_seq"]),
            )
        }
        missing = [s for s in range(gap["start_seq"], gap["end_seq"] + 1) if s not in filled]
        gap = self.conn.execute("SELECT * FROM gaps WHERE id = ?", (gap_id,)).fetchone()
        return {
            "gap": self._gap_dict(gap),
            "missing_ranges": _to_ranges(missing),
            "missing_count": len(missing),
        }

    def _maybe_close_gap(self, gap, now: float) -> None:
        filled = self.conn.execute(
            "SELECT COUNT(*) AS c FROM records WHERE device_id = ? AND status = 'gap_filled' "
            "AND seq BETWEEN ? AND ?",
            (gap["device_id"], gap["start_seq"], gap["end_seq"]),
        ).fetchone()["c"]
        if filled >= gap["end_seq"] - gap["start_seq"] + 1:
            self.conn.execute(
                "UPDATE gaps SET status = 'recovered', updated_at = ? WHERE id = ?",
                (now, gap["id"]),
            )
