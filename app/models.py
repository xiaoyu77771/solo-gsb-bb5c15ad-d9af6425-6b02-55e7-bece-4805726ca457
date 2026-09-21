"""Request/response schemas for the HTTP API."""
from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field


class RecordIn(BaseModel):
    seq: int = Field(ge=0, description="Monotonically increasing per-device sequence number")
    payload: dict[str, Any] = Field(default_factory=dict, description="Telemetry payload")


class BatchIn(BaseModel):
    device_id: str = Field(min_length=1, max_length=128)
    batch_id: Optional[str] = Field(
        default=None,
        description="Optional client-side batch identifier (echoed back; retries are "
        "safe because every sequence is idempotent on its own)",
    )
    records: list[RecordIn] = Field(min_length=1, max_length=10_000)


class RecordResult(BaseModel):
    seq: int
    status: str  # committed | buffered | duplicate | gap_filled | out_of_window | stale


class WindowSummary(BaseModel):
    next_expected: int
    buffered_count: int


class BatchResponse(BaseModel):
    device_id: str
    batch_id: Optional[str]
    accepted: int
    results: list[RecordResult]
    window: WindowSummary


class RecordOut(BaseModel):
    seq: int
    payload: dict[str, Any]
    status: str  # committed | buffered | gap_filled
    received_at: float


class RecordPage(BaseModel):
    device_id: str
    seq_from: int = Field(description="Effective lower bound (inclusive) of this query")
    seq_to: Optional[int] = Field(description="Effective upper bound (inclusive); null = unbounded")
    limit: int
    records: list[RecordOut]
    next_cursor: Optional[str] = Field(
        default=None,
        description="Opaque cursor for the next page; null when this is the last page",
    )
