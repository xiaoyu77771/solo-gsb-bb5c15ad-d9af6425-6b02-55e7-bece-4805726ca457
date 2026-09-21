"""Consistent pagination over device records: rowid high-water snapshot,
keyset paging by seq, HMAC-signed opaque cursors."""
from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "test.db"),
        window_size=10_000,
        gap_timeout_seconds=0.2,
        sweep_enabled=False,  # deterministic: gaps are declared lazily on access
        cursor_secret="test-secret",
    )
    with TestClient(create_app(settings)) as c:
        yield c


def submit(client, device, seqs):
    resp = client.post(
        "/api/v1/batches",
        json={"device_id": device, "records": [{"seq": s, "payload": {"v": s}} for s in seqs]},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def get_page(client, device, **params):
    resp = client.get(f"/api/v1/devices/{device}/records", params=params)
    assert resp.status_code == 200, resp.text
    return resp.json()


def walk(client, device, **params):
    """Follow next_cursor to the end; returns (seqs, page_count)."""
    seqs, pages, cursor = [], 0, None
    while True:
        q = dict(params)
        if cursor is not None:
            q["cursor"] = cursor
        body = get_page(client, device, **q)
        pages += 1
        assert pages < 100, "cursor loop did not terminate"
        seqs += [r["seq"] for r in body["records"]]
        cursor = body["next_cursor"]
        if cursor is None:
            return seqs, pages


# --------------------------------------------------------------------- #
# basic paging
# --------------------------------------------------------------------- #

def test_paginates_in_ascending_order(client):
    submit(client, "dev-a", range(1, 11))
    seqs, pages = walk(client, "dev-a", limit=4)
    assert seqs == list(range(1, 11))
    assert pages == 3  # 4 + 4 + 2


def test_first_page_shape_and_null_cursor_at_end(client):
    submit(client, "dev-a", [1, 2])
    body = get_page(client, "dev-a", limit=10)
    assert body["device_id"] == "dev-a"
    assert body["seq_from"] == 0 and body["seq_to"] is None
    assert body["limit"] == 10
    assert body["next_cursor"] is None
    assert [r["seq"] for r in body["records"]] == [1, 2]
    assert body["records"][0]["payload"] == {"v": 1}
    assert body["records"][0]["status"] == "committed"
    assert body["records"][0]["received_at"] > 0


def test_exact_multiple_of_limit_has_null_cursor(client):
    submit(client, "dev-a", [1, 2, 3, 4])
    body = get_page(client, "dev-a", limit=4)
    assert len(body["records"]) == 4
    assert body["next_cursor"] is None


def test_range_filter(client):
    submit(client, "dev-a", range(1, 11))
    seqs, pages = walk(client, "dev-a", seq_from=3, seq_to=7, limit=3)
    assert seqs == [3, 4, 5, 6, 7]
    assert pages == 2


def test_empty_range_returns_empty_page(client):
    submit(client, "dev-a", [1, 2, 3])
    body = get_page(client, "dev-a", seq_from=100, seq_to=200)
    assert body["records"] == []
    assert body["next_cursor"] is None


def test_devices_are_isolated(client):
    submit(client, "dev-a", [1, 2, 3])
    submit(client, "dev-b", [7, 8])
    assert walk(client, "dev-a", limit=2)[0] == [1, 2, 3]
    assert walk(client, "dev-b", limit=2)[0] == [7, 8]


# --------------------------------------------------------------------- #
# snapshot consistency while paging
# --------------------------------------------------------------------- #

def test_new_batches_during_pagination_are_not_mixed_in(client):
    submit(client, "dev-a", [1, 2, 3, 4, 5, 6])
    page1 = get_page(client, "dev-a", limit=4)
    assert [r["seq"] for r in page1["records"]] == [1, 2, 3, 4]

    submit(client, "dev-a", [7, 8, 9])  # arrives after the snapshot froze

    page2 = get_page(client, "dev-a", limit=4, cursor=page1["next_cursor"])
    assert [r["seq"] for r in page2["records"]] == [5, 6]
    assert page2["next_cursor"] is None

    # A fresh session sees everything.
    assert walk(client, "dev-a", limit=4)[0] == list(range(1, 10))


def test_gap_backfill_during_pagination_is_not_mixed_in(client):
    submit(client, "dev-a", [1, 2, 4, 5])  # gap pending at 3
    time.sleep(0.3)  # > gap_timeout_seconds
    client.get("/api/v1/devices/dev-a/window")  # lazy 断档 declaration

    page1 = get_page(client, "dev-a", limit=2)
    assert [r["seq"] for r in page1["records"]] == [1, 2]

    body = submit(client, "dev-a", [3])  # backfill after the snapshot froze
    assert body["results"][0]["status"] == "gap_filled"

    page2 = get_page(client, "dev-a", limit=2, cursor=page1["next_cursor"])
    assert [r["seq"] for r in page2["records"]] == [4, 5]
    assert page2["next_cursor"] is None

    # A fresh session includes the backfilled record.
    assert walk(client, "dev-a", limit=2)[0] == [1, 2, 3, 4, 5]


def test_status_transitions_do_not_duplicate_or_skip(client):
    submit(client, "dev-a", [1, 3])  # 3 buffered behind the hole at 2
    page1 = get_page(client, "dev-a", limit=1)
    assert [r["seq"] for r in page1["records"]] == [1]

    # seq 2 is a brand-new row -> outside the frozen high-water mark.
    # Draining it flips 3 buffered -> committed in place (same rowid),
    # which must not disturb the frozen snapshot set.
    submit(client, "dev-a", [2])

    page2 = get_page(client, "dev-a", limit=1, cursor=page1["next_cursor"])
    assert [r["seq"] for r in page2["records"]] == [3]  # no dup of 1, no skip of 3
    assert page2["records"][0]["status"] == "committed"
    assert page2["next_cursor"] is None

    # The late-arriving seq 2 only shows up in a fresh session.
    assert walk(client, "dev-a", limit=1)[0] == [1, 2, 3]


# --------------------------------------------------------------------- #
# cursor validation
# --------------------------------------------------------------------- #

def test_tampered_cursor_rejected(client):
    submit(client, "dev-a", range(1, 6))
    cursor = get_page(client, "dev-a", limit=2)["next_cursor"]
    assert cursor

    # Flip one character inside the signed payload.
    flip = "A" if cursor[10] != "A" else "B"
    tampered = cursor[:10] + flip + cursor[11:]
    resp = client.get("/api/v1/devices/dev-a/records", params={"cursor": tampered})
    assert resp.status_code == 400

    resp = client.get("/api/v1/devices/dev-a/records", params={"cursor": "not-a-cursor"})
    assert resp.status_code == 400


def test_cursor_bound_to_device(client):
    submit(client, "dev-a", range(1, 6))
    submit(client, "dev-b", range(1, 6))
    cursor = get_page(client, "dev-a", limit=2)["next_cursor"]
    resp = client.get("/api/v1/devices/dev-b/records", params={"cursor": cursor})
    assert resp.status_code == 400


def test_cursor_rejected_when_range_changes(client):
    submit(client, "dev-a", range(1, 11))
    cursor = get_page(client, "dev-a", seq_from=2, seq_to=8, limit=3)["next_cursor"]
    assert cursor

    # Repeating the same range alongside the cursor is fine.
    ok = client.get("/api/v1/devices/dev-a/records",
                    params={"cursor": cursor, "seq_from": 2, "seq_to": 8})
    assert ok.status_code == 200

    for bad in ({"cursor": cursor, "seq_from": 3},
                {"cursor": cursor, "seq_to": 9},
                {"cursor": cursor, "seq_from": 1, "seq_to": 8}):
        resp = client.get("/api/v1/devices/dev-a/records", params=bad)
        assert resp.status_code == 400, bad


def test_cursor_signed_with_another_secret_rejected(tmp_path):
    def make_client(secret):
        settings = Settings(
            db_path=str(tmp_path / f"{secret}.db"),
            window_size=100,
            sweep_enabled=False,
            cursor_secret=secret,
        )
        return TestClient(create_app(settings))

    with make_client("secret-one") as c1, make_client("secret-two") as c2:
        submit(c1, "dev-a", range(1, 6))
        submit(c2, "dev-a", range(1, 6))
        cursor = get_page(c1, "dev-a", limit=3)["next_cursor"]
        resp = c2.get("/api/v1/devices/dev-a/records", params={"cursor": cursor})
        assert resp.status_code == 400


# --------------------------------------------------------------------- #
# request validation
# --------------------------------------------------------------------- #

def test_unknown_device_404(client):
    assert client.get("/api/v1/devices/nope/records").status_code == 404


def test_inverted_range_rejected(client):
    submit(client, "dev-a", [1])
    resp = client.get("/api/v1/devices/dev-a/records",
                      params={"seq_from": 9, "seq_to": 3})
    assert resp.status_code == 400


def test_limit_bounds_validated(client):
    submit(client, "dev-a", [1])
    assert client.get("/api/v1/devices/dev-a/records", params={"limit": 0}).status_code == 422
    assert client.get("/api/v1/devices/dev-a/records", params={"limit": 1001}).status_code == 422
    assert client.get("/api/v1/devices/dev-a/records", params={"seq_from": -1}).status_code == 422
