"""Consistent pagination tests for GET /api/v1/devices/{device_id}/records."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


def make_client(tmp_path, **overrides):
    kwargs = dict(
        db_path=str(tmp_path / "test.db"),
        window_size=5,
        gap_timeout_seconds=0.25,
        sweep_enabled=False,  # deterministic: gaps are declared lazily on access
        cursor_secret="test-secret",
    )
    kwargs.update(overrides)
    return TestClient(create_app(Settings(**kwargs)))


@pytest.fixture()
def client(tmp_path):
    with make_client(tmp_path) as c:
        yield c


def submit(client, device, seqs):
    resp = client.post(
        "/api/v1/batches",
        json={"device_id": device, "records": [{"seq": s, "payload": {"v": s}} for s in seqs]},
    )
    assert resp.status_code == 200, resp.text
    return resp.json()


def fetch_all(client, device, **params):
    """Follow next_cursor to the end; return (all records, per-page responses)."""
    records, pages = [], []
    cursor = None
    while True:
        q = dict(params) if cursor is None else {"cursor": cursor}
        resp = client.get(f"/api/v1/devices/{device}/records", params=q)
        assert resp.status_code == 200, resp.text
        body = resp.json()
        pages.append(body)
        records.extend(body["records"])
        cursor = body["next_cursor"]
        if cursor is None:
            return records, pages


# --------------------------------------------------------------------- #
# basic keyset pagination
# --------------------------------------------------------------------- #

def test_paginates_all_records_exactly_once(client):
    submit(client, "dev-a", list(range(1, 11)))
    records, pages = fetch_all(client, "dev-a", limit=3)
    assert [r["seq"] for r in records] == list(range(1, 11))  # ascending, no dup/miss
    assert [len(p["records"]) for p in pages] == [3, 3, 3, 1]
    assert pages[-1]["next_cursor"] is None
    assert all(p["next_cursor"] for p in pages[:-1])
    assert [r["payload"] for r in records] == [{"v": s} for s in range(1, 11)]


def test_exact_multiple_of_limit_has_no_trailing_empty_page(client):
    submit(client, "dev-a", [1, 2, 3, 4])
    records, pages = fetch_all(client, "dev-a", limit=2)
    assert [r["seq"] for r in records] == [1, 2, 3, 4]
    assert len(pages) == 2
    assert pages[-1]["next_cursor"] is None


def test_default_limit_and_single_page(client):
    submit(client, "dev-a", [1, 2, 3])
    body = client.get("/api/v1/devices/dev-a/records").json()
    assert [r["seq"] for r in body["records"]] == [1, 2, 3]
    assert body["next_cursor"] is None


def test_range_filter(client):
    submit(client, "dev-a", list(range(1, 11)))
    records, _ = fetch_all(client, "dev-a", seq_from=3, seq_to=7, limit=2)
    assert [r["seq"] for r in records] == [3, 4, 5, 6, 7]


def test_open_ended_range(client):
    submit(client, "dev-a", list(range(1, 6)))
    records, _ = fetch_all(client, "dev-a", seq_from=4, limit=10)
    assert [r["seq"] for r in records] == [4, 5]


def test_empty_range_yields_empty_page_and_null_cursor(client):
    submit(client, "dev-a", [10, 11])
    body = client.get("/api/v1/devices/dev-a/records", params={"seq_from": 1, "seq_to": 5}).json()
    assert body == {"device_id": "dev-a", "records": [], "next_cursor": None}


def test_unknown_device_404(client):
    assert client.get("/api/v1/devices/nope/records").status_code == 404


def test_inverted_range_400(client):
    submit(client, "dev-a", [1])
    resp = client.get("/api/v1/devices/dev-a/records", params={"seq_from": 5, "seq_to": 2})
    assert resp.status_code == 400


# --------------------------------------------------------------------- #
# snapshot consistency while paging
# --------------------------------------------------------------------- #

def test_new_batches_during_pagination_are_invisible(client):
    submit(client, "dev-a", [1, 2, 3, 4, 5])
    page1 = client.get("/api/v1/devices/dev-a/records", params={"limit": 2}).json()
    assert [r["seq"] for r in page1["records"]] == [1, 2]

    submit(client, "dev-a", [6, 7])  # arrives after the snapshot was pinned

    rest, _ = fetch_all(client, "dev-a", cursor=page1["next_cursor"])
    assert [r["seq"] for r in rest] == [3, 4, 5]  # 6 and 7 never leak in


def test_gap_backfill_during_pagination_is_invisible(client):
    submit(client, "dev-a", [1, 2, 4, 5])  # gap pending at 3
    import time

    time.sleep(0.3)
    gaps = client.get("/api/v1/devices/dev-a/gaps").json()["gaps"]  # lazy 断档 declaration
    assert [(g["start_seq"], g["end_seq"]) for g in gaps] == [(3, 3)]

    page1 = client.get("/api/v1/devices/dev-a/records", params={"limit": 2}).json()
    assert [r["seq"] for r in page1["records"]] == [1, 2]

    body = submit(client, "dev-a", [3])  # 断档补录 lands after the high-water mark
    assert body["results"][0]["status"] == "gap_filled"

    rest, _ = fetch_all(client, "dev-a", cursor=page1["next_cursor"])
    assert [r["seq"] for r in rest] == [4, 5]  # backfilled seq 3 stays out of the set


def test_in_place_status_change_does_not_lose_rows(tmp_path):
    # buffered -> committed drains update rows in place (rowid unchanged);
    # they must still appear on later pages of the same snapshot.
    with make_client(tmp_path, gap_timeout_seconds=600) as client:
        submit(client, "dev-a", [1, 3])  # 3 buffered behind a pending gap
        page1 = client.get("/api/v1/devices/dev-a/records", params={"limit": 1}).json()
        assert [r["seq"] for r in page1["records"]] == [1]

        submit(client, "dev-a", [2])  # fills the hole, drains 3 to committed

        rest, _ = fetch_all(client, "dev-a", cursor=page1["next_cursor"])
        assert [r["seq"] for r in rest] == [3]  # not lost; seq 2 is new, stays out
        assert rest[0]["status"] == "committed"


def test_range_is_pinned_across_pages(client):
    submit(client, "dev-a", list(range(1, 11)))
    page1 = client.get(
        "/api/v1/devices/dev-a/records", params={"seq_from": 2, "seq_to": 9, "limit": 4}
    ).json()
    assert [r["seq"] for r in page1["records"]] == [2, 3, 4, 5]
    submit(client, "dev-a", [11, 12])
    # cursor alone reproduces the original range; no need to repeat params
    rest, _ = fetch_all(client, "dev-a", cursor=page1["next_cursor"])
    assert [r["seq"] for r in rest] == [6, 7, 8, 9]


# --------------------------------------------------------------------- #
# cursor integrity
# --------------------------------------------------------------------- #

def first_cursor(client, device="dev-a", **params):
    submit(client, device, [1, 2, 3])
    body = client.get(f"/api/v1/devices/{device}/records", params={"limit": 1, **params}).json()
    assert body["next_cursor"] is not None
    return body["next_cursor"]


def test_tampered_cursor_400(client):
    cursor = first_cursor(client)
    payload, sig = cursor.split(".")
    tampered_payload = payload[:-1] + ("A" if payload[-1] != "A" else "B")
    resp = client.get("/api/v1/devices/dev-a/records", params={"cursor": f"{tampered_payload}.{sig}"})
    assert resp.status_code == 400

    resp = client.get("/api/v1/devices/dev-a/records", params={"cursor": "not-a-cursor"})
    assert resp.status_code == 400


def test_cursor_signed_with_other_secret_400(client, tmp_path):
    cursor = first_cursor(client)
    with make_client(tmp_path / "other", cursor_secret="other-secret") as other:
        submit(other, "dev-a", [1, 2, 3])
        resp = other.get("/api/v1/devices/dev-a/records", params={"cursor": cursor})
        assert resp.status_code == 400


def test_cross_device_cursor_400(client):
    cursor = first_cursor(client, device="dev-a")
    submit(client, "dev-b", [1, 2, 3])
    resp = client.get("/api/v1/devices/dev-b/records", params={"cursor": cursor})
    assert resp.status_code == 400


def test_changed_range_with_cursor_400(client):
    submit(client, "dev-a", list(range(1, 11)))
    page1 = client.get(
        "/api/v1/devices/dev-a/records", params={"seq_from": 2, "seq_to": 9, "limit": 4}
    ).json()
    cursor = page1["next_cursor"]

    for mismatch in ({"seq_from": 3}, {"seq_to": 8}, {"limit": 2}):
        resp = client.get("/api/v1/devices/dev-a/records", params={"cursor": cursor, **mismatch})
        assert resp.status_code == 400, mismatch

    # repeating the identical parameters explicitly is accepted
    resp = client.get(
        "/api/v1/devices/dev-a/records",
        params={"cursor": cursor, "seq_from": 2, "seq_to": 9, "limit": 4},
    )
    assert resp.status_code == 200
    assert [r["seq"] for r in resp.json()["records"]] == [6, 7, 8, 9]
