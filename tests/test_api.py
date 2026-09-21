"""End-to-end tests: ordering, idempotency, gap timeout, replay, concurrency."""
from __future__ import annotations

import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        db_path=str(tmp_path / "test.db"),
        window_size=5,
        gap_timeout_seconds=0.25,
        sweep_enabled=False,  # deterministic: gaps are declared lazily on access
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


def statuses(body):
    return {r["seq"]: r["status"] for r in body["results"]}


# --------------------------------------------------------------------- #
# ordering & idempotency
# --------------------------------------------------------------------- #

def test_in_order_commit(client):
    body = submit(client, "dev-a", [1, 2, 3])
    assert statuses(body) == {1: "committed", 2: "committed", 3: "committed"}
    assert body["window"]["next_expected"] == 4


def test_out_of_order_buffered_then_drained(client):
    body = submit(client, "dev-a", [1, 3, 4])
    st = statuses(body)
    assert st[1] == "committed"
    assert st[3] == "buffered"
    assert st[4] == "buffered"

    win = client.get("/api/v1/devices/dev-a/window").json()
    assert win["next_expected"] == 2
    assert win["buffered_sequences"] == [3, 4]
    assert win["pending_gap"]["start_seq"] == 2
    assert win["pending_gap"]["end_seq"] == 2

    body = submit(client, "dev-a", [2])
    assert statuses(body)[2] == "committed"
    win = client.get("/api/v1/devices/dev-a/window").json()
    assert win["next_expected"] == 5  # 3 and 4 drained
    assert win["buffered_sequences"] == []
    assert win["pending_gap"] is None


def test_duplicate_is_idempotent(client):
    submit(client, "dev-a", [1, 2, 3])
    body = submit(client, "dev-a", [2, 3])  # retry
    assert statuses(body) == {2: "duplicate", 3: "duplicate"}
    win = client.get("/api/v1/devices/dev-a/window").json()
    assert win["stats"]["committed"] == 3
    assert win["stats"]["duplicates"] == 2


def test_duplicate_within_same_batch(client):
    body = submit(client, "dev-a", [1, 1, 2])
    sts = [r["status"] for r in body["results"]]
    assert sts.count("committed") == 2
    assert sts.count("duplicate") == 1


def test_out_of_window_rejected(client):
    body = submit(client, "dev-a", [1, 7])  # window_size=5 -> [2,6] acceptable
    st = statuses(body)
    assert st[1] == "committed"
    assert st[7] == "out_of_window"
    # retry once the window advances far enough
    submit(client, "dev-a", [2, 3, 4, 5])
    body = submit(client, "dev-a", [7])
    assert statuses(body)[7] in ("committed", "buffered")


def test_device_bootstraps_at_first_seen_seq(client):
    body = submit(client, "dev-new", [100, 101])
    assert statuses(body) == {100: "committed", 101: "committed"}
    win = client.get("/api/v1/devices/dev-new/window").json()
    assert win["next_expected"] == 102


# --------------------------------------------------------------------- #
# gap timeout (断档) & replay
# --------------------------------------------------------------------- #

def test_gap_declared_after_timeout(client):
    submit(client, "dev-a", [1, 2, 4, 5])  # gap at seq 3
    time.sleep(0.3)  # > gap_timeout_seconds (0.25)

    win = client.get("/api/v1/devices/dev-a/window").json()  # lazy sweep
    assert win["next_expected"] == 6  # jumped past the hole, 4/5 drained
    assert len(win["gaps"]) == 1
    gap = win["gaps"][0]
    assert gap["start_seq"] == 3 and gap["end_seq"] == 3
    assert gap["status"] == "open"
    assert win["pending_gap"] is None


def test_gap_not_declared_before_timeout(client):
    submit(client, "dev-a", [1, 3])
    win = client.get("/api/v1/devices/dev-a/window").json()
    assert win["gaps"] == []
    assert win["pending_gap"]["start_seq"] == 2
    assert win["pending_gap"]["declares_in_seconds"] > 0


def test_replay_flow_recovers_gap(client):
    submit(client, "dev-a", [1, 2, 4, 6])  # gaps pending at 3 and 5
    time.sleep(0.3)
    gaps = client.get("/api/v1/devices/dev-a/gaps").json()["gaps"]
    assert [(g["start_seq"], g["end_seq"]) for g in gaps] == [(3, 3), (5, 5)]

    replay = client.post("/api/v1/devices/dev-a/gaps/1/replay").json()
    assert replay["gap"]["status"] == "replaying"
    assert replay["missing_ranges"] == [[3, 3]]

    body = submit(client, "dev-a", [3])
    assert statuses(body)[3] == "gap_filled"

    gaps = client.get("/api/v1/devices/dev-a/gaps").json()["gaps"]
    by_id = {g["gap_id"]: g for g in gaps}
    assert by_id[1]["status"] == "recovered"
    assert by_id[2]["status"] == "open"

    win = client.get("/api/v1/devices/dev-a/window").json()
    assert win["stats"]["gap_filled"] == 1


def test_replay_multi_seq_gap_partial_fill(client):
    submit(client, "dev-a", [1, 5])  # gap 2..4
    time.sleep(0.3)
    gaps = client.get("/api/v1/devices/dev-a/gaps").json()["gaps"]
    assert (gaps[0]["start_seq"], gaps[0]["end_seq"]) == (2, 4)

    replay = client.post("/api/v1/devices/dev-a/gaps/1/replay").json()
    assert replay["missing_ranges"] == [[2, 4]]
    assert replay["missing_count"] == 3

    submit(client, "dev-a", [2, 4])  # partial fill
    replay = client.post("/api/v1/devices/dev-a/gaps/1/replay").json()
    assert replay["missing_ranges"] == [[3, 3]]
    assert replay["gap"]["status"] == "replaying"

    submit(client, "dev-a", [3])
    gap = client.get("/api/v1/devices/dev-a/gaps").json()["gaps"][0]
    assert gap["status"] == "recovered"


def test_stale_sequence_below_window(client):
    submit(client, "dev-a", [10, 11])  # bootstrap at 10
    body = submit(client, "dev-a", [5])  # never recorded, no gap covers it
    assert statuses(body)[5] == "stale"


def test_window_rebases_when_anchored_too_high(client):
    # A racing first batch anchored the window at 4; lower sequences that
    # arrive shortly after (within window_size, no gap declared) pull the
    # window start down instead of being lost as stale.
    submit(client, "dev-a", [4, 5])
    body = submit(client, "dev-a", [1, 2, 3])
    assert statuses(body) == {1: "committed", 2: "committed", 3: "committed"}
    win = client.get("/api/v1/devices/dev-a/window").json()
    assert win["next_expected"] == 6
    assert win["stats"]["committed"] == 5


def test_no_rebase_after_gap_declared(client):
    submit(client, "dev-a", [10, 12])  # gap pending at 11
    time.sleep(0.3)
    client.get("/api/v1/devices/dev-a/window")  # declares gap [11,11]
    body = submit(client, "dev-a", [8])  # within window distance but frozen
    assert statuses(body)[8] == "stale"


def test_unknown_device_404(client):
    assert client.get("/api/v1/devices/nope/window").status_code == 404
    assert client.get("/api/v1/devices/nope/gaps").status_code == 404
    assert client.post("/api/v1/devices/nope/gaps/1/replay").status_code == 404


# --------------------------------------------------------------------- #
# concurrency: real server, parallel batch submissions
# --------------------------------------------------------------------- #

def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def test_concurrent_submissions_keep_state_consistent(tmp_path):
    import uvicorn

    settings = Settings(
        db_path=str(tmp_path / "conc.db"),
        window_size=10_000,
        gap_timeout_seconds=600,  # no 断档 during the test
        sweep_enabled=False,
    )
    port = _free_port()
    config = uvicorn.Config(create_app(settings), host="127.0.0.1", port=port, log_level="error")
    server = uvicorn.Server(config)
    thread = threading.Thread(target=server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                httpx.get(f"{base}/health", timeout=0.2)
                break
            except Exception:
                time.sleep(0.05)
        else:
            pytest.fail("server did not start")

        threads, chunk, total = 8, 25, 200  # seqs 1..200, sharded across threads

        def worker(shard: int):
            with httpx.Client(base_url=base, timeout=10) as c:
                for start in range(shard * chunk + 1, (shard + 1) * chunk + 1, 5):
                    seqs = list(range(start, min(start + 5, (shard + 1) * chunk + 1)))
                    r = c.post("/api/v1/batches", json={
                        "device_id": "dev-conc",
                        "records": [{"seq": s, "payload": {"v": s}} for s in seqs],
                    })
                    assert r.status_code == 200, r.text

        with ThreadPoolExecutor(max_workers=threads) as pool:
            list(pool.map(worker, range(threads)))

        with httpx.Client(base_url=base, timeout=10) as c:
            win = c.get("/api/v1/devices/dev-conc/window").json()
            assert win["next_expected"] == total + 1
            assert win["stats"]["committed"] == total
            assert win["buffered_count"] == 0
            assert win["gaps"] == []

            # Full replay of every sequence must be a pure no-op (idempotent).
            r = c.post("/api/v1/batches", json={
                "device_id": "dev-conc",
                "records": [{"seq": s, "payload": {"v": s}} for s in range(1, total + 1)],
            })
            assert all(x["status"] == "duplicate" for x in r.json()["results"])
            win = c.get("/api/v1/devices/dev-conc/window").json()
            assert win["stats"]["committed"] == total
    finally:
        server.should_exit = True
        thread.join(timeout=5)
