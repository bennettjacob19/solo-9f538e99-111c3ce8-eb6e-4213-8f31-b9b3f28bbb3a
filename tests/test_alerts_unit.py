"""In-process unit tests for the alert outbox, dispatcher and receiver.

These need no network services.  They drive ``app.admit`` /
``app.record_outcome`` / ``app.dispatch_pending`` against a temporary SQLite
file with a fake receiver, and exercise ``receiver.py`` through its ASGI app.
"""

from __future__ import annotations

import importlib
import sys
import threading
import time
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


@pytest.fixture()
def gw(tmp_path, monkeypatch):
    monkeypatch.setenv("GATEWAY_DB", str(tmp_path / "breaker.db"))
    monkeypatch.setenv("REQUEST_TIMEOUT", "1.0")
    monkeypatch.setenv("COOLDOWN", "2.0")
    monkeypatch.setenv("FAILURE_THRESHOLD", "3")
    monkeypatch.setenv("UPSTREAM_URL", "http://127.0.0.1:1")
    monkeypatch.setenv("RECEIVER_URL", "http://receiver.test")
    import app as app_module

    importlib.reload(app_module)
    app_module.init_db()
    return app_module


@pytest.fixture()
def rcv(tmp_path, monkeypatch):
    monkeypatch.setenv("RECEIVER_DB", str(tmp_path / "receiver.db"))
    import receiver as receiver_module

    importlib.reload(receiver_module)
    receiver_module.init_db()
    return receiver_module


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _trip(gw, artifact="a"):
    fail = gw.Outcome("failure", "boom 503")
    for _ in range(3):
        adm = gw.admit(artifact)
        assert adm.allowed
        gw.record_outcome(artifact, adm, fail)


def _recover(gw, artifact="a"):
    _force_cooldown_over(gw, artifact)
    adm = gw.admit(artifact)
    assert adm.role == "probe"
    gw.record_outcome(artifact, adm, gw.Outcome("success", "200"))


def _force_cooldown_over(gw, artifact="a"):
    conn = gw.connect()
    try:
        conn.execute(
            "UPDATE breakers SET opened_at = opened_at - 10 WHERE artifact = ?", (artifact,)
        )
    finally:
        conn.close()


def _events(gw, artifact="a"):
    conn = gw.connect()
    try:
        return [
            dict(r)
            for r in conn.execute(
                "SELECT * FROM alert_outbox WHERE artifact = ? ORDER BY id", (artifact,)
            ).fetchall()
        ]
    finally:
        conn.close()


def _force_due(gw):
    conn = gw.connect()
    try:
        conn.execute("UPDATE alert_outbox SET next_attempt_at = 0 WHERE delivered_at IS NULL")
    finally:
        conn.close()


class FakeReceiver:
    """In-memory stand-in for the receiver's POST /alerts endpoint."""

    def __init__(self):
        self.records: dict[str, dict] = {}
        self.accepting = True
        self.calls: list[str] = []

    def post(self, payload):
        self.calls.append(payload["event_id"])
        if not self.accepting:
            return False, "receiver HTTP 503: receiver_rejecting"
        rec = self.records.setdefault(payload["event_id"], {**payload, "deliveries": 0})
        rec["deliveries"] += 1
        return True, ""


@pytest.fixture()
def fake_receiver(gw, monkeypatch):
    fake = FakeReceiver()
    monkeypatch.setattr(gw, "_post_event", lambda payload: fake.post(payload))
    return fake


# ---------------------------------------------------------------------------
# event generation
# ---------------------------------------------------------------------------


def test_trip_enqueues_single_opened_event(gw):
    _trip(gw)
    events = _events(gw)
    assert len(events) == 1
    ev = events[0]
    assert ev["kind"] == "opened"
    assert ev["fault_gen"] == 1
    assert "boom 503" in ev["reason"]
    assert ev["delivered_at"] is None
    # Rejected requests while open must not add events.
    assert not gw.admit("a").allowed
    assert len(_events(gw)) == 1


def test_concurrent_failures_enqueue_exactly_one_event(gw):
    fail = gw.Outcome("failure", "boom")

    def worker():
        adm = gw.admit("a")
        if adm.allowed:
            gw.record_outcome("a", adm, fail)

    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    opened = [e for e in _events(gw) if e["kind"] == "opened"]
    assert len(opened) == 1


def test_failed_probe_stays_in_same_fault_no_new_event(gw):
    _trip(gw)
    _force_cooldown_over(gw)
    adm = gw.admit("a")
    assert adm.role == "probe"
    gw.record_outcome("a", adm, gw.Outcome("failure", "still down"))
    events = _events(gw)
    assert len(events) == 1
    assert events[0]["kind"] == "opened"


def test_probe_success_enqueues_recovery_with_same_generation(gw):
    _trip(gw)
    _recover(gw)
    events = _events(gw)
    assert [e["kind"] for e in events] == ["opened", "recovered"]
    assert events[0]["fault_gen"] == events[1]["fault_gen"] == 1


def test_second_fault_uses_next_generation(gw):
    _trip(gw)
    _recover(gw)
    _trip(gw)
    kinds = [(e["kind"], e["fault_gen"]) for e in _events(gw)]
    assert kinds == [("opened", 1), ("recovered", 1), ("opened", 2)]


def test_reset_enqueues_reset_event_for_faulted_artifacts_only(gw):
    _trip(gw, "a")
    adm = gw.admit("b")  # healthy artifact stays closed
    gw.record_outcome("b", adm, gw.Outcome("success", "200"))
    gw._reset()
    assert [(e["kind"], e["fault_gen"]) for e in _events(gw, "a")] == [
        ("opened", 1),
        ("reset", 1),
    ]
    assert _events(gw, "b") == []
    # The fault counter survives the reset: the next trip is generation 2.
    _trip(gw, "a")
    assert _events(gw, "a")[-1]["fault_gen"] == 2


def test_reset_without_fault_generates_no_event(gw):
    adm = gw.admit("a")
    gw.record_outcome("a", adm, gw.Outcome("success", "200"))
    gw._reset()
    assert _events(gw) == []


def test_stale_results_generate_no_events(gw):
    _trip(gw)
    _force_cooldown_over(gw)
    slow = gw.admit("a")  # probe generation 1, abandoned
    assert slow.role == "probe"
    conn = gw.connect()
    try:
        conn.execute(
            "UPDATE breakers SET state='open', opened_at=opened_at-10, probe_holder=NULL "
            "WHERE artifact='a'"
        )
    finally:
        conn.close()
    newer = gw.admit("a")
    gw.record_outcome("a", newer, gw.Outcome("failure", "still down"))
    result = gw.record_outcome("a", slow, gw.Outcome("success", "200"))
    assert result["effect"] == "stale_probe_ignored"
    # Only the original trip produced an event; the late old-generation
    # result produced nothing.
    assert [e["kind"] for e in _events(gw)] == ["opened"]


# ---------------------------------------------------------------------------
# dispatcher
# ---------------------------------------------------------------------------


def test_dispatcher_delivers_and_marks_delivered(gw, fake_receiver):
    _trip(gw)
    gw.dispatch_pending()
    ev = _events(gw)[0]
    assert ev["delivered_at"] is not None
    assert ev["attempts"] == 1
    assert list(fake_receiver.records) == [ev["event_id"]]
    payload = fake_receiver.records[ev["event_id"]]
    assert payload["artifact"] == "a"
    assert payload["fault_gen"] == 1
    assert payload["kind"] == "opened"
    assert "boom 503" in payload["reason"]


def test_receiver_rejection_keeps_event_pending_until_accepted(gw, fake_receiver):
    fake_receiver.accepting = False
    _trip(gw)
    gw.dispatch_pending()
    ev = _events(gw)[0]
    assert ev["delivered_at"] is None
    assert ev["attempts"] == 1
    assert ev["next_attempt_at"] > time.time()
    assert "503" in ev["last_error"]
    # Backoff has not elapsed: not due, no new attempt.
    gw.dispatch_pending()
    assert _events(gw)[0]["attempts"] == 1
    # Receiver accepts again: the same event is delivered.
    fake_receiver.accepting = True
    _force_due(gw)
    gw.dispatch_pending()
    ev = _events(gw)[0]
    assert ev["delivered_at"] is not None
    assert len(fake_receiver.records) == 1
    assert fake_receiver.records[ev["event_id"]]["deliveries"] == 1


def test_recovery_cannot_overtake_pending_trip(gw, fake_receiver):
    fake_receiver.accepting = False
    _trip(gw)
    _recover(gw)
    assert [e["kind"] for e in _events(gw)] == ["opened", "recovered"]
    for _ in range(3):
        gw.dispatch_pending()
    # Only the head event was ever attempted; recovery never jumped the queue.
    assert fake_receiver.calls
    assert {eid.split(":")[-1] for eid in fake_receiver.calls} == {"opened"}
    # Receiver recovers: both events are delivered in occurrence order.
    fake_receiver.accepting = True
    _force_due(gw)
    gw.dispatch_pending()
    assert [eid.split(":")[-1] for eid in fake_receiver.records] == ["opened", "recovered"]
    assert all(e["delivered_at"] is not None for e in _events(gw))


def test_lost_ack_redelivers_same_event_id(gw, monkeypatch):
    """Receiver stored the event but the ack was lost: retry, same record."""
    fake = FakeReceiver()
    state = {"flaked": False}

    def flaky(payload):
        if not state["flaked"]:
            state["flaked"] = True
            rec = fake.records.setdefault(payload["event_id"], {**payload, "deliveries": 0})
            rec["deliveries"] += 1
            return False, "connection reset after store"  # ack lost on the wire
        return fake.post(payload)

    monkeypatch.setattr(gw, "_post_event", flaky)
    _trip(gw)
    gw.dispatch_pending()  # stored by the receiver, ack lost -> still pending
    assert _events(gw)[0]["delivered_at"] is None
    _force_due(gw)
    gw.dispatch_pending()  # retry hits the receiver's idempotent store
    assert _events(gw)[0]["delivered_at"] is not None
    assert len(fake.records) == 1
    assert next(iter(fake.records.values()))["deliveries"] == 2


def test_lease_blocks_second_dispatcher(gw, fake_receiver):
    _trip(gw)
    conn = gw.connect()
    try:
        conn.execute(
            "UPDATE dispatcher_lease SET holder = ?, expires_at = ? WHERE id = 1",
            ("other-worker", time.time() + 60),
        )
    finally:
        conn.close()
    gw.dispatch_pending()
    assert fake_receiver.calls == []
    assert _events(gw)[0]["delivered_at"] is None
    # The other worker's lease expires (it died): this worker takes over.
    conn = gw.connect()
    try:
        conn.execute("UPDATE dispatcher_lease SET expires_at = 0 WHERE id = 1")
    finally:
        conn.close()
    gw.dispatch_pending()
    assert len(fake_receiver.records) == 1


def test_per_artifact_progress_is_independent(gw, monkeypatch):
    fake = FakeReceiver()
    monkeypatch.setattr(gw, "_post_event", lambda payload: fake.post(payload))
    _trip(gw, "a")
    _trip(gw, "b")

    def selective(payload):
        if payload["artifact"] == "a":
            return False, "boom"
        return fake.post(payload)

    monkeypatch.setattr(gw, "_post_event", selective)
    gw.dispatch_pending()
    assert _events(gw, "a")[0]["delivered_at"] is None
    assert _events(gw, "b")[0]["delivered_at"] is not None


def test_pending_events_survive_process_restart(gw, fake_receiver, tmp_path, monkeypatch):
    """Events committed before a restart are delivered by the new instance."""
    fake_receiver.accepting = False
    _trip(gw)
    gw.dispatch_pending()
    assert _events(gw)[0]["delivered_at"] is None

    # Simulate a gateway restart: re-import the module against the same DB.
    import app as app_module

    old_instance = app_module.INSTANCE_ID
    importlib.reload(app_module)
    assert app_module.INSTANCE_ID != old_instance
    app_module.init_db()
    monkeypatch.setattr(app_module, "_post_event", lambda payload: fake_receiver.post(payload))
    fake_receiver.accepting = True
    _force_due(app_module)
    # The old instance's dispatcher lease is still held; once it expires (the
    # process is gone), the new instance takes over and delivers.
    conn = app_module.connect()
    try:
        conn.execute("UPDATE dispatcher_lease SET expires_at = 0 WHERE id = 1")
    finally:
        conn.close()
    app_module.dispatch_pending()
    assert len(fake_receiver.records) == 1
    conn = app_module.connect()
    try:
        row = conn.execute("SELECT * FROM alert_outbox").fetchone()
        assert row["delivered_at"] is not None
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# receiver service
# ---------------------------------------------------------------------------


def _payload(event_id="a#1:opened", artifact="a", kind="opened", gen=1):
    return {
        "event_id": event_id,
        "artifact": artifact,
        "fault_gen": gen,
        "kind": kind,
        "reason": "boom",
        "occurred_at": time.time(),
    }


def test_receiver_dedupes_by_event_id(rcv):
    from starlette.testclient import TestClient

    with TestClient(rcv.app) as client:
        r1 = client.post("/alerts", json=_payload())
        assert r1.status_code == 200 and r1.json()["duplicate"] is False
        r2 = client.post("/alerts", json=_payload())
        assert r2.status_code == 200 and r2.json()["duplicate"] is True
        listed = client.get("/alerts").json()
        assert listed["count"] == 1
        rec = listed["received"][0]
        assert rec["event_id"] == "a#1:opened"
        assert rec["deliveries"] == 2
        assert rec["artifact"] == "a"
        assert rec["fault_gen"] == 1


def test_receiver_reject_mode(rcv):
    from starlette.testclient import TestClient

    with TestClient(rcv.app) as client:
        assert client.post("/control/mode", json={"mode": "reject"}).status_code == 200
        r = client.post("/alerts", json=_payload())
        assert r.status_code == 503
        assert client.get("/alerts").json()["count"] == 0
        assert client.post("/control/mode", json={"mode": "bogus"}).status_code == 400
        assert client.post("/control/mode", json={"mode": "accept"}).status_code == 200
        r = client.post("/alerts", json=_payload())
        assert r.status_code == 200
        assert client.get("/alerts").json()["count"] == 1


def test_receiver_reset_clears_records(rcv):
    from starlette.testclient import TestClient

    with TestClient(rcv.app) as client:
        client.post("/alerts", json=_payload())
        assert client.get("/alerts").json()["count"] == 1
        assert client.post("/control/reset").status_code == 200
        assert client.get("/alerts").json()["count"] == 0
