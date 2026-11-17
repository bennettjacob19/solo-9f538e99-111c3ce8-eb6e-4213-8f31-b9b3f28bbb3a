"""HTTP tests for the alert pipeline (gateway outbox -> local receiver).

Run against a live deployment (the Docker Compose stack or locally started
uvicorn processes for upstream, receiver and gateway):

    GATEWAY=http://127.0.0.1:8000 python -m pytest -q tests/test_alerts.py

The tests talk to the gateway only; the receiver is controlled through the
gateway's ``/api/receiver-*`` proxy endpoints.  The restart test additionally
needs ``GATEWAY_RESTART_CMD`` and is skipped otherwise.
"""

from __future__ import annotations

import concurrent.futures
import os
import subprocess
import time

import httpx
import pytest

GATEWAY = os.environ.get("GATEWAY", "http://127.0.0.1:8000").rstrip("/")
RESTART_CMD = os.environ.get("GATEWAY_RESTART_CMD", "")

CLIENT = httpx.Client(timeout=15.0)


def reset() -> None:
    CLIENT.post(f"{GATEWAY}/api/reset")
    CLIENT.post(f"{GATEWAY}/api/upstream-stats/reset")
    CLIENT.post(f"{GATEWAY}/api/upstream-script", json={"responses": []})
    set_mode("normal")
    set_receiver("accept")
    # Let reset events generated above drain, then clear the receiver log so
    # each test starts from an empty alert history.
    wait_for(lambda: alerts()["outbox"]["pending_count"] == 0)
    CLIENT.post(f"{GATEWAY}/api/receiver-reset")


def set_mode(mode: str, delay: float | None = None) -> None:
    payload: dict[str, object] = {"mode": mode}
    if delay is not None:
        payload["delay"] = delay
    r = CLIENT.post(f"{GATEWAY}/api/upstream-mode", json=payload)
    assert r.status_code == 200, r.text


def set_receiver(mode: str) -> None:
    r = CLIENT.post(f"{GATEWAY}/api/receiver-mode", json={"mode": mode})
    assert r.status_code == 200, r.text


def meta(artifact: str) -> httpx.Response:
    return CLIENT.get(f"{GATEWAY}/api/meta/{artifact}")


def alerts() -> dict:
    r = CLIENT.get(f"{GATEWAY}/api/alerts")
    assert r.status_code == 200, r.text
    return r.json()


def received_for(artifact: str) -> list[dict]:
    return [e for e in alerts()["receiver"].get("received", []) if e["artifact"] == artifact]


def pending_for(artifact: str) -> list[dict]:
    return [e for e in alerts()["outbox"]["pending"] if e["artifact"] == artifact]


def wait_for(cond, timeout: float = 10.0, interval: float = 0.3) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if cond():
                return True
        except Exception:
            pass
        time.sleep(interval)
    return False


def trip(artifact: str) -> None:
    set_mode("503")
    codes = [meta(artifact).status_code for _ in range(3)]
    assert codes == [502, 502, 502], codes


@pytest.fixture(autouse=True)
def _reset():
    reset()
    yield


def test_trip_generates_opened_event_and_delivers():
    art = "example-app.json"
    trip(art)
    assert wait_for(lambda: len(received_for(art)) == 1)
    ev = received_for(art)[0]
    assert ev["kind"] == "opened"
    assert ev["fault_gen"] >= 1
    assert "503" in ev["reason"]
    assert ev["deliveries"] == 1
    # The outbox shows the same event as delivered; nothing left pending.
    data = alerts()
    assert pending_for(art) == []
    delivered = [e for e in data["outbox"]["delivered"] if e["artifact"] == art]
    assert any(e["event_id"] == ev["event_id"] for e in delivered)


def test_concurrent_failures_generate_single_event():
    art = "shared-lib.json"
    set_mode("503")
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as pool:
        list(pool.map(lambda _: meta(art), range(10)))
    assert wait_for(lambda: len(received_for(art)) >= 1)
    time.sleep(1.0)  # no second event may appear
    opened = [e for e in received_for(art) if e["kind"] == "opened"]
    assert len(opened) == 1


def test_failed_probe_stays_in_same_fault_no_new_event():
    art = "reports-tool.json"
    trip(art)
    assert wait_for(lambda: len(received_for(art)) == 1)
    gen = received_for(art)[0]["fault_gen"]
    time.sleep(2.2)
    probe = meta(art)
    assert probe.status_code == 502 and probe.json()["role"] == "probe"
    time.sleep(1.5)  # give the dispatcher time; nothing new may arrive
    events = received_for(art)
    assert len(events) == 1
    assert events[0]["kind"] == "opened"
    assert events[0]["fault_gen"] == gen


def test_recovery_event_follows_trip_event_in_order():
    art = "example-app.json"
    trip(art)
    assert wait_for(lambda: len(received_for(art)) == 1)
    set_mode("normal")
    time.sleep(2.2)
    probe = meta(art)
    assert probe.status_code == 200 and probe.json()["role"] == "probe"
    assert wait_for(lambda: len(received_for(art)) == 2)
    events = sorted(received_for(art), key=lambda e: e["seq"])
    assert [e["kind"] for e in events] == ["opened", "recovered"]
    assert events[0]["fault_gen"] == events[1]["fault_gen"]


def test_manual_reset_generates_reset_event():
    art = "shared-lib.json"
    trip(art)
    assert wait_for(lambda: len(received_for(art)) == 1)
    CLIENT.post(f"{GATEWAY}/api/reset")
    assert wait_for(lambda: len(received_for(art)) == 2)
    events = sorted(received_for(art), key=lambda e: e["seq"])
    assert [e["kind"] for e in events] == ["opened", "reset"]
    assert events[0]["fault_gen"] == events[1]["fault_gen"]


def test_receiver_rejection_keeps_event_pending_until_accepted():
    art = "reports-tool.json"
    set_receiver("reject")
    trip(art)
    time.sleep(2.0)  # several dispatch passes happen while rejecting
    pend = pending_for(art)
    assert len(pend) == 1
    assert pend[0]["kind"] == "opened"
    assert pend[0]["attempts"] >= 1
    assert pend[0]["last_error"]
    assert received_for(art) == []
    set_receiver("accept")
    assert wait_for(lambda: len(received_for(art)) == 1)
    ev = received_for(art)[0]
    assert ev["kind"] == "opened"
    assert ev["deliveries"] == 1  # rejected attempts never reached the log
    assert pending_for(art) == []


def test_pending_events_survive_gateway_restart():
    if not RESTART_CMD:
        pytest.skip("set GATEWAY_RESTART_CMD to test alert persistence across restart")
    art = "example-app.json"
    set_receiver("reject")
    trip(art)
    time.sleep(1.0)
    assert len(pending_for(art)) == 1
    subprocess.run(RESTART_CMD, shell=True, check=True)
    for _ in range(60):
        try:
            if CLIENT.get(f"{GATEWAY}/healthz", timeout=1).status_code == 200:
                break
        except httpx.HTTPError:
            pass
        time.sleep(0.5)
    # The event is still pending after the restart...
    assert wait_for(lambda: len(pending_for(art)) == 1)
    assert received_for(art) == []
    # ...and gets delivered once the receiver accepts again.
    set_receiver("accept")
    assert wait_for(lambda: len(received_for(art)) == 1, timeout=15)
    assert received_for(art)[0]["kind"] == "opened"
