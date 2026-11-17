"""Two-worker circuit-breaker gateway for artifact metadata lookups.

Architecture
------------
* FastAPI/uvicorn runs two worker processes.  Each worker has its own asyncio
  event loop, so a slow upstream query never blocks another query.
* Breaker state lives in SQLite (WAL mode, ``BEGIN IMMEDIATE`` for writes), so
  it is shared by both workers and survives restarts.
* Every upstream call is bounded to ``REQUEST_TIMEOUT`` seconds end-to-end.
* A breaker failure is a connect failure, a timeout, or a 5xx response.  Any
  other completed response (2xx/3xx/4xx) resets the failure counter.

Breaker semantics (per artifact name)
-------------------------------------
* CLOSED: calls pass through.  Failures are counted in completion order.  The
  third consecutive failure opens the circuit; a healthy response resets to 0.
* OPEN: every request is answered locally with 503; the upstream is not
  contacted.  After ``COOLDOWN`` seconds the first caller atomically becomes
  the single HALF_OPEN probe; everybody else is still rejected.
* A failed/timed-out probe re-opens the breaker with a fresh cooldown.
* A probe that crashes or silently disappears releases the probe slot after a
  lease, so a new probe can eventually run.
* Results carry the epoch observed at admission.  When the breaker has tripped
  since then the result is stale: a slow success that started before the trip
  can neither reset the counter nor close the circuit.
* Probe results carry the probe generation.  A late result from an older probe
  generation is discarded, so it cannot overwrite a newer verdict.

Alerting
--------
* Transitions an on-call engineer must see — the first trip of a fault,
  recovery by a successful probe, and manual reset — are recorded as status
  events (artifact, fault generation, reason) in a durable SQLite outbox, in
  the same transaction as the breaker state change.  If the state change
  committed, the event is committed.
* A background dispatcher (lease-gated, exactly one of the two workers) posts
  pending events to a local alert receiver and retries with backoff until the
  receiver acknowledges.  Receiver rejections and gateway restarts only delay
  delivery; the receiver deduplicates by event id, so a retried delivery
  (e.g. after a lost acknowledgement) never creates a second record.
* Events for one artifact are delivered in occurrence order: a recovery can
  never overtake a still-pending trip.  A failed probe belongs to the same
  fault generation and raises no new event; stale results from older
  generations raise none either.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import sqlite3
import time
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("GATEWAY_DB", BASE_DIR / "data" / "breaker.db"))
UPSTREAM_URL = os.environ.get("UPSTREAM_URL", "http://upstream:9000").rstrip("/")
REQUEST_TIMEOUT = float(os.environ.get("REQUEST_TIMEOUT", "1.0"))
COOLDOWN = float(os.environ.get("COOLDOWN", "2.0"))
FAILURE_THRESHOLD = int(os.environ.get("FAILURE_THRESHOLD", "3"))
# A live probe is bounded by REQUEST_TIMEOUT; anything older than this lease is
# considered abandoned (e.g. its process died before writing a verdict).
PROBE_LEASE = max(2.0, REQUEST_TIMEOUT + 1.0)

# Alert pipeline: durable outbox -> local alert receiver.
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://receiver:9001").rstrip("/")
DISPATCH_INTERVAL = float(os.environ.get("DISPATCH_INTERVAL", "0.5"))
DELIVERY_TIMEOUT = float(os.environ.get("ALERT_DELIVERY_TIMEOUT", "2.0"))
# The dispatching worker renews this lease before every delivery; if it dies,
# the other worker takes over once the lease expires.
DISPATCH_LEASE_TTL = max(4.0, 2.0 * DELIVERY_TIMEOUT)

CLOSED = "closed"
OPEN = "open"
HALF_OPEN = "half_open"

# Unique id of this worker process.  Used to tell our own live probe apart from
# a stale half-open record left behind by a dead process.
INSTANCE_ID = f"{os.getpid()}-{time.time_ns()}"
ACTIVE_PROBES: set[str] = set()


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS breakers (
    artifact        TEXT PRIMARY KEY,
    state           TEXT NOT NULL DEFAULT 'closed',
    failures        INTEGER NOT NULL DEFAULT 0,
    opened_at       REAL NOT NULL DEFAULT 0,
    probe_gen       INTEGER NOT NULL DEFAULT 0,
    probe_holder    TEXT,
    epoch           INTEGER NOT NULL DEFAULT 0,
    fault_gen       INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    last_transition REAL NOT NULL DEFAULT 0,
    updated_at      REAL NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS events (
    id       INTEGER PRIMARY KEY AUTOINCREMENT,
    ts       REAL NOT NULL,
    artifact TEXT NOT NULL,
    kind     TEXT NOT NULL,
    detail   TEXT
);
CREATE TABLE IF NOT EXISTS upstream_stats (
    id           INTEGER PRIMARY KEY CHECK (id = 1),
    hits         INTEGER NOT NULL DEFAULT 0,
    hits_by_mode TEXT NOT NULL DEFAULT '{}'
);
INSERT OR IGNORE INTO upstream_stats (id, hits) VALUES (1, 0);
-- Durable alert outbox.  Rows are inserted in the same transaction as the
-- breaker state change they describe, and deleted only long after delivery.
CREATE TABLE IF NOT EXISTS alert_outbox (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id        TEXT NOT NULL UNIQUE,
    artifact        TEXT NOT NULL,
    fault_gen       INTEGER NOT NULL,
    kind            TEXT NOT NULL,
    reason          TEXT NOT NULL DEFAULT '',
    created_at      REAL NOT NULL,
    next_attempt_at REAL NOT NULL,
    attempts        INTEGER NOT NULL DEFAULT 0,
    delivered_at    REAL,
    last_error      TEXT
);
CREATE INDEX IF NOT EXISTS idx_alert_outbox_pending
    ON alert_outbox (artifact, id) WHERE delivered_at IS NULL;
-- Monotonic per-artifact fault generation.  Survives /api/reset so event ids
-- are never reused for a different incident.
CREATE TABLE IF NOT EXISTS fault_counter (
    artifact TEXT PRIMARY KEY,
    next_gen INTEGER NOT NULL DEFAULT 0
);
-- Lease ensuring exactly one of the two workers dispatches alerts.
CREATE TABLE IF NOT EXISTS dispatcher_lease (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    holder     TEXT NOT NULL DEFAULT '',
    expires_at REAL NOT NULL DEFAULT 0
);
INSERT OR IGNORE INTO dispatcher_lease (id, holder, expires_at) VALUES (1, '', 0);
"""


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    conn.execute("PRAGMA synchronous=NORMAL")
    return conn


def init_db() -> None:
    last: Optional[Exception] = None
    for _ in range(50):
        try:
            conn = connect()
            try:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.executescript(SCHEMA)
                _migrate(conn)
            finally:
                conn.close()
            return
        except sqlite3.OperationalError as exc:  # two workers racing at startup
            last = exc
            time.sleep(0.1)
    raise RuntimeError(f"unable to initialise breaker database: {last}")


def _migrate(conn: sqlite3.Connection) -> None:
    """Idempotent schema upgrades for databases created by older versions."""
    cols = {row["name"] for row in conn.execute("PRAGMA table_info(breakers)")}
    if "fault_gen" not in cols:
        conn.execute("ALTER TABLE breakers ADD COLUMN fault_gen INTEGER NOT NULL DEFAULT 0")
    # A breaker already faulted when the column appeared gets a generation
    # allocated, so its recovery/reset alerts can reference the fault.
    rows = conn.execute(
        "SELECT artifact FROM breakers WHERE state <> ? AND fault_gen = 0", (CLOSED,)
    ).fetchall()
    for row in rows:
        gen = _next_fault_gen(conn, row["artifact"])
        conn.execute(
            "UPDATE breakers SET fault_gen = ? WHERE artifact = ?", (gen, row["artifact"])
        )


@contextlib.contextmanager
def write_tx():
    """Exclusive write transaction, serialising both worker processes."""
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")
    finally:
        conn.close()


def log_event(conn: sqlite3.Connection, artifact: str, kind: str, detail: str = "") -> None:
    conn.execute(
        "INSERT INTO events (ts, artifact, kind, detail) VALUES (?, ?, ?, ?)",
        (time.time(), artifact, kind, detail[:300]),
    )
    conn.execute("DELETE FROM events WHERE id <= (SELECT MAX(id) - 500 FROM events)")


def _next_fault_gen(conn: sqlite3.Connection, artifact: str) -> int:
    """Allocate the next fault generation for an artifact.

    Generations are monotonic per artifact and survive ``/api/reset``, so an
    event id never refers to two different incidents.
    """
    conn.execute(
        "INSERT OR IGNORE INTO fault_counter (artifact, next_gen) VALUES (?, 0)", (artifact,)
    )
    conn.execute("UPDATE fault_counter SET next_gen = next_gen + 1 WHERE artifact = ?", (artifact,))
    row = conn.execute(
        "SELECT next_gen FROM fault_counter WHERE artifact = ?", (artifact,)
    ).fetchone()
    return int(row["next_gen"])


def _enqueue_alert(
    conn: sqlite3.Connection, artifact: str, fault_gen: int, kind: str, reason: str, now: float
) -> None:
    """Queue a status event in the same transaction as the state change.

    The event id is deterministic (``artifact#generation:kind``), so even a
    duplicate enqueue attempt cannot create a second outbox row for the same
    transition.
    """
    conn.execute(
        "INSERT OR IGNORE INTO alert_outbox "
        "(event_id, artifact, fault_gen, kind, reason, created_at, next_attempt_at) "
        "VALUES (?, ?, ?, ?, ?, ?, ?)",
        (f"{artifact}#{fault_gen}:{kind}", artifact, fault_gen, kind, reason[:300], now, now),
    )


# ---------------------------------------------------------------------------
# Breaker state machine
# ---------------------------------------------------------------------------


@dataclass
class Admission:
    allowed: bool
    role: str  # "normal" | "probe" | "none"
    epoch: int
    probe_gen: int = 0
    holder: Optional[str] = None
    state: str = CLOSED
    retry_after: float = 0.0


@dataclass
class Outcome:
    kind: str  # "success" | "failure"
    detail: str
    status: Optional[int] = None
    payload: Any = None


def _probe_is_live(row: sqlite3.Row, now: float) -> bool:
    holder = row["probe_holder"] or ""
    if not holder:
        return False
    pid_text = holder.split("|", 1)[0]
    try:
        pid = int(pid_text)
    except ValueError:
        return False
    if pid == os.getpid():
        # Only probes this process is actively running count as live; a
        # half-open row from an earlier incarnation is stale.
        return holder in ACTIVE_PROBES
    try:
        os.kill(pid, 0)
    except (ProcessLookupError, PermissionError):
        return False
    else:
        # A SIGKILLed worker can linger as a zombie until its parent reaps it;
        # ``kill(pid, 0)`` still succeeds for zombies, so check /proc when
        # present.
        try:
            stat = Path(f"/proc/{pid}/stat").read_text()
        except OSError:
            pass
        else:
            if stat.rsplit(")", 1)[-1].split()[0] == "Z":
                return False
    # Backstop against PID reuse / a live process that abandoned its probe: a
    # probe is bounded by REQUEST_TIMEOUT, so anything older than the lease is
    # considered dead.
    return now - row["opened_at"] <= PROBE_LEASE


def effective_state(row: sqlite3.Row, now: float) -> tuple[str, float]:
    """Persisted state with crash recovery applied.

    A HALF_OPEN row whose probe owner is gone is treated as OPEN measured from
    the probe start, so the normal cooldown rules (and its 503 responses)
    still apply before another probe is elected.
    """
    if row["state"] == HALF_OPEN and not _probe_is_live(row, now):
        return OPEN, row["opened_at"]
    return row["state"], row["opened_at"]


def admit(artifact: str) -> Admission:
    """Atomically decide whether this request may call the upstream.

    Fast paths (CLOSED and rejection-while-open) are read-only, so normal
    traffic and bursts of rejections never serialise on the SQLite write lock.
    Only the probe election takes ``BEGIN IMMEDIATE``.
    """
    now = time.time()
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM breakers WHERE artifact = ?", (artifact,)).fetchone()
    finally:
        conn.close()

    if row is not None:
        fast = _fast_admission(row, now)
        if fast is not None:
            return fast

    with write_tx() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO breakers (artifact, updated_at) VALUES (?, ?)",
            (artifact, now),
        )
        row = conn.execute("SELECT * FROM breakers WHERE artifact = ?", (artifact,)).fetchone()
        now = time.time()
        state, opened_at = effective_state(row, now)

        if state == CLOSED:
            return Admission(True, "normal", row["epoch"], state=CLOSED)

        if state == HALF_OPEN:
            # A live probe is already in flight; nobody else may pass.
            return Admission(False, "none", row["epoch"], state=HALF_OPEN, retry_after=0.2)

        remaining = COOLDOWN - (now - opened_at)
        if remaining > 0:
            return Admission(False, "none", row["epoch"], state=OPEN, retry_after=remaining)

        # Cooldown elapsed: promote exactly one caller to probe.  BEGIN
        # IMMEDIATE serialises this across both worker processes.
        probe_gen = row["probe_gen"] + 1
        holder = f"{os.getpid()}|{INSTANCE_ID}|{probe_gen}"
        conn.execute(
            "UPDATE breakers SET state = ?, probe_gen = ?, probe_holder = ?, opened_at = ?, "
            "updated_at = ? WHERE artifact = ?",
            (HALF_OPEN, probe_gen, holder, now, now, artifact),
        )
        log_event(conn, artifact, "probe_started", f"generation {probe_gen}")
        ACTIVE_PROBES.add(holder)
        return Admission(
            True, "probe", row["epoch"], probe_gen, holder, HALF_OPEN, 0.0
        )


def _fast_admission(row: sqlite3.Row, now: float) -> Optional[Admission]:
    """Admission decision that needs no write, or ``None`` if a write is needed."""
    state, opened_at = effective_state(row, now)
    if state == CLOSED:
        return Admission(True, "normal", row["epoch"], state=CLOSED)
    if state == HALF_OPEN:
        return Admission(False, "none", row["epoch"], state=HALF_OPEN, retry_after=0.2)
    remaining = COOLDOWN - (now - opened_at)
    if remaining > 0:
        return Admission(False, "none", row["epoch"], state=OPEN, retry_after=remaining)
    return None  # cooldown elapsed: must take the write lock to elect the probe


def record_outcome(artifact: str, adm: Admission, outcome: Outcome, hit_role: Optional[str] = None) -> dict[str, Any]:
    """Fold one completed upstream result into the shared state.

    ``hit_role`` counts this call towards the gateway's per-mode upstream
    counters in the same transaction, so each request needs one write.
    """
    now = time.time()
    result: dict[str, Any] = {"effect": "ignored", "state": None}
    with write_tx() as conn:
        if hit_role:
            _bump_upstream_hit(conn, hit_role)
        row = conn.execute("SELECT * FROM breakers WHERE artifact = ?", (artifact,)).fetchone()
        if row is None:
            return result

        if adm.role == "probe":
            own_probe = (
                row["probe_gen"] == adm.probe_gen
                and row["state"] == HALF_OPEN
                and (row["probe_holder"] or "") == (adm.holder or "")
            )
            if not own_probe:
                # A newer probe generation (or a newer decision) already won.
                result.update(effect="stale_probe_ignored", state=row["state"])
                log_event(conn, artifact, "probe_stale", f"generation {adm.probe_gen}")
                return result
            if outcome.kind == "success":
                conn.execute(
                    "UPDATE breakers SET state = ?, failures = 0, probe_holder = NULL, "
                    "epoch = epoch + 1, last_error = NULL, last_transition = ?, updated_at = ? "
                    "WHERE artifact = ?",
                    (CLOSED, now, now, artifact),
                )
                _enqueue_alert(
                    conn,
                    artifact,
                    row["fault_gen"],
                    "recovered",
                    f"recovery probe succeeded ({outcome.detail})",
                    now,
                )
                log_event(conn, artifact, "probe_success", "circuit closed")
                result.update(effect="closed", state=CLOSED)
            else:
                conn.execute(
                    "UPDATE breakers SET state = ?, failures = ?, opened_at = ?, "
                    "probe_holder = NULL, last_error = ?, last_transition = ?, updated_at = ? "
                    "WHERE artifact = ?",
                    (OPEN, FAILURE_THRESHOLD, now, outcome.detail, now, now, artifact),
                )
                log_event(conn, artifact, "probe_failed", outcome.detail)
                result.update(effect="reopened", state=OPEN)
            return result

        # Normal (non-probe) completed attempt.
        if row["state"] != CLOSED:
            if outcome.kind == "success":
                # Slow success started before the trip: it must not recover.
                result.update(effect="stale_success_ignored", state=row["state"])
                log_event(conn, artifact, "stale_success_ignored", "late success after trip")
            else:
                result.update(effect="stale_failure_ignored", state=row["state"])
            return result

        if adm.epoch != row["epoch"]:
            # The breaker opened (and possibly closed again) while this call
            # was in flight; this attempt belongs to an older epoch.
            result.update(effect="stale_attempt_ignored", state=row["state"])
            return result

        if outcome.kind == "success":
            conn.execute(
                "UPDATE breakers SET failures = 0, last_error = NULL, updated_at = ? "
                "WHERE artifact = ?",
                (now, artifact),
            )
            result.update(effect="reset", state=CLOSED)
        else:
            failures = row["failures"] + 1
            if failures >= FAILURE_THRESHOLD:
                fault_gen = _next_fault_gen(conn, artifact)
                conn.execute(
                    "UPDATE breakers SET state = ?, failures = ?, opened_at = ?, "
                    "epoch = epoch + 1, probe_holder = NULL, last_error = ?, "
                    "last_transition = ?, updated_at = ?, fault_gen = ? WHERE artifact = ?",
                    (OPEN, failures, now, outcome.detail, now, now, fault_gen, artifact),
                )
                reason = f"{failures} consecutive failures: {outcome.detail}"
                _enqueue_alert(conn, artifact, fault_gen, "opened", reason, now)
                log_event(conn, artifact, "opened", reason)
                result.update(effect="opened", state=OPEN)
            else:
                conn.execute(
                    "UPDATE breakers SET failures = ?, last_error = ?, updated_at = ? "
                    "WHERE artifact = ?",
                    (failures, outcome.detail, now, artifact),
                )
                log_event(conn, artifact, "failure", f"{failures}/{FAILURE_THRESHOLD} {outcome.detail}")
                result.update(effect="failure_counted", state=CLOSED)
        return result


def clean_stale_probes() -> None:
    """Release HALF_OPEN rows left behind by dead processes at startup."""
    now = time.time()
    with write_tx() as conn:
        rows = conn.execute("SELECT * FROM breakers WHERE state = ?", (HALF_OPEN,)).fetchall()
        for row in rows:
            if not _probe_is_live(row, now):
                conn.execute(
                    "UPDATE breakers SET state = ?, probe_holder = NULL, updated_at = ? "
                    "WHERE artifact = ? AND state = ?",
                    (OPEN, now, row["artifact"], HALF_OPEN),
                )
                log_event(conn, row["artifact"], "probe_abandoned", "released at startup")


# ---------------------------------------------------------------------------
# Upstream call
# ---------------------------------------------------------------------------


# One pooled client per worker process: connection setup is not repeated for
# every lookup, which keeps 1s timeouts meaningful under concurrency.
_client: Optional[httpx.AsyncClient] = None


def upstream_client() -> httpx.AsyncClient:
    global _client
    if _client is None or _client.is_closed:
        limits = httpx.Limits(max_connections=100, max_keepalive_connections=50)
        _client = httpx.AsyncClient(timeout=httpx.Timeout(REQUEST_TIMEOUT), limits=limits)
    return _client


async def close_upstream_client() -> None:
    global _client
    if _client is not None and not _client.is_closed:
        await _client.aclose()
    _client = None


async def _get(url: str) -> httpx.Response:
    return await upstream_client().get(url, headers={"Accept": "application/json"})


async def call_upstream(artifact: str) -> Outcome:
    """One GET bounded by REQUEST_TIMEOUT in total (not per phase)."""
    url = f"{UPSTREAM_URL}/artifacts/{quote(artifact, safe='')}"
    try:
        resp = await asyncio.wait_for(_get(url), timeout=REQUEST_TIMEOUT)
    except (asyncio.TimeoutError, httpx.TimeoutException, httpx.ConnectError,
            httpx.NetworkError, httpx.RemoteProtocolError, httpx.HTTPError) as exc:
        return Outcome("failure", f"{type(exc).__name__}: {exc}"[:300])
    except asyncio.CancelledError:
        raise
    except Exception as exc:  # defensive: any transport oddity counts as failure
        return Outcome("failure", f"{type(exc).__name__}: {exc}"[:300])

    if resp.status_code >= 500:
        return Outcome(
            "failure",
            f"upstream {resp.status_code}: {resp.text[:200]}",
            status=resp.status_code,
        )
    try:
        payload = resp.json()
    except ValueError:
        payload = resp.text[:2000]
    return Outcome("success", f"{resp.status_code}", status=resp.status_code, payload=payload)


# ---------------------------------------------------------------------------
# Auxiliary shared counters
# ---------------------------------------------------------------------------


def _bump_upstream_hit(conn: sqlite3.Connection, role: str) -> None:
    row = conn.execute("SELECT hits, hits_by_mode FROM upstream_stats WHERE id = 1").fetchone()
    try:
        by_mode = json.loads(row["hits_by_mode"] or "{}")
    except ValueError:
        by_mode = {}
    by_mode[role] = by_mode.get(role, 0) + 1
    conn.execute(
        "UPDATE upstream_stats SET hits = ?, hits_by_mode = ? WHERE id = 1",
        (row["hits"] + 1, json.dumps(by_mode, sort_keys=True)),
    )


def get_gateway_hits() -> dict[str, Any]:
    conn = connect()
    try:
        row = conn.execute("SELECT hits, hits_by_mode FROM upstream_stats WHERE id = 1").fetchone()
        return {"hits": row["hits"], "by_mode": json.loads(row["hits_by_mode"] or "{}")}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Alert dispatcher: durable outbox -> local receiver
# ---------------------------------------------------------------------------
#
# Exactly one worker dispatches at a time (dispatcher_lease).  Events are
# delivered per artifact in insertion order: only the oldest undelivered event
# of each artifact is a candidate, so a recovery can never overtake a pending
# trip.  Delivery is at-least-once; the receiver deduplicates by event id, so
# a retry after a lost acknowledgement updates the same record.


def _has_due_events(now: float) -> bool:
    conn = connect()
    try:
        row = conn.execute(
            "SELECT 1 FROM alert_outbox WHERE delivered_at IS NULL AND next_attempt_at <= ? "
            "LIMIT 1",
            (now,),
        ).fetchone()
        return row is not None
    finally:
        conn.close()


def _acquire_dispatch_lease() -> bool:
    """Acquire or renew the dispatcher lease; False if another worker holds it."""
    now = time.time()
    with write_tx() as conn:
        row = conn.execute(
            "SELECT holder, expires_at FROM dispatcher_lease WHERE id = 1"
        ).fetchone()
        if row and row["holder"] != INSTANCE_ID and row["expires_at"] > now:
            return False
        conn.execute(
            "INSERT INTO dispatcher_lease (id, holder, expires_at) VALUES (1, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET holder = excluded.holder, "
            "expires_at = excluded.expires_at",
            (INSTANCE_ID, now + DISPATCH_LEASE_TTL),
        )
    return True


def _due_heads(now: float) -> list[sqlite3.Row]:
    """Oldest due undelivered event per artifact.

    Later events of the same artifact are never selected while an earlier one
    is undelivered, which enforces per-artifact occurrence order.
    """
    conn = connect()
    try:
        return conn.execute(
            """
            SELECT o.* FROM alert_outbox o
            WHERE o.delivered_at IS NULL
              AND o.next_attempt_at <= ?
              AND o.id = (
                    SELECT MIN(o2.id) FROM alert_outbox o2
                    WHERE o2.artifact = o.artifact AND o2.delivered_at IS NULL
              )
            ORDER BY o.id
            """,
            (now,),
        ).fetchall()
    finally:
        conn.close()


def _post_event(payload: dict[str, Any]) -> tuple[bool, str]:
    """POST one event to the receiver; (ok, error) — module-level for tests."""
    try:
        with httpx.Client(timeout=httpx.Timeout(DELIVERY_TIMEOUT)) as client:
            resp = client.post(f"{RECEIVER_URL}/alerts", json=payload)
    except Exception as exc:
        return False, f"{type(exc).__name__}: {exc}"[:300]
    if 200 <= resp.status_code < 300:
        return True, ""
    return False, f"receiver HTTP {resp.status_code}: {resp.text[:200]}"


def _deliver(row: sqlite3.Row) -> None:
    payload = {
        "event_id": row["event_id"],
        "artifact": row["artifact"],
        "fault_gen": row["fault_gen"],
        "kind": row["kind"],
        "reason": row["reason"],
        "occurred_at": row["created_at"],
        "attempt": row["attempts"] + 1,
    }
    ok, err = _post_event(payload)
    now = time.time()
    with write_tx() as conn:
        if ok:
            conn.execute(
                "UPDATE alert_outbox SET delivered_at = ?, attempts = attempts + 1, "
                "last_error = NULL WHERE id = ?",
                (now, row["id"]),
            )
        else:
            attempts = row["attempts"] + 1
            delay = min(0.5 * (2 ** min(attempts, 5)), 10.0)
            conn.execute(
                "UPDATE alert_outbox SET attempts = ?, next_attempt_at = ?, last_error = ? "
                "WHERE id = ?",
                (attempts, now + delay, err, row["id"]),
            )


_last_prune = 0.0


def _prune_delivered(now: float) -> None:
    """Drop delivery history older than an hour (throttled, best-effort)."""
    global _last_prune
    if now - _last_prune < 600:
        return
    _last_prune = now
    try:
        with write_tx() as conn:
            conn.execute(
                "DELETE FROM alert_outbox WHERE delivered_at IS NOT NULL AND delivered_at < ?",
                (now - 3600,),
            )
    except sqlite3.Error:
        pass


def dispatch_pending() -> None:
    """One dispatcher pass; only the lease holder delivers."""
    if not RECEIVER_URL:
        return
    now = time.time()
    if not _has_due_events(now):
        _prune_delivered(now)
        return
    if not _acquire_dispatch_lease():
        return
    for _ in range(100):  # bound one pass; the periodic loop re-enters
        heads = _due_heads(time.time())
        if not heads:
            return
        for row in heads:
            # Renew before every delivery (a delivery may take up to
            # DELIVERY_TIMEOUT); stop if another worker won the lease.
            if not _acquire_dispatch_lease():
                return
            _deliver(row)


async def dispatcher_loop() -> None:
    while True:
        try:
            await asyncio.to_thread(dispatch_pending)
        except asyncio.CancelledError:
            raise
        except Exception:  # the outbox is durable; a failed pass is retried
            pass
        await asyncio.sleep(DISPATCH_INTERVAL)


# ---------------------------------------------------------------------------
# HTTP API
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    clean_stale_probes()
    dispatcher = asyncio.create_task(dispatcher_loop())
    try:
        yield
    finally:
        dispatcher.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await dispatcher
        await close_upstream_client()


app = FastAPI(title="Artifact Metadata Circuit-Breaker Gateway", lifespan=lifespan)


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok", "pid": str(os.getpid())}


def _finish(artifact: str, adm: Admission, outcome: Outcome, hit_role: Optional[str]) -> dict[str, Any]:
    try:
        return record_outcome(artifact, adm, outcome, hit_role=hit_role)
    finally:
        if adm.holder:
            ACTIVE_PROBES.discard(adm.holder)


@app.get("/api/meta/{artifact:path}")
async def get_meta(artifact: str) -> JSONResponse:
    artifact = artifact.strip()
    if not artifact:
        raise HTTPException(status_code=400, detail="artifact name required")

    try:
        # SQLite writes serialise across processes; run them off the event loop
        # so a request waiting on the write lock never stalls other queries in
        # the same worker.
        adm = await asyncio.to_thread(admit, artifact)
    except sqlite3.Error as exc:
        raise HTTPException(status_code=503, detail=f"breaker store unavailable: {exc}") from exc
    if not adm.allowed:
        return JSONResponse(
            status_code=503,
            content={
                "artifact": artifact,
                "error": "circuit_open",
                "state": adm.state,
                "retry_after": round(adm.retry_after, 3),
                "pid": os.getpid(),
            },
            headers={"Retry-After": f"{max(0.0, adm.retry_after):.2f}"},
        )

    started = time.time()
    try:
        outcome = await call_upstream(artifact)
    except asyncio.CancelledError:
        # Client went away / server shutdown: release the probe slot and treat
        # it as a failed probe so recovery can be retried later.
        with contextlib.suppress(Exception):
            await asyncio.shield(
                asyncio.to_thread(
                    _finish, artifact, adm, Outcome("failure", "cancelled"), None
                )
            )
        raise
    elapsed = time.time() - started
    effect = await asyncio.to_thread(_finish, artifact, adm, outcome, adm.role)

    body: dict[str, Any] = {
        "artifact": artifact,
        "role": adm.role,
        "upstream": outcome.kind,
        "detail": outcome.detail,
        "elapsed_ms": round(elapsed * 1000, 1),
        "breaker_effect": effect["effect"],
        "breaker_state": effect["state"],
        "pid": os.getpid(),
    }
    if outcome.payload is not None:
        body["metadata"] = outcome.payload

    headers = {"X-Breaker-Role": adm.role, "X-Worker-Pid": str(os.getpid())}
    if outcome.kind == "success":
        return JSONResponse(status_code=outcome.status or 200, content=body, headers=headers)
    return JSONResponse(status_code=502, content=body, headers=headers)


@app.get("/api/breakers")
async def breakers() -> dict[str, Any]:
    return await asyncio.to_thread(_snapshot)


def _snapshot() -> dict[str, Any]:
    now = time.time()
    conn = connect()
    try:
        rows = conn.execute("SELECT * FROM breakers ORDER BY artifact").fetchall()
        items = []
        for row in rows:
            state, opened_at = effective_state(row, now)
            remaining = (
                max(0.0, COOLDOWN - (now - opened_at)) if state == OPEN else 0.0
            )
            items.append(
                {
                    "artifact": row["artifact"],
                    "state": state,
                    "failures": row["failures"],
                    "opened_at": round(row["opened_at"], 3),
                    "cooldown_remaining": round(remaining, 3),
                    "probe_gen": row["probe_gen"],
                    "probe_holder": row["probe_holder"],
                    "epoch": row["epoch"],
                    "fault_gen": row["fault_gen"],
                    "last_error": row["last_error"],
                }
            )
        events = [
            dict(r)
            for r in conn.execute(
                "SELECT ts, artifact, kind, detail FROM events ORDER BY id DESC LIMIT 40"
            ).fetchall()
        ]
    finally:
        conn.close()
    return {
        "now": now,
        "cooldown": COOLDOWN,
        "threshold": FAILURE_THRESHOLD,
        "timeout": REQUEST_TIMEOUT,
        "breakers": items,
        "events": events,
        "gateway_upstream": get_gateway_hits(),
        "pid": os.getpid(),
    }


@app.post("/api/reset")
async def reset() -> dict[str, Any]:
    await asyncio.to_thread(_reset)
    return {"ok": True}


def _reset() -> None:
    now = time.time()
    with write_tx() as conn:
        rows = conn.execute("SELECT * FROM breakers").fetchall()
        for row in rows:
            state, _ = effective_state(row, now)
            if state == CLOSED:
                continue  # no active fault -> nothing to alert about
            gen = row["fault_gen"] or _next_fault_gen(conn, row["artifact"])
            _enqueue_alert(conn, row["artifact"], gen, "reset", "manual reset", now)
        conn.execute("DELETE FROM breakers")
        conn.execute("DELETE FROM events")
        conn.execute("UPDATE upstream_stats SET hits = 0, hits_by_mode = '{}' WHERE id = 1")
        log_event(conn, "*", "reset")


# -- control-plane proxy to the example upstream ----------------------------


async def _proxy(method: str, path: str, payload: Any = None) -> JSONResponse:
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(3.0)) as client:
            resp = await client.request(method, f"{UPSTREAM_URL}{path}", json=payload)
        try:
            content = resp.json()
        except ValueError:
            content = {"raw": resp.text[:1000]}
        return JSONResponse(status_code=resp.status_code, content=content)
    except Exception as exc:
        return JSONResponse(status_code=502, content={"error": str(exc)[:300]})


@app.get("/api/upstream-mode")
async def get_upstream_mode() -> JSONResponse:
    return await _proxy("GET", "/control/mode")


@app.post("/api/upstream-mode")
async def set_upstream_mode(request: Request) -> JSONResponse:
    payload = await request.json()
    return await _proxy(
        "POST",
        "/control/mode",
        {"mode": payload.get("mode"), "delay": payload.get("delay")},
    )


@app.get("/api/upstream-stats")
async def upstream_stats() -> JSONResponse:
    return await _proxy("GET", "/control/stats")


@app.post("/api/upstream-stats/reset")
async def reset_upstream_stats() -> JSONResponse:
    return await _proxy("POST", "/control/reset")


@app.post("/api/upstream-script")
async def set_upstream_script(request: Request) -> JSONResponse:
    """Install scripted upstream responses (used by the automated tests)."""
    payload = await request.json()
    return await _proxy("POST", "/control/script", payload)


# -- alert outbox + receiver control ----------------------------------------


@app.get("/api/alerts")
async def alerts() -> JSONResponse:
    """Gateway outbox view plus the receiver's view (mode, received records)."""
    outbox = await asyncio.to_thread(_outbox_snapshot)
    receiver: dict[str, Any]
    if not RECEIVER_URL:
        receiver = {"error": "RECEIVER_URL not configured"}
    else:
        try:
            async with httpx.AsyncClient(timeout=httpx.Timeout(3.0)) as client:
                resp = await client.get(f"{RECEIVER_URL}/alerts")
            receiver = resp.json()
        except Exception as exc:
            receiver = {"error": f"{type(exc).__name__}: {exc}"[:300]}
    return JSONResponse(
        {
            "receiver_url": RECEIVER_URL,
            "outbox": outbox,
            "receiver": receiver,
            "pid": os.getpid(),
        }
    )


def _outbox_snapshot() -> dict[str, Any]:
    now = time.time()
    conn = connect()
    try:
        pending = [
            dict(r)
            for r in conn.execute(
                "SELECT id, event_id, artifact, fault_gen, kind, reason, created_at, attempts, "
                "next_attempt_at, last_error FROM alert_outbox "
                "WHERE delivered_at IS NULL ORDER BY id"
            ).fetchall()
        ]
        delivered = [
            dict(r)
            for r in conn.execute(
                "SELECT id, event_id, artifact, fault_gen, kind, reason, created_at, attempts, "
                "delivered_at FROM alert_outbox WHERE delivered_at IS NOT NULL "
                "ORDER BY id DESC LIMIT 20"
            ).fetchall()
        ]
        delivered_count = conn.execute(
            "SELECT COUNT(*) AS c FROM alert_outbox WHERE delivered_at IS NOT NULL"
        ).fetchone()["c"]
    finally:
        conn.close()
    for row in pending:
        row["retry_in"] = round(max(0.0, row["next_attempt_at"] - now), 2)
    return {
        "pending": pending,
        "delivered": delivered,
        "pending_count": len(pending),
        "delivered_count": delivered_count,
    }


async def _receiver_proxy(method: str, path: str, payload: Any = None) -> JSONResponse:
    if not RECEIVER_URL:
        return JSONResponse(status_code=503, content={"error": "RECEIVER_URL not configured"})
    try:
        async with httpx.AsyncClient(timeout=httpx.Timeout(3.0)) as client:
            resp = await client.request(method, f"{RECEIVER_URL}{path}", json=payload)
        try:
            content = resp.json()
        except ValueError:
            content = {"raw": resp.text[:1000]}
        return JSONResponse(status_code=resp.status_code, content=content)
    except Exception as exc:
        return JSONResponse(status_code=502, content={"error": str(exc)[:300]})


@app.post("/api/receiver-mode")
async def set_receiver_mode(request: Request) -> JSONResponse:
    """Switch the local receiver between accepting and rejecting deliveries."""
    payload = await request.json()
    return await _receiver_proxy("POST", "/control/mode", {"mode": payload.get("mode")})


@app.post("/api/receiver-reset")
async def reset_receiver() -> JSONResponse:
    """Clear the receiver's received-records log (demo helper)."""
    return await _receiver_proxy("POST", "/control/reset")


# -- web UI ----------------------------------------------------------------


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(BASE_DIR / "static" / "index.html")


app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")
