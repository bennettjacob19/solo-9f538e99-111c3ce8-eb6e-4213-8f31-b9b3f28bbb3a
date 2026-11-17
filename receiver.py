"""Local alert receiver for the circuit-breaker gateway.

Stores the status events posted by the gateway's durable outbox
(``app.dispatch_pending``).  Delivery is idempotent: the gateway's event id
(``artifact#generation:kind``) is unique, so a retried delivery — after a
lost acknowledgement or a gateway restart — updates the same record and
bumps its ``deliveries`` counter instead of creating a duplicate.

A small control plane lets the demo web page switch the receiver between
accepting and rejecting deliveries, which exercises the gateway's retry
path.  Received records live in SQLite (``RECEIVER_DB``) and survive
receiver restarts.
"""

from __future__ import annotations

import os
import sqlite3
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

BASE_DIR = Path(__file__).resolve().parent
DB_PATH = Path(os.environ.get("RECEIVER_DB", BASE_DIR / "data" / "receiver.db"))

ACCEPT = "accept"
REJECT = "reject"

# In-memory demo toggle: the receiver starts accepting after a restart.
_mode: dict[str, str] = {"mode": ACCEPT}

VALID_KINDS = {"opened", "recovered", "reset"}

SCHEMA = """
CREATE TABLE IF NOT EXISTS received (
    seq               INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id          TEXT NOT NULL UNIQUE,
    artifact          TEXT NOT NULL,
    fault_gen         INTEGER NOT NULL DEFAULT 0,
    kind              TEXT NOT NULL,
    reason            TEXT NOT NULL DEFAULT '',
    occurred_at       REAL NOT NULL DEFAULT 0,
    first_received_at REAL NOT NULL,
    last_received_at  REAL NOT NULL,
    deliveries        INTEGER NOT NULL DEFAULT 1
);
"""


def connect() -> sqlite3.Connection:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(DB_PATH), timeout=5.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def init_db() -> None:
    conn = connect()
    try:
        conn.execute("PRAGMA journal_mode=WAL")
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@asynccontextmanager
async def lifespan(_: FastAPI):
    init_db()
    yield


app = FastAPI(title="Local alert receiver", lifespan=lifespan)


@app.post("/alerts")
async def receive_alert(request: Request) -> JSONResponse:
    """Store one status event; idempotent on the gateway's event id."""
    if _mode["mode"] != ACCEPT:
        return JSONResponse(
            status_code=503,
            content={"error": "receiver_rejecting", "mode": _mode["mode"]},
        )
    try:
        payload = await request.json()
    except ValueError:
        return JSONResponse(status_code=400, content={"error": "invalid json"})
    if not isinstance(payload, dict):
        return JSONResponse(status_code=400, content={"error": "json object expected"})

    event_id = str(payload.get("event_id") or "").strip()
    artifact = str(payload.get("artifact") or "").strip()
    kind = str(payload.get("kind") or "").strip()
    reason = str(payload.get("reason") or "")[:300]
    if not event_id or not artifact or kind not in VALID_KINDS:
        return JSONResponse(
            status_code=400,
            content={"error": "event_id, artifact and a known kind are required"},
        )
    try:
        fault_gen = int(payload.get("fault_gen"))
        occurred_at = float(payload.get("occurred_at") or 0.0)
    except (TypeError, ValueError):
        return JSONResponse(status_code=400, content={"error": "invalid fault_gen/occurred_at"})

    now = time.time()
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute(
            "INSERT OR IGNORE INTO received "
            "(event_id, artifact, fault_gen, kind, reason, occurred_at, "
            "first_received_at, last_received_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (event_id, artifact, fault_gen, kind, reason, occurred_at, now, now),
        )
        duplicate = cur.rowcount == 0
        if duplicate:
            # Already stored earlier; the previous acknowledgement was lost.
            conn.execute(
                "UPDATE received SET deliveries = deliveries + 1, last_received_at = ? "
                "WHERE event_id = ?",
                (now, event_id),
            )
        conn.execute("COMMIT")
    finally:
        conn.close()
    return JSONResponse(content={"ok": True, "duplicate": duplicate, "event_id": event_id})


@app.get("/alerts")
async def list_alerts() -> dict[str, Any]:
    conn = connect()
    try:
        rows = [
            dict(r)
            for r in conn.execute(
                "SELECT seq, event_id, artifact, fault_gen, kind, reason, occurred_at, "
                "first_received_at, last_received_at, deliveries "
                "FROM received ORDER BY seq DESC LIMIT 200"
            ).fetchall()
        ]
        total = conn.execute("SELECT COUNT(*) AS c FROM received").fetchone()["c"]
    finally:
        conn.close()
    return {"mode": _mode["mode"], "count": total, "received": rows}


@app.get("/control/mode")
async def get_mode() -> dict[str, str]:
    return dict(_mode)


@app.post("/control/mode")
async def set_mode(request: Request) -> JSONResponse:
    try:
        payload = await request.json()
    except ValueError:
        payload = {}
    mode = str(payload.get("mode") or "") if isinstance(payload, dict) else ""
    if mode not in {ACCEPT, REJECT}:
        return JSONResponse(status_code=400, content={"error": "mode must be accept or reject"})
    _mode["mode"] = mode
    return JSONResponse(content=dict(_mode))


@app.post("/control/reset")
async def reset_received() -> dict[str, Any]:
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        cur = conn.execute("DELETE FROM received")
        conn.execute("COMMIT")
    finally:
        conn.close()
    return {"ok": True, "cleared": cur.rowcount}


@app.get("/healthz")
async def healthz() -> dict[str, str]:
    return {"status": "ok"}
