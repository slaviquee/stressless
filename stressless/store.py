"""Stressless storage: a small dedicated asyncpg pool + non-blocking writers.

Constraints:
- NEVER raise into the host agent path — every collector write is fire-and-forget.
- NEVER block the agent — writes are spawned as tasks; payloads truncated client-side.
- Independent pool (max 3 connections) so Stressless cannot starve the app's pool.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import time
from typing import Any, Awaitable

import asyncpg

logger = logging.getLogger(__name__)

TRUNCATE_CHARS = int(os.environ.get("STRESSLESS_TRUNCATE_CHARS", "8000"))

_pool: asyncpg.Pool | None = None
_pool_lock: asyncio.Lock | None = None
_last_warn = 0.0


def enabled() -> bool:
    return os.environ.get("STRESSLESS_ENABLED", "1").strip().lower() not in {
        "0",
        "false",
        "no",
        "off",
    }


def database_url() -> str:
    url = os.environ.get("STRESSLESS_DATABASE_URL")
    if url:
        return url
    try:
        import config

        return config.DATABASE_URL
    except Exception:
        return os.environ.get("DATABASE_URL", "postgresql://localhost:5432/stressless")


async def _init_conn(conn: asyncpg.Connection) -> None:
    for type_name in ("json", "jsonb"):
        await conn.set_type_codec(
            type_name,
            encoder=lambda value: json.dumps(value, ensure_ascii=True, default=str),
            decoder=json.loads,
            schema="pg_catalog",
        )


async def get_pool() -> asyncpg.Pool:
    global _pool, _pool_lock
    if _pool is not None:
        return _pool
    if _pool_lock is None:
        _pool_lock = asyncio.Lock()
    async with _pool_lock:
        if _pool is None:
            _pool = await asyncpg.create_pool(
                database_url(), min_size=0, max_size=3, init=_init_conn
            )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


def _warn_throttled(context: str, exc: Exception) -> None:
    global _last_warn
    now = time.monotonic()
    if now - _last_warn > 60:
        _last_warn = now
        logger.warning("stressless write failed (%s): %s", context, exc)


def fire(coro: Awaitable[Any]) -> None:
    """Schedule a write on the running loop; silently drop if there is none."""

    async def _guarded() -> None:
        try:
            await coro
        except Exception as exc:  # noqa: BLE001 — telemetry must never propagate
            _warn_throttled("async write", exc)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        if hasattr(coro, "close"):
            coro.close()
        return
    loop.create_task(_guarded())


def pack_payload(value: Any) -> tuple[Any, str | None, int | None]:
    """Return (json-ready truncated payload, sha16 of full payload, byte length)."""
    if value is None:
        return None, None, None
    try:
        serialized = json.dumps(value, ensure_ascii=True, default=str)
    except Exception:
        serialized = json.dumps(repr(value))
    digest = hashlib.sha256(serialized.encode("utf-8")).hexdigest()[:16]
    size = len(serialized)
    if size > TRUNCATE_CHARS:
        return (
            {"_truncated": True, "bytes": size, "preview": serialized[:TRUNCATE_CHARS]},
            digest,
            size,
        )
    try:
        return json.loads(serialized), digest, size
    except Exception:
        return {"repr": serialized[:TRUNCATE_CHARS]}, digest, size


_RUN_COLUMNS = (
    "id, agent_kind, external_ref, session_id, parent_run_id, attempt, mode, model, "
    "status, outcome, stop_subtype, error, num_turns, duration_ms, duration_api_ms, "
    "input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, "
    "cost_usd, cost_estimated, budget_usd, model_usage, meta, tracecard, "
    "created_at, finished_at"
)
_RUN_PLACEHOLDERS = ", ".join(f"${i}" for i in range(1, 28))

_RUN_INSERT = (
    f"INSERT INTO stressless.runs ({_RUN_COLUMNS}) VALUES ({_RUN_PLACEHOLDERS}) "
    "ON CONFLICT (id) DO NOTHING"
)

# Finalize fallback: the start-insert task may still be in flight on another
# connection when finalize runs, so the fallback must win on conflict.
_RUN_UPSERT = (
    f"INSERT INTO stressless.runs ({_RUN_COLUMNS}) VALUES ({_RUN_PLACEHOLDERS}) "
    "ON CONFLICT (id) DO UPDATE SET "
    + ", ".join(
        f"{column} = EXCLUDED.{column}"
        for column in (
            "session_id",
            "model",
            "status",
            "outcome",
            "stop_subtype",
            "error",
            "num_turns",
            "duration_ms",
            "duration_api_ms",
            "input_tokens",
            "output_tokens",
            "cache_read_tokens",
            "cache_write_tokens",
            "cost_usd",
            "cost_estimated",
            "model_usage",
            "meta",
            "tracecard",
            "finished_at",
        )
    )
)

_RUN_FIELD_ORDER = [
    "id",
    "agent_kind",
    "external_ref",
    "session_id",
    "parent_run_id",
    "attempt",
    "mode",
    "model",
    "status",
    "outcome",
    "stop_subtype",
    "error",
    "num_turns",
    "duration_ms",
    "duration_api_ms",
    "input_tokens",
    "output_tokens",
    "cache_read_tokens",
    "cache_write_tokens",
    "cost_usd",
    "cost_estimated",
    "budget_usd",
    "model_usage",
    "meta",
    "tracecard",
    "created_at",
    "finished_at",
]

_RUN_UPDATE = """
UPDATE stressless.runs SET
  session_id = COALESCE($2, session_id),
  model = COALESCE($3, model),
  status = $4,
  outcome = COALESCE($5, outcome),
  stop_subtype = $6,
  error = $7,
  num_turns = $8,
  duration_ms = $9,
  duration_api_ms = $10,
  input_tokens = $11,
  output_tokens = $12,
  cache_read_tokens = $13,
  cache_write_tokens = $14,
  cost_usd = $15,
  cost_estimated = $16,
  model_usage = $17,
  meta = $18,
  tracecard = $19,
  finished_at = $20
WHERE id = $1
"""

_STEP_INSERT = """
INSERT INTO stressless.steps
  (run_id, idx, kind, name, tool_use_id, input, output, input_sha, output_sha,
   input_bytes, output_bytes, is_error, duration_ms, tokens, ts)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
"""


async def insert_run_start(row: dict[str, Any]) -> None:
    pool = await get_pool()
    row.setdefault("attempt", 1)
    row.setdefault("mode", "normal")
    row.setdefault("status", "running")
    row.setdefault("cost_estimated", False)
    row.setdefault("meta", {})
    if row.get("parent_run_id") is None and str(row.get("mode", "")).startswith("repair"):
        row["parent_run_id"] = await pool.fetchval(
            "SELECT id FROM stressless.runs WHERE agent_kind = $1 AND external_ref = $2 "
            "AND id <> $3 ORDER BY created_at DESC LIMIT 1",
            row["agent_kind"],
            row["external_ref"],
            row["id"],
        )
    await pool.execute(_RUN_INSERT, *[row.get(field) for field in _RUN_FIELD_ORDER])


async def insert_run_complete(row: dict[str, Any], steps: list[tuple] | None = None) -> None:
    await insert_run_start(row)
    if steps:
        pool = await get_pool()
        await pool.executemany(_STEP_INSERT, steps)


async def finalize_run(row: dict[str, Any], steps: list[tuple]) -> None:
    pool = await get_pool()
    status = await pool.execute(
        _RUN_UPDATE,
        row["id"],
        row.get("session_id"),
        row.get("model"),
        row.get("status"),
        row.get("outcome"),
        row.get("stop_subtype"),
        row.get("error"),
        row.get("num_turns"),
        row.get("duration_ms"),
        row.get("duration_api_ms"),
        row.get("input_tokens"),
        row.get("output_tokens"),
        row.get("cache_read_tokens"),
        row.get("cache_write_tokens"),
        row.get("cost_usd"),
        row.get("cost_estimated"),
        row.get("model_usage"),
        row.get("meta"),
        row.get("tracecard"),
        row.get("finished_at"),
    )
    if status.endswith(" 0"):
        # Start-row insert lost the race (or was dropped) — upsert the full row.
        await pool.execute(_RUN_UPSERT, *[row.get(field) for field in _RUN_FIELD_ORDER])
    if steps:
        await pool.executemany(_STEP_INSERT, steps)


async def note_outcome(kind: str, ref: str, outcome: str) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE stressless.runs SET outcome = $3 WHERE agent_kind = $1 AND external_ref = $2 "
        "AND created_at > now() - interval '6 hours'",
        kind,
        ref,
        outcome,
    )
