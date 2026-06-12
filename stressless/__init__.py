"""Stressless — agent observability for Claude Agent SDK apps.

Watches every agent run (Claude Agent SDK + raw Anthropic API), records
runs/steps/cost into the ``stressless`` schema, and distills each run into a
TraceCard. Designed to never block and never raise into the host agent path.

Public surface:
    run(kind, ref=..., mode=...)   — group one logical job (async context manager)
    tee_query_stream(stream)       — wrap an SDK query stream (used by pipeline.sdk)
    wrap_anthropic(client, kind=…) — wrap a raw AsyncAnthropic client
    tee_session_stream(stream, …)  — wrap a Managed Agents session event stream
    ingest_session(client, id)     — capture a Managed Agents session after the fact
    note_outcome(kind, ref, out)   — attach a domain outcome to recent runs
"""

from __future__ import annotations

from .anthropic_wrap import wrap_anthropic
from .cma import ingest_session, tee_session_stream
from .collector import RunHandle, current_run, run, tee_query_stream
from .store import enabled


def note_outcome(kind: str, ref: object, outcome: object) -> None:
    """Fire-and-forget: stamp the latest runs for (kind, ref) with an outcome."""
    from . import store

    if not store.enabled():
        return
    store.fire(store.note_outcome(kind, str(ref), str(outcome)))


__all__ = [
    "RunHandle",
    "current_run",
    "enabled",
    "ingest_session",
    "note_outcome",
    "run",
    "tee_query_stream",
    "tee_session_stream",
    "wrap_anthropic",
]
