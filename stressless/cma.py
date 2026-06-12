"""Managed Agents (CMA) adapter: capture Anthropic-hosted agent sessions.

With Managed Agents the loop runs on Anthropic's orchestration layer, so there
is no in-process query() stream to tee — but the session event stream carries
everything stressless needs: tool use/results, per-request token usage on
``span.model_request_end`` (finer-grained than the SDK's run-level aggregate),
stop reasons, outcome-grader verdicts, compaction markers.

Two capture paths, one run row (deterministic uuid5 on the session id, so they
upsert rather than duplicate):

  tee_session_stream(stream, kind=..., session_id=...)
      wrap the SSE iterator your orchestrator already consumes — zero
      behavior change, telemetry recorded as events pass through.

  ingest_session(client, session_id)  /  ``python -m stressless ingest-cma``
      after-the-fact capture via events.list() — works for webhook-driven or
      historical sessions; CMA retains full event history server-side.

Cost note: CMA reports tokens, never dollars — all costs here are estimates
from the pricing table and flagged ``cost_estimated``. The beta surface
(``managed-agents-2026-04-01``) is still evolving; all field access is
defensive, unknown event types are ignored.
"""

from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from . import pricing, store

logger = logging.getLogger(__name__)

_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "stressless-cma")

_TOOL_USE_TYPES = {"agent.tool_use", "agent.mcp_tool_use", "agent.custom_tool_use"}
_TOOL_RESULT_TYPES = {"agent.tool_result", "agent.mcp_tool_result"}


def _get(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _ts(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        try:
            return datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
    return None


def session_run_id(session_id: str) -> uuid.UUID:
    """Deterministic run id — tee + ingest of the same session upsert one row."""
    return uuid.uuid5(_NAMESPACE, f"cma:{session_id}")


def is_terminal_event(event: Any) -> bool:
    """True when a session event ends the turn (the standard drain-loop gate:
    terminated, or idle with a stop_reason other than requires_action)."""
    event_type = str(_get(event, "type", ""))
    if event_type == "session.status_terminated":
        return True
    if event_type == "session.status_idle":
        stop = _get(event, "stop_reason")
        if stop is None:
            return True
        return str(_get(stop, "type") or "") != "requires_action"
    return False


class CMASessionHandle:
    """Accumulates one Managed Agents session into a stressless run."""

    def __init__(
        self,
        kind: str,
        *,
        session_id: str | None = None,
        ref: Any = None,
        model: str | None = None,
        meta: dict[str, Any] | None = None,
    ) -> None:
        self.kind = kind
        self.session_id = session_id
        self.ref = str(ref) if ref is not None else None
        self.model = model
        self.meta: dict[str, Any] = {"cma": True, **(meta or {})}
        self.id = session_run_id(session_id) if session_id else uuid.uuid4()
        self.status = "running"
        self.stop_subtype: str | None = None
        self.error: str | None = None
        self.num_requests = 0
        self.tokens = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0}
        self.steps: list[dict[str, Any]] = []
        self.scores: list[tuple[str, str, str | None]] = []  # (name, label, reasoning)
        self.result_preview: str | None = None
        self.created_at = datetime.now(timezone.utc)
        self.first_event_at: datetime | None = None
        self.last_event_at: datetime | None = None
        self.finished = False
        self._pending_tools: list[dict[str, Any]] = []
        self._tool_counts: dict[str, list[int]] = {}
        self._flags: set[str] = set()

    # -- event observation ----------------------------------------------------

    def observe_event(self, event: Any) -> None:
        event_type = str(_get(event, "type", ""))
        when = _ts(_get(event, "processed_at")) or datetime.now(timezone.utc)
        if self.first_event_at is None:
            self.first_event_at = when
        self.last_event_at = when

        if event_type in _TOOL_USE_TYPES:
            name = _get(event, "name") or event_type.rsplit(".", 1)[-1]
            payload, sha, size = store.pack_payload(_get(event, "input"))
            step = {
                "idx": len(self.steps), "kind": "tool", "name": name,
                "tool_use_id": str(_get(event, "id") or ""),
                "input": payload, "input_sha": sha, "input_bytes": size,
                "output": None, "output_sha": None, "output_bytes": None,
                "is_error": False, "duration_ms": None, "tokens": None, "ts": when,
            }
            self.steps.append(step)
            self._pending_tools.append(step)
            self._tool_counts.setdefault(name, [0, 0])[0] += 1

        elif event_type in _TOOL_RESULT_TYPES:
            content = _get(event, "content", _get(event, "result"))
            payload, sha, size = store.pack_payload(content)
            is_error = bool(_get(event, "is_error") or False)
            link = str(_get(event, "tool_use_id") or _get(event, "custom_tool_use_id") or "")
            step = None
            if link:
                step = next((s for s in self._pending_tools if s["tool_use_id"] == link), None)
            if step is None and self._pending_tools:
                step = self._pending_tools[0]
            if step is not None:
                self._pending_tools.remove(step)
                step["output"], step["output_sha"], step["output_bytes"] = payload, sha, size
                step["is_error"] = is_error
                if step["ts"] and when:
                    step["duration_ms"] = max(int((when - step["ts"]).total_seconds() * 1000), 0)
                if is_error:
                    self._tool_counts.setdefault(step["name"], [0, 0])[1] += 1
                    self._flags.add(f"tool_error:{step['name']}")

        elif event_type == "agent.message":
            texts = [
                str(_get(block, "text") or "")
                for block in (_get(event, "content") or [])
                if _get(block, "type") == "text"
            ]
            text = "\n".join(t for t in texts if t)
            if text.strip():
                self.result_preview = text[:700]
                self.steps.append({
                    "idx": len(self.steps), "kind": "text", "name": None,
                    "tool_use_id": None, "input": None, "input_sha": None,
                    "input_bytes": None, "output": {"preview": text[:500]},
                    "output_sha": None, "output_bytes": len(text),
                    "is_error": False, "duration_ms": None, "tokens": None, "ts": when,
                })

        elif event_type == "span.model_request_end":
            self.num_requests += 1
            usage = _get(event, "model_usage") or {}
            self.tokens["input"] += int(_get(usage, "input_tokens") or 0)
            self.tokens["output"] += int(_get(usage, "output_tokens") or 0)
            self.tokens["cache_read"] += int(_get(usage, "cache_read_input_tokens") or 0)
            self.tokens["cache_write"] += int(_get(usage, "cache_creation_input_tokens") or 0)
            if _get(event, "is_error"):
                self._flags.add("model_request_error")

        elif event_type == "session.status_idle":
            stop = _get(event, "stop_reason")
            stop_type = str(_get(stop, "type") or "") if stop is not None else ""
            if stop_type and stop_type != "requires_action":
                self.stop_subtype = stop_type

        elif event_type == "session.status_terminated":
            self.stop_subtype = "terminated"

        elif event_type == "session.error":
            message = _get(event, "message") or _get(event, "error") or "session.error"
            self.error = str(message)[:2000]
            self._flags.add("session_error")

        elif event_type == "agent.thread_context_compacted":
            self._flags.add("context_compacted")

        elif event_type == "span.outcome_evaluation_end":
            result = str(_get(event, "result") or "unknown")
            explanation = _get(event, "explanation")
            self.scores.append(("cma_outcome", result, str(explanation)[:2000] if explanation else None))
        # session.thread_* and user.* events are intentionally ignored in v1.

    def is_terminal(self, event: Any) -> bool:
        return is_terminal_event(event)

    # -- finishing -------------------------------------------------------------

    def finish(self, *, error: BaseException | str | None = None) -> None:
        if self.finished:
            return
        self.finished = True
        if error is not None:
            self.status = "failed"
            self.error = (self.error or str(error))[:2000]
        elif self.stop_subtype in ("terminated", "retries_exhausted") or self.error:
            self.status = "failed"
        else:
            self.status = "succeeded"

    def _cost(self) -> float | None:
        return pricing.estimate_cost_usd(
            self.model,
            input_tokens=self.tokens["input"],
            output_tokens=self.tokens["output"],
            cache_read_tokens=self.tokens["cache_read"],
            cache_write_tokens=self.tokens["cache_write"],
        )

    def finish_row(self) -> dict[str, Any]:
        cost = self._cost()
        duration_ms = None
        if self.first_event_at and self.last_event_at:
            duration_ms = max(
                int((self.last_event_at - self.first_event_at).total_seconds() * 1000), 0
            )
        tracecard = {
            "kind": self.kind, "cma": True, "session": self.session_id,
            "status": self.status, "stop": self.stop_subtype, "model": self.model,
            "model_requests": self.num_requests, "wall_ms": duration_ms,
            "cost_usd": round(cost, 6) if cost is not None else None,
            "cost_estimated": True if cost is not None else None,
            "tokens": {k: v for k, v in self.tokens.items() if v},
            "tools": {
                name: (f"{calls}({errors} err)" if errors else calls)
                for name, (calls, errors) in sorted(self._tool_counts.items())
            },
            "flags": sorted(self._flags),
            "result": self.result_preview,
            "error": self.error,
        }
        return {
            "id": self.id,
            "agent_kind": self.kind,
            "external_ref": self.ref,
            "session_id": self.session_id,
            "attempt": 1,
            "mode": "normal",
            "model": self.model,
            "status": self.status,
            "stop_subtype": self.stop_subtype,
            "error": self.error,
            "num_turns": self.num_requests or None,
            "duration_ms": duration_ms,
            "input_tokens": self.tokens["input"] or None,
            "output_tokens": self.tokens["output"] or None,
            "cache_read_tokens": self.tokens["cache_read"] or None,
            "cache_write_tokens": self.tokens["cache_write"] or None,
            "cost_usd": cost,
            "cost_estimated": cost is not None,
            "meta": self.meta,
            "tracecard": {k: v for k, v in tracecard.items() if v not in (None, {}, [])},
            "created_at": self.first_event_at or self.created_at,
            "finished_at": self.last_event_at or datetime.now(timezone.utc),
        }

    def step_rows(self) -> list[tuple]:
        return [
            (
                self.id, s["idx"], s["kind"], s.get("name"), s.get("tool_use_id"),
                s.get("input"), s.get("output"), s.get("input_sha"), s.get("output_sha"),
                s.get("input_bytes"), s.get("output_bytes"), bool(s.get("is_error")),
                s.get("duration_ms"), s.get("tokens"), s.get("ts"),
            )
            for s in self.steps
        ]


async def _persist(handle: CMASessionHandle) -> None:
    pool = await store.get_pool()
    await pool.execute(
        "DELETE FROM stressless.steps WHERE run_id = $1", handle.id
    )  # idempotent re-ingest
    await store.finalize_run(handle.finish_row(), handle.step_rows())
    for name, label, reasoning in handle.scores:
        await pool.execute(
            """INSERT INTO stressless.scores
                 (run_id, name, source, data_type, value_text, reasoning)
               VALUES ($1, $2, 'cma_grader', 'categorical', $3, $4)""",
            handle.id, name, label, reasoning,
        )


async def tee_session_stream(
    stream: AsyncIterator[Any],
    *,
    kind: str = "cma",
    session_id: str | None = None,
    ref: Any = None,
    model: str | None = None,
    meta: dict[str, Any] | None = None,
) -> AsyncIterator[Any]:
    """Pass a CMA session event stream through unchanged while capturing it."""
    if not store.enabled():
        async for event in stream:
            yield event
        return
    handle = CMASessionHandle(kind, session_id=session_id, ref=ref, model=model, meta=meta)
    error: BaseException | None = None
    try:
        async for event in stream:
            try:
                handle.observe_event(event)
            except Exception:  # noqa: BLE001 — observation must never break the host
                logger.debug("stressless: cma observe_event failed", exc_info=True)
            yield event
    except GeneratorExit:
        raise  # consumer closed us at the terminal event — normal completion
    except BaseException as exc:
        error = exc
        raise
    finally:
        handle.finish(error=error)
        store.fire(_persist(handle))


async def ingest_session(
    client: Any,
    session_id: str,
    *,
    kind: str | None = None,
    model: str | None = None,
    meta: dict[str, Any] | None = None,
) -> uuid.UUID:
    """Fetch a session's full event history from the API and store it as a run."""
    session = await client.beta.sessions.retrieve(session_id)
    agent = _get(session, "agent")
    if model is None:
        raw_model = _get(agent, "model")
        model = str(_get(raw_model, "id") or raw_model) if raw_model is not None else None
    if kind is None:
        name = str(_get(agent, "name") or "cma")
        kind = "cma:" + name.lower().replace(" ", "-")[:40]

    handle = CMASessionHandle(kind, session_id=session_id, model=model, meta=meta)
    events = await client.beta.sessions.events.list(session_id=session_id)
    data = _get(events, "data") or events
    for event in data:
        handle.observe_event(event)
    handle.finish()
    await _persist(handle)
    return handle.id
