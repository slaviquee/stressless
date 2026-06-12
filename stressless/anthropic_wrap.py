"""Wrap a raw (Async)Anthropic client so every messages.create lands in stressless.runs.

Each non-streaming call becomes one run of the given kind with cache-aware cost,
plus one `llm` step holding the truncated request messages and response text —
that captured input is what the sampled judge grades later. Set
STRESSLESS_CAPTURE_PROMPTS=0 to record sizes/SHAs only (PII-sensitive hosts).
Streaming calls pass through unrecorded (no host call site uses them today).
"""

from __future__ import annotations

import inspect
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from typing import Any

from . import pricing, store

logger = logging.getLogger(__name__)


def wrap_anthropic(
    client: Any,
    *,
    kind: str,
    ref: Any = None,
    meta: dict[str, Any] | None = None,
    mode: str = "normal",
) -> Any:
    """Return the same client with messages.create instrumented. Never raises."""
    if not store.enabled():
        return client
    try:
        messages = client.messages
        original = messages.create
        if getattr(original, "_stressless_wrapped", False):
            return client
        # The anthropic SDK decorates `create` (@required_args), which hides the
        # coroutine — unwrap before checking, and accept Async* clients outright.
        is_async = inspect.iscoroutinefunction(
            inspect.unwrap(original)
        ) or type(client).__name__.startswith("Async")
        if not is_async:
            logger.debug("stressless: sync Anthropic client not wrapped (kind=%s)", kind)
            return client

        async def create(*args: Any, **kwargs: Any) -> Any:
            if kwargs.get("stream"):
                return await original(*args, **kwargs)
            started = time.monotonic()
            created_at = datetime.now(timezone.utc)
            try:
                response = await original(*args, **kwargs)
            except BaseException as exc:
                row = _run_row(kind, ref, meta, kwargs, None, exc, started, created_at, mode)
                store.fire(store.insert_run_complete(row, _llm_steps(row, kwargs, None)))
                raise
            row = _run_row(kind, ref, meta, kwargs, response, None, started, created_at, mode)
            store.fire(store.insert_run_complete(row, _llm_steps(row, kwargs, response)))
            return response

        create._stressless_wrapped = True  # type: ignore[attr-defined]
        messages.create = create
    except Exception:  # noqa: BLE001 — instrumentation must never break the caller
        logger.debug("stressless: wrap_anthropic failed", exc_info=True)
    return client


def _run_row(
    kind: str,
    ref: Any,
    meta: dict[str, Any] | None,
    kwargs: dict[str, Any],
    response: Any,
    error: BaseException | None,
    started: float,
    created_at: datetime,
    mode: str = "normal",
) -> dict[str, Any]:
    duration_ms = int((time.monotonic() - started) * 1000)
    model = kwargs.get("model")
    tokens = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0}
    result_preview: str | None = None
    stop_reason: str | None = None

    if response is not None:
        usage = getattr(response, "usage", None)
        if usage is not None:
            tokens["input"] = int(getattr(usage, "input_tokens", 0) or 0)
            tokens["output"] = int(getattr(usage, "output_tokens", 0) or 0)
            tokens["cache_read"] = int(
                getattr(usage, "cache_read_input_tokens", 0) or 0
            )
            tokens["cache_write"] = int(
                getattr(usage, "cache_creation_input_tokens", 0) or 0
            )
        stop_reason = getattr(response, "stop_reason", None)
        for block in getattr(response, "content", []) or []:
            text = getattr(block, "text", None)
            if text:
                result_preview = text[:300]
                break

    cost = pricing.estimate_cost_usd(
        model,
        input_tokens=tokens["input"],
        output_tokens=tokens["output"],
        cache_read_tokens=tokens["cache_read"],
        cache_write_tokens=tokens["cache_write"],
    )

    run_meta = dict(meta or {})
    run_meta["api"] = "messages.create"
    if kwargs.get("max_tokens"):
        run_meta["max_tokens"] = kwargs["max_tokens"]

    tracecard = {
        "kind": kind,
        "model": model,
        "wall_ms": duration_ms,
        "cost_usd": round(cost, 6) if cost is not None else None,
        "cost_estimated": True if cost is not None else None,
        "tokens": {key: value for key, value in tokens.items() if value},
        "stop": stop_reason,
        "result": result_preview,
        "error": str(error)[:500] if error is not None else None,
    }

    return {
        "id": uuid.uuid4(),
        "agent_kind": kind,
        "external_ref": str(ref) if ref is not None else None,
        "attempt": 1,
        "mode": mode,
        "model": model,
        "status": "failed" if error is not None else "succeeded",
        "stop_subtype": stop_reason,
        "error": str(error)[:2000] if error is not None else None,
        "duration_ms": duration_ms,
        "input_tokens": tokens["input"] or None,
        "output_tokens": tokens["output"] or None,
        "cache_read_tokens": tokens["cache_read"] or None,
        "cache_write_tokens": tokens["cache_write"] or None,
        "cost_usd": cost,
        "cost_estimated": cost is not None,
        "meta": run_meta,
        "tracecard": {k: v for k, v in tracecard.items() if v not in (None, {}, [])},
        "created_at": created_at,
        "finished_at": datetime.now(timezone.utc),
    }


def _llm_steps(row: dict[str, Any], kwargs: dict[str, Any], response: Any) -> list[tuple]:
    """One `llm` step per call: request messages in, response text out."""
    capture = os.environ.get("STRESSLESS_CAPTURE_PROMPTS", "1").strip().lower() not in {
        "0", "false", "no", "off"
    }
    request: dict[str, Any] = {}
    if kwargs.get("system") is not None:
        request["system"] = kwargs["system"]
    request["messages"] = kwargs.get("messages")
    input_payload, input_sha, input_bytes = store.pack_payload(request)
    output_text = None
    if response is not None:
        output_text = "\n".join(
            text for text in (
                getattr(block, "text", None) for block in (getattr(response, "content", None) or [])
            ) if text
        ) or None
    output_payload, output_sha, output_bytes = store.pack_payload(output_text)
    return [(
        row["id"], 0, "llm", kwargs.get("model"), None,
        input_payload if capture else None,
        output_payload if capture else None,
        input_sha, output_sha, input_bytes, output_bytes,
        row["status"] == "failed", row.get("duration_ms"), None, row["created_at"],
    )]
