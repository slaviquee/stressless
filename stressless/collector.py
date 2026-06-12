"""Run capture: ambient run context, SDK stream tee, message → step parsing.

Works against claude-agent-sdk >= 0.1.48. Newer ResultMessage fields
(model_usage, permission_denials, stop_reason, errors, api_error_status,
uuid) and server-side tool blocks (ServerToolUseBlock/ServerToolResultBlock)
are read defensively via getattr so the collector keeps working as the SDK
evolves.
"""

from __future__ import annotations

import contextlib
import logging
import time
import uuid
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from typing import Any, AsyncIterator

from . import pricing, store

logger = logging.getLogger(__name__)

current_run: ContextVar["RunHandle | None"] = ContextVar(
    "stressless_current_run", default=None
)

_STATUS_BY_SUBTYPE = {
    "success": "succeeded",
    "error_max_turns": "timeout",
    "error_max_budget_usd": "budget_exceeded",
}

_DICT_MESSAGE_TYPES = {
    "assistant": "AssistantMessage",
    "user": "UserMessage",
    "result": "ResultMessage",
    "system": "SystemMessage",
}

_DICT_BLOCK_TYPES = {
    "tool_use": "ToolUseBlock",
    "tool_result": "ToolResultBlock",
    "server_tool_use": "ServerToolUseBlock",
    "server_tool_result": "ServerToolResultBlock",
    "text": "TextBlock",
    "thinking": "ThinkingBlock",
}


def _getter(obj: Any):
    if isinstance(obj, dict):
        return lambda key, default=None: obj.get(key, default)
    return lambda key, default=None: getattr(obj, key, default)


class RunHandle:
    """Mutable in-memory record of one logical agent run."""

    def __init__(
        self,
        kind: str,
        *,
        ref: Any = None,
        mode: str = "normal",
        attempt: int = 1,
        meta: dict[str, Any] | None = None,
        budget_usd: float | None = None,
    ) -> None:
        self.id = uuid.uuid4()
        self.kind = kind
        self.ref = str(ref) if ref is not None else None
        self.mode = mode
        self.attempt = attempt
        self.meta: dict[str, Any] = dict(meta or {})
        self.budget_usd = budget_usd
        self.model: str | None = None
        self.session_id: str | None = None
        self.status = "running"
        self.outcome: str | None = None
        self.stop_subtype: str | None = None
        self.stop_reason: str | None = None
        self.error: str | None = None
        self.num_turns: int | None = None
        self.duration_ms: int | None = None
        self.duration_api_ms: int | None = None
        self.tokens = {"input": 0, "output": 0, "cache_read": 0, "cache_write": 0}
        self.cost_usd: float | None = None
        self.cost_estimated = False
        self.model_usage: Any = None
        self.result_preview: str | None = None
        self.steps: list[dict[str, Any]] = []
        self.created_at = datetime.now(timezone.utc)
        self.finished_at: datetime | None = None
        self.finished = False
        self._t0 = time.monotonic()
        self._pending_tools: dict[str, dict[str, Any]] = {}
        self._tool_counts: dict[str, list[int]] = {}
        self._flags: set[str] = set()

    # -- message observation -------------------------------------------------

    def observe_message(self, message: Any) -> None:
        name = type(message).__name__
        if isinstance(message, dict):
            name = _DICT_MESSAGE_TYPES.get(str(message.get("type")), "")
        if name == "SystemMessage":
            get = _getter(message)
            if get("subtype") == "init":
                data = get("data") or {}
                if isinstance(data, dict):
                    self.model = data.get("model") or self.model
                    self.session_id = data.get("session_id") or self.session_id
        elif name == "AssistantMessage":
            get = _getter(message)
            self.model = get("model") or self.model
            for block in get("content") or []:
                self._observe_block(block)
        elif name == "UserMessage":
            content = _getter(message)("content")
            if isinstance(content, list):
                for block in content:
                    self._observe_block(block)
        elif name == "ResultMessage":
            self._apply_result(message)
        # StreamEvent and unknown message types are intentionally ignored.

    def _observe_block(self, block: Any) -> None:
        bname = type(block).__name__
        if isinstance(block, dict):
            bname = _DICT_BLOCK_TYPES.get(str(block.get("type")), "")
        get = _getter(block)

        # Server-side tools (web_search/web_fetch, SDK 0.2.x) carry the same
        # id/name/input and tool_use_id/content shapes as client tool blocks.
        if bname in ("ToolUseBlock", "ServerToolUseBlock"):
            payload, sha, size = store.pack_payload(get("input"))
            tool_name = get("name") or "?"
            step = {
                "idx": len(self.steps),
                "kind": "tool",
                "name": tool_name,
                "tool_use_id": get("id"),
                "input": payload,
                "input_sha": sha,
                "input_bytes": size,
                "output": None,
                "output_sha": None,
                "output_bytes": None,
                "is_error": False,
                "duration_ms": None,
                "tokens": None,
                "ts": datetime.now(timezone.utc),
                "_t0": time.monotonic(),
            }
            self.steps.append(step)
            if step["tool_use_id"]:
                self._pending_tools[step["tool_use_id"]] = step
            self._tool_counts.setdefault(tool_name, [0, 0])[0] += 1

        elif bname in ("ToolResultBlock", "ServerToolResultBlock"):
            tool_use_id = get("tool_use_id")
            payload, sha, size = store.pack_payload(get("content"))
            is_error = bool(get("is_error") or False)
            step = self._pending_tools.pop(tool_use_id, None) if tool_use_id else None
            if step is not None:
                step["output"] = payload
                step["output_sha"] = sha
                step["output_bytes"] = size
                step["is_error"] = is_error
                started = step.pop("_t0", None)
                if started is not None:
                    step["duration_ms"] = int((time.monotonic() - started) * 1000)
                tool_name = step["name"]
            else:
                tool_name = "?"
                self.steps.append(
                    {
                        "idx": len(self.steps),
                        "kind": "tool_result",
                        "name": None,
                        "tool_use_id": tool_use_id,
                        "input": None,
                        "input_sha": None,
                        "input_bytes": None,
                        "output": payload,
                        "output_sha": sha,
                        "output_bytes": size,
                        "is_error": is_error,
                        "duration_ms": None,
                        "tokens": None,
                        "ts": datetime.now(timezone.utc),
                    }
                )
            if is_error:
                self._tool_counts.setdefault(tool_name, [0, 0])[1] += 1
                self._flags.add(f"tool_error:{tool_name}")
            if size and size > store.TRUNCATE_CHARS:
                self._flags.add(f"tool_output_gt_{store.TRUNCATE_CHARS // 1000}kb:{tool_name}")

        elif bname == "TextBlock":
            text = get("text") or ""
            if text.strip():
                self.steps.append(
                    {
                        "idx": len(self.steps),
                        "kind": "text",
                        "name": None,
                        "tool_use_id": None,
                        "input": None,
                        "input_sha": None,
                        "input_bytes": None,
                        "output": {"preview": text[:500]},
                        "output_sha": None,
                        "output_bytes": len(text),
                        "is_error": False,
                        "duration_ms": None,
                        "tokens": None,
                        "ts": datetime.now(timezone.utc),
                    }
                )

        elif bname == "ThinkingBlock":
            thinking = get("thinking") or ""
            self.steps.append(
                {
                    "idx": len(self.steps),
                    "kind": "thinking",
                    "name": None,
                    "tool_use_id": None,
                    "input": None,
                    "input_sha": None,
                    "input_bytes": None,
                    "output": None,
                    "output_sha": None,
                    "output_bytes": len(thinking),
                    "is_error": False,
                    "duration_ms": None,
                    "tokens": None,
                    "ts": datetime.now(timezone.utc),
                }
            )

    def _apply_result(self, message: Any) -> None:
        get = _getter(message)
        self.stop_subtype = get("subtype") or self.stop_subtype
        self.num_turns = get("num_turns")
        self.duration_ms = get("duration_ms") or self.duration_ms
        self.duration_api_ms = get("duration_api_ms")
        self.session_id = get("session_id") or self.session_id

        usage = get("usage") or {}
        if isinstance(usage, dict):
            self.tokens["input"] = int(usage.get("input_tokens") or 0)
            self.tokens["output"] = int(usage.get("output_tokens") or 0)
            self.tokens["cache_read"] = int(usage.get("cache_read_input_tokens") or 0)
            self.tokens["cache_write"] = int(
                usage.get("cache_creation_input_tokens") or 0
            )

        cost = get("total_cost_usd")
        if cost is not None:
            self.cost_usd = float(cost)
            self.cost_estimated = False
        else:
            estimate = pricing.estimate_cost_usd(
                self.model,
                input_tokens=self.tokens["input"],
                output_tokens=self.tokens["output"],
                cache_read_tokens=self.tokens["cache_read"],
                cache_write_tokens=self.tokens["cache_write"],
            )
            if estimate is not None:
                self.cost_usd = estimate
                self.cost_estimated = True

        # Newer-SDK fields — absent on 0.1.48, read defensively.
        model_usage = get("model_usage")
        if model_usage is not None:
            self.model_usage = store.pack_payload(model_usage)[0]
        denials = get("permission_denials")
        if denials:
            self.meta["permission_denials"] = store.pack_payload(denials)[0]
            self._flags.add("permission_denied")
        # API-level stop_reason (end_turn/max_tokens/refusal/…) — kept apart
        # from stop_subtype, which holds the SDK's run-level subtype.
        self.stop_reason = get("stop_reason") or self.stop_reason
        errors = get("errors")
        if errors:
            self.meta["errors"] = store.pack_payload(errors)[0]
            self._flags.add("result_errors")
        api_error_status = get("api_error_status")
        if api_error_status is not None:
            self.meta["api_error_status"] = api_error_status
            self._flags.add(f"api_error:{api_error_status}")
        result_uuid = get("uuid")
        if result_uuid is not None:
            self.meta["result_uuid"] = str(result_uuid)

        result = get("result")
        if isinstance(result, str) and result:
            self.result_preview = result[:700]
        if get("is_error"):
            self._flags.add("result_is_error")

    # -- finishing ------------------------------------------------------------

    def finish(self, *, error: BaseException | str | None = None) -> None:
        if self.finished:
            return
        self.finished = True
        self.finished_at = datetime.now(timezone.utc)
        if self.duration_ms is None:
            self.duration_ms = int((time.monotonic() - self._t0) * 1000)
        if error is not None:
            self.status = "failed"
            self.error = str(error)[:2000] or type(error).__name__
        elif self.stop_subtype is not None:
            self.status = _STATUS_BY_SUBTYPE.get(self.stop_subtype, "failed")
        else:
            self.status = "succeeded"
        if (
            (self.num_turns or 0) >= 3
            and self.tokens["input"] + self.tokens["cache_write"] > 20_000
            and self.tokens["cache_read"] == 0
        ):
            self._flags.add("cache_cold")
        if (
            self.budget_usd
            and self.cost_usd
            and self.cost_usd >= 0.8 * float(self.budget_usd)
        ):
            self._flags.add("near_budget")

    def build_tracecard(self) -> dict[str, Any]:
        card: dict[str, Any] = {
            "kind": self.kind,
            "ref": self.ref,
            "mode": self.mode if self.mode != "normal" else None,
            "status": self.status,
            "outcome": self.outcome,
            "stop": self.stop_subtype,
            "stop_reason": self.stop_reason,
            "model": self.model,
            "turns": self.num_turns,
            "wall_ms": self.duration_ms,
            "api_ms": self.duration_api_ms,
            "cost_usd": round(self.cost_usd, 6) if self.cost_usd is not None else None,
            "cost_estimated": self.cost_estimated or None,
            "tokens": {key: value for key, value in self.tokens.items() if value},
            "tools": {
                name: (f"{calls}({errors} err)" if errors else calls)
                for name, (calls, errors) in sorted(self._tool_counts.items())
            },
            "flags": sorted(self._flags),
            "result": self.result_preview,
            "error": self.error,
        }
        return {k: v for k, v in card.items() if v not in (None, {}, [])}

    # -- row builders ----------------------------------------------------------

    def start_row(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "agent_kind": self.kind,
            "external_ref": self.ref,
            "attempt": self.attempt,
            "mode": self.mode,
            "status": "running",
            "budget_usd": self.budget_usd,
            "meta": self.meta,
            "created_at": self.created_at,
        }

    def finish_row(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "agent_kind": self.kind,
            "external_ref": self.ref,
            "session_id": self.session_id,
            "attempt": self.attempt,
            "mode": self.mode,
            "model": self.model,
            "status": self.status,
            "outcome": self.outcome,
            "stop_subtype": self.stop_subtype,
            "error": self.error,
            "num_turns": self.num_turns,
            "duration_ms": self.duration_ms,
            "duration_api_ms": self.duration_api_ms,
            "input_tokens": self.tokens["input"] or None,
            "output_tokens": self.tokens["output"] or None,
            "cache_read_tokens": self.tokens["cache_read"] or None,
            "cache_write_tokens": self.tokens["cache_write"] or None,
            "cost_usd": self.cost_usd,
            "cost_estimated": self.cost_estimated,
            "budget_usd": self.budget_usd,
            "model_usage": self.model_usage,
            "meta": self.meta,
            "tracecard": self.build_tracecard(),
            "created_at": self.created_at,
            "finished_at": self.finished_at,
        }

    def step_rows(self) -> list[tuple]:
        rows: list[tuple] = []
        for step in self.steps:
            rows.append(
                (
                    self.id,
                    step["idx"],
                    step["kind"],
                    step.get("name"),
                    step.get("tool_use_id"),
                    step.get("input"),
                    step.get("output"),
                    step.get("input_sha"),
                    step.get("output_sha"),
                    step.get("input_bytes"),
                    step.get("output_bytes"),
                    bool(step.get("is_error")),
                    step.get("duration_ms"),
                    step.get("tokens"),
                    step.get("ts"),
                )
            )
        return rows


@asynccontextmanager
async def run(
    kind: str,
    *,
    ref: Any = None,
    mode: str = "normal",
    attempt: int = 1,
    meta: dict[str, Any] | None = None,
    budget_usd: float | None = None,
) -> AsyncIterator[RunHandle]:
    """Group one logical job; nested SDK/API calls attach to this run."""
    handle = RunHandle(
        kind, ref=ref, mode=mode, attempt=attempt, meta=meta, budget_usd=budget_usd
    )
    if not store.enabled():
        yield handle
        return
    store.fire(store.insert_run_start(handle.start_row()))
    token = current_run.set(handle)
    try:
        yield handle
    except BaseException as exc:
        handle.finish(error=exc)
        raise
    finally:
        current_run.reset(token)
        handle.finish()
        store.fire(store.finalize_run(handle.finish_row(), handle.step_rows()))


async def tee_query_stream(
    stream: AsyncIterator[Any], *, default_kind: str = "sdk"
) -> AsyncIterator[Any]:
    """Pass an SDK query stream through unchanged while capturing telemetry.

    Attaches to the ambient ``run()`` context when present; otherwise opens an
    implicit run so no query goes unrecorded.
    """
    if not store.enabled():
        async for message in stream:
            yield message
        return

    handle = current_run.get()
    implicit: RunHandle | None = None
    if handle is None:
        implicit = handle = RunHandle(default_kind)
        store.fire(store.insert_run_start(handle.start_row()))

    error: BaseException | None = None
    try:
        async for message in stream:
            try:
                handle.observe_message(message)
            except Exception:  # noqa: BLE001 — observation must never break the agent
                logger.debug("stressless: observe_message failed", exc_info=True)
            yield message
    except GeneratorExit:
        raise  # consumer closed the stream early — not a run failure
    except BaseException as exc:
        error = exc
        raise
    finally:
        # Make sure the inner SDK generator is closed when the consumer
        # closes us (the host calls .aclose() on early exits).
        aclose = getattr(stream, "aclose", None)
        if aclose is not None:
            with contextlib.suppress(Exception):
                await aclose()
        if implicit is not None:
            implicit.finish(error=error)
            store.fire(store.finalize_run(implicit.finish_row(), implicit.step_rows()))
