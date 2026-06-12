"""Backfill stressless.runs/steps from historical agent_trace.jsonl files.

Understands all three trace generations found in the original host deployment:

A) old MCP item flow:    item_start / sdk_message / sdk_result /
                         item_stream_complete / item_error
B) structured item flow: structured_agent_start / structured_agent_complete /
                         structured_agent_timeout + executor_complete (outcome)
C) raw-API enrich calls: enrich_*_llm_start / enrich_*_llm_done  (carry usage!)
D) lens / dashboard:     {lens,dashboard}_agent_*_start / _done

Cost: only (C) logs token usage, so only those runs get estimated cost.
SDK runs (A/B) historically discarded usage — that telemetry gap is exactly
what the live collector closes. Run ids are deterministic (uuid5) so
re-importing the same file is idempotent.

Usage:  python -m stressless backfill [path.jsonl ...]
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime
from pathlib import Path
from typing import Any

from . import pricing, store

logger = logging.getLogger(__name__)

# Seed string predates the rename — do not change: it keys the deterministic
# run ids of already-imported historical data.
_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "pitcrew-backfill")

_ENRICH_SUBCALL_PREFIX = "enrich_"
_SUBCALL_SONNET_STEPS = {"funding"}  # DIRECT_ENRICH_FUNDING_MODEL override


def _parse_ts(value: str) -> datetime | None:
    try:
        return datetime.fromisoformat(value)
    except Exception:
        return None


def _models() -> tuple[str, str, str]:
    """(agent_model, subcall_model, funding_model) from host config with fallbacks."""
    agent = "claude-sonnet-4-6"
    subcall = "claude-haiku-4-5-20251001"
    funding = "claude-sonnet-4-6"
    try:
        import config

        agent = getattr(config, "AGENT_MODEL", agent)
        subcall = getattr(config, "DIRECT_ENRICH_SUBCALL_MODEL", subcall)
        funding = getattr(config, "DIRECT_ENRICH_FUNDING_MODEL", funding)
    except Exception:
        pass
    return agent, subcall, funding


class _OpenRun:
    __slots__ = ("row", "steps", "message_count")

    def __init__(self, row: dict[str, Any]):
        self.row = row
        self.steps: list[tuple] = []
        self.message_count = 0


def parse_trace_file(path: Path) -> tuple[list[dict[str, Any]], list[list[tuple]]]:
    """Return (run rows, per-run step tuples) parsed from one trace file."""
    agent_model, subcall_model, funding_model = _models()
    file_tag = path.name

    open_items: dict[str, _OpenRun] = {}  # ref -> old-flow run (A)
    open_structured: dict[str, _OpenRun] = {}  # ref -> structured run (B)
    open_subcalls: dict[tuple[str, str], dict[str, Any]] = {}  # (ref, step) -> start
    open_surface: dict[tuple[str, str], dict[str, Any]] = {}  # (ref, kind) -> start
    executor_status: dict[str, str] = {}

    runs: list[dict[str, Any]] = []
    steps: list[list[tuple]] = []

    def _close(open_run: _OpenRun, finished_at: datetime | None, status: str) -> None:
        row = open_run.row
        row["status"] = status
        row["finished_at"] = finished_at or row["created_at"]
        if finished_at and row.get("created_at"):
            row["duration_ms"] = int(
                (finished_at - row["created_at"]).total_seconds() * 1000
            )
        if open_run.message_count:
            row["meta"]["message_count"] = open_run.message_count
        runs.append(row)
        steps.append(open_run.steps)

    def _run_id(*parts: Any) -> uuid.UUID:
        return uuid.uuid5(_NAMESPACE, ":".join(str(p) for p in parts))

    def _base_row(
        run_id: uuid.UUID,
        kind: str,
        ref: str,
        mode: str,
        ts: datetime | None,
        meta: dict[str, Any],
        model: str | None,
    ) -> dict[str, Any]:
        return {
            "id": run_id,
            "agent_kind": kind,
            "external_ref": ref,
            "attempt": 1,
            "mode": mode,
            "model": model,
            "status": "succeeded",
            "meta": {"backfill": file_tag, **meta},
            "created_at": ts,
            "finished_at": None,
        }

    with path.open("r", encoding="utf-8", errors="replace") as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except Exception:
                continue
            event = record.get("event", "")
            ref = str(record.get("raw_item_id", ""))
            ts = _parse_ts(record.get("ts", ""))
            details = record.get("details") or {}
            if not isinstance(details, dict):
                details = {}

            # ---- A: old MCP item flow ------------------------------------
            if event == "item_start":
                mode = details.get("mode") or "normal"
                run_id = _run_id(file_tag, ref, "item", mode, record.get("ts"))
                meta = {
                    "source": details.get("source"),
                    "title": (details.get("title") or "")[:200],
                }
                row = _base_row(
                    run_id, "item_processor", ref, mode, ts, meta, agent_model
                )
                row["budget_usd"] = details.get("max_budget_usd")
                open_items[ref] = _OpenRun(row)
            elif event == "sdk_message" and ref in open_items:
                open_run = open_items[ref]
                open_run.message_count += 1
                payload = details.get("payload") or {}
                summary = details.get("summary") or {}
                kind = (
                    summary.get("message_kind")
                    if isinstance(summary, dict)
                    else None
                ) or (payload.get("message_kind") if isinstance(payload, dict) else None)
                if kind in ("tool_call", "tool_result") and isinstance(payload, dict):
                    packed, sha, size = store.pack_payload(payload)
                    open_run.steps.append(
                        (
                            open_run.row["id"],
                            len(open_run.steps),
                            "tool" if kind == "tool_call" else "tool_result",
                            payload.get("tool_name") or payload.get("name"),
                            payload.get("tool_use_id") or payload.get("id"),
                            packed if kind == "tool_call" else None,
                            packed if kind == "tool_result" else None,
                            sha if kind == "tool_call" else None,
                            sha if kind == "tool_result" else None,
                            size if kind == "tool_call" else None,
                            size if kind == "tool_result" else None,
                            bool(payload.get("is_error")),
                            None,
                            None,
                            ts,
                        )
                    )
            elif event == "sdk_result" and ref in open_items:
                result = details.get("result")
                if isinstance(result, str):
                    open_items[ref].row.setdefault("meta", {})["result_preview"] = result[
                        :300
                    ]
            elif event == "item_stream_complete" and ref in open_items:
                _close(open_items.pop(ref), ts, "succeeded")
            elif event == "item_error" and ref in open_items:
                open_run = open_items.pop(ref)
                open_run.row["error"] = store.pack_payload(details)[0]
                if isinstance(open_run.row["error"], dict):
                    open_run.row["error"] = json.dumps(open_run.row["error"])[:2000]
                _close(open_run, ts, "failed")

            # ---- B: structured item flow ----------------------------------
            elif event == "structured_agent_start":
                run_id = _run_id(file_tag, ref, "structured", record.get("ts"))
                meta = {
                    "source": details.get("source"),
                    "title": (details.get("title") or "")[:200],
                }
                row = _base_row(
                    run_id, "item_processor", ref, "structured", ts, meta, agent_model
                )
                open_structured[ref] = _OpenRun(row)
            elif event == "structured_agent_complete" and ref in open_structured:
                open_run = open_structured.pop(ref)
                open_run.row["meta"]["message_count"] = details.get("message_count")
                open_run.row["meta"]["has_result"] = details.get("has_result")
                if ref in executor_status:
                    open_run.row["outcome"] = executor_status.pop(ref)
                _close(open_run, ts, "succeeded")
            elif event == "structured_agent_timeout" and ref in open_structured:
                open_run = open_structured.pop(ref)
                open_run.row["error"] = "structured agent timeout"
                _close(open_run, ts, "timeout")
            elif event == "executor_complete":
                status = details.get("status")
                if status:
                    executor_status[ref] = status
                    # Stamp the most recent already-closed structured run for this ref.
                    for row in reversed(runs):
                        if (
                            row["external_ref"] == ref
                            and row["agent_kind"] == "item_processor"
                        ):
                            row["outcome"] = status
                            break

            # ---- C: raw-API enrich subcalls (have usage) -------------------
            elif event.startswith(_ENRICH_SUBCALL_PREFIX) and event.endswith(
                ("_llm_start", "_llm_start_retry")
            ):
                step_name = event[len(_ENRICH_SUBCALL_PREFIX) : event.index("_llm_")]
                open_subcalls[(ref, step_name)] = {"ts": ts, "raw": record.get("ts")}
            elif event.startswith(_ENRICH_SUBCALL_PREFIX) and event.endswith(
                ("_llm_done", "_llm_done_retry", "_llm_error", "_llm_error_retry")
            ):
                step_name = event[len(_ENRICH_SUBCALL_PREFIX) : event.index("_llm_")]
                start = open_subcalls.pop((ref, step_name), None)
                usage = details.get("usage") or {}
                model = funding_model if step_name in _SUBCALL_SONNET_STEPS else subcall_model
                run_id = _run_id(
                    file_tag, ref, "enrich", step_name, (start or {}).get("raw") or record.get("ts")
                )
                input_tokens = int(usage.get("input_tokens") or 0)
                output_tokens = int(usage.get("output_tokens") or 0)
                cost = pricing.estimate_cost_usd(
                    model, input_tokens=input_tokens, output_tokens=output_tokens
                )
                failed = "_error" in event
                row = _base_row(
                    run_id,
                    "enrich_subcall",
                    ref,
                    "normal",
                    (start or {}).get("ts") or ts,
                    {"step": step_name, "parsed_ok": details.get("parsed_ok")},
                    model,
                )
                row["status"] = "failed" if failed else "succeeded"
                row["input_tokens"] = input_tokens or None
                row["output_tokens"] = output_tokens or None
                row["cost_usd"] = cost
                row["cost_estimated"] = cost is not None
                row["finished_at"] = ts
                if start and start.get("ts") and ts:
                    row["duration_ms"] = int(
                        (ts - start["ts"]).total_seconds() * 1000
                    )
                row["tracecard"] = {
                    "kind": "enrich_subcall",
                    "step": step_name,
                    "model": model,
                    "tokens": {"input": input_tokens, "output": output_tokens},
                    "cost_usd": round(cost, 6) if cost is not None else None,
                }
                runs.append(row)
                steps.append([])

            # ---- D: lens / dashboard sessions ------------------------------
            elif event in (
                "lens_agent_start",
                "dashboard_agent_start",
                "dashboard_agent_chat_start",
                "dashboard_agent_summary_start",
            ):
                kind = "lens" if event.startswith("lens") else "dashboard"
                open_surface[(ref, event)] = {"ts": ts, "raw": record.get("ts"), "kind": kind}
            elif event in (
                "lens_agent_done",
                "dashboard_agent_done",
                "dashboard_agent_chat_done",
                "dashboard_agent_summary_done",
                "dashboard_agent_summary_error",
            ):
                start_event = event.rsplit("_", 1)[0] + "_start"
                start = open_surface.pop((ref, start_event), None)
                kind = "lens" if event.startswith("lens") else "dashboard"
                run_id = _run_id(
                    file_tag, ref, kind, event, (start or {}).get("raw") or record.get("ts")
                )
                row = _base_row(
                    run_id,
                    kind,
                    ref,
                    event.replace("_start", "").replace(f"{kind}_agent_", "") or "normal",
                    (start or {}).get("ts") or ts,
                    {"event": event},
                    agent_model,
                )
                row["status"] = "failed" if event.endswith("_error") else "succeeded"
                row["finished_at"] = ts
                if start and start.get("ts") and ts:
                    row["duration_ms"] = int((ts - start["ts"]).total_seconds() * 1000)
                runs.append(row)
                steps.append([])

    # Anything left open at EOF: record as failed/incomplete.
    for leftover in (*open_items.values(), *open_structured.values()):
        leftover.row["error"] = "trace ended before completion"
        _close(leftover, None, "failed")

    return runs, steps


async def backfill(paths: list[Path]) -> dict[str, int]:
    """Import trace files, then stamp item outcomes/sources from raw_items."""
    pool = await store.get_pool()
    totals = {"runs": 0, "steps": 0, "files": 0}
    for path in paths:
        if not path.exists():
            logger.warning("backfill: %s does not exist, skipping", path)
            continue
        runs, steps = parse_trace_file(path)
        for row, step_rows in zip(runs, steps):
            try:
                await store.insert_run_complete(row, step_rows)
                totals["runs"] += 1
                totals["steps"] += len(step_rows)
            except Exception as exc:  # keep importing on bad rows
                logger.warning("backfill: failed to insert run %s: %s", row.get("id"), exc)
        totals["files"] += 1
        logger.info("backfill: %s -> %d runs", path.name, len(runs))

    # Authoritative outcomes + source from raw_items for item_processor runs.
    await pool.execute(
        """
        UPDATE stressless.runs r
        SET outcome = COALESCE(r.outcome, ri.status),
            meta = r.meta || jsonb_build_object('source', ri.source)
        FROM raw_items ri
        WHERE r.agent_kind = 'item_processor'
          AND r.external_ref ~ '^[0-9a-f-]{36}$'
          AND ri.id = r.external_ref::uuid
        """
    )
    return totals
