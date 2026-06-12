"""Offline unit tests for the Stressless collector (no DB, no network)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from stressless import pricing
from stressless.backfill import parse_trace_file
from stressless.collector import RunHandle, tee_query_stream


def test_pricing_sonnet_vs_haiku() -> None:
    sonnet = pricing.estimate_cost_usd(
        "claude-sonnet-4-6", input_tokens=1_000_000, output_tokens=0
    )
    haiku = pricing.estimate_cost_usd(
        "claude-haiku-4-5-20251001", input_tokens=1_000_000, output_tokens=0
    )
    assert sonnet == 3.0
    assert haiku == 1.0
    assert pricing.estimate_cost_usd("claude-opus-4-1-20250805", input_tokens=1_000_000) == 15.0
    assert pricing.estimate_cost_usd("unknown-model", input_tokens=100) is None


def test_pricing_cache_multipliers() -> None:
    cost = pricing.estimate_cost_usd(
        "claude-sonnet-4-6",
        cache_read_tokens=1_000_000,
        cache_write_tokens=1_000_000,
    )
    assert cost == pytest.approx(3.0 * 0.1 + 3.0 * 1.25)


def test_run_handle_observes_dict_messages() -> None:
    handle = RunHandle("item_processor", ref="item-1", mode="normal", budget_usd=3.0)
    handle.observe_message(
        {"type": "system", "subtype": "init", "data": {"model": "claude-sonnet-4-6", "session_id": "s1"}}
    )
    handle.observe_message(
        {
            "type": "assistant",
            "model": "claude-sonnet-4-6",
            "content": [
                {"type": "thinking", "thinking": "hmm"},
                {"type": "tool_use", "id": "t1", "name": "find_company", "input": {"q": "acme"}},
            ],
        }
    )
    handle.observe_message(
        {
            "type": "user",
            "content": [
                {"type": "tool_result", "tool_use_id": "t1", "content": "no match", "is_error": True}
            ],
        }
    )
    handle.observe_message(
        {
            "type": "result",
            "subtype": "error_max_budget_usd",
            "num_turns": 7,
            "duration_ms": 5000,
            "duration_api_ms": 4000,
            "session_id": "s1",
            "usage": {"input_tokens": 100, "output_tokens": 10},
            "is_error": True,
        }
    )
    handle.finish()

    assert handle.status == "budget_exceeded"
    assert handle.session_id == "s1"
    assert handle.num_turns == 7
    # No total_cost_usd in the result -> estimated from tokens.
    assert handle.cost_estimated
    assert handle.cost_usd == pytest.approx((100 * 3 + 10 * 15) / 1e6)

    card = handle.build_tracecard()
    assert card["tools"] == {"find_company": "1(1 err)"}
    assert "tool_error:find_company" in card["flags"]
    assert "result_is_error" in card["flags"]

    # Tool step captured input+output with error flag.
    tool_steps = [s for s in handle.steps if s["kind"] == "tool"]
    assert len(tool_steps) == 1
    assert tool_steps[0]["is_error"] is True
    assert tool_steps[0]["output"] == "no match"


def test_run_handle_prefers_reported_cost() -> None:
    handle = RunHandle("lens")
    handle.observe_message(
        {"type": "result", "subtype": "success", "total_cost_usd": 0.42,
         "usage": {"input_tokens": 5, "output_tokens": 5}}
    )
    handle.finish()
    assert handle.cost_usd == 0.42
    assert not handle.cost_estimated
    assert handle.status == "succeeded"


async def test_tee_passes_messages_through_disabled(monkeypatch) -> None:
    monkeypatch.setenv("STRESSLESS_ENABLED", "0")

    async def stream():
        yield {"type": "assistant", "content": []}
        yield {"type": "result", "subtype": "success"}

    seen = [message async for message in tee_query_stream(stream())]
    assert len(seen) == 2


def test_backfill_parses_all_three_generations(tmp_path: Path) -> None:
    lines = [
        # B: structured flow + executor outcome
        {"ts": "2026-06-01T10:00:00+00:00", "event": "structured_agent_start",
         "raw_item_id": "item-1", "details": {"source": "rss", "title": "T"}},
        {"ts": "2026-06-01T10:00:30+00:00", "event": "structured_agent_complete",
         "raw_item_id": "item-1", "details": {"message_count": 9, "has_result": True}},
        {"ts": "2026-06-01T10:00:31+00:00", "event": "executor_complete",
         "raw_item_id": "item-1", "details": {"status": "completed"}},
        # C: enrich subcall with usage
        {"ts": "2026-06-01T11:00:00+00:00", "event": "enrich_narrative_llm_start",
         "raw_item_id": "co-1", "details": {"attempt": 1}},
        {"ts": "2026-06-01T11:00:05+00:00", "event": "enrich_narrative_llm_done",
         "raw_item_id": "co-1",
         "details": {"usage": {"input_tokens": 600, "output_tokens": 1400}, "parsed_ok": True}},
        # A: old MCP flow
        {"ts": "2026-06-01T12:00:00+00:00", "event": "item_start", "raw_item_id": "item-2",
         "details": {"mode": "normal", "source": "x", "max_budget_usd": 3.0}},
        {"ts": "2026-06-01T12:00:10+00:00", "event": "sdk_message", "raw_item_id": "item-2",
         "details": {"summary": {"message_kind": "tool_call"},
                     "payload": {"message_kind": "tool_call", "tool_name": "create_signal"}}},
        {"ts": "2026-06-01T12:00:40+00:00", "event": "item_stream_complete",
         "raw_item_id": "item-2", "details": {"mode": "normal"}},
    ]
    trace = tmp_path / "trace.jsonl"
    trace.write_text("\n".join(json.dumps(line) for line in lines))

    runs, steps = parse_trace_file(trace)
    by_kind = {}
    for row in runs:
        by_kind.setdefault(row["agent_kind"], []).append(row)

    structured = by_kind["item_processor"][0]
    assert structured["mode"] == "structured"
    assert structured["outcome"] == "completed"
    assert structured["duration_ms"] == 30_000

    enrich = by_kind["enrich_subcall"][0]
    assert enrich["input_tokens"] == 600
    assert enrich["cost_usd"] == pytest.approx((600 * 1 + 1400 * 5) / 1e6)

    old = by_kind["item_processor"][1]
    assert old["mode"] == "normal"
    assert old["budget_usd"] == 3.0
    old_steps = steps[runs.index(old)]
    assert len(old_steps) == 1  # the tool_call became a step

    # Idempotent ids: re-parsing yields identical run ids.
    runs2, _ = parse_trace_file(trace)
    assert [r["id"] for r in runs] == [r["id"] for r in runs2]
