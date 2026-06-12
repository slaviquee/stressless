"""Stressless CLI: python -m stressless {init-db|backfill|rules|report|smoke}"""

from __future__ import annotations

import argparse
import asyncio
import logging
from pathlib import Path

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")

REPO_ROOT = Path(__file__).resolve().parent.parent
MIGRATION = Path(__file__).resolve().parent / "schema.sql"
DEFAULT_TRACES = [
    REPO_ROOT / "logs" / "agent_trace.jsonl",
    REPO_ROOT / "logs" / "test_trace.jsonl",
]


async def _init_db() -> None:
    from . import store

    pool = await store.get_pool()
    sql = MIGRATION.read_text(encoding="utf-8")
    async with pool.acquire() as conn:
        await conn.execute(sql)
    print(f"applied {MIGRATION.name} to {store.database_url()}")
    await store.close_pool()


async def _backfill(paths: list[str]) -> None:
    from . import store
    from .backfill import backfill

    targets = [Path(p) for p in paths] if paths else [p for p in DEFAULT_TRACES if p.exists()]
    if not targets:
        print("no trace files found; pass paths explicitly")
        return
    totals = await backfill(targets)
    print(f"imported {totals['runs']} runs / {totals['steps']} steps from {totals['files']} file(s)")
    await store.close_pool()


async def _rules(days: int) -> None:
    from . import store
    from .rules import sweep

    counts = await sweep(days)
    print(f"rules: wrote {counts['scores']} scores, upserted {counts['findings']} findings")
    await store.close_pool()


async def _report(days: int) -> None:
    from . import store
    from .report import format_text, gather

    print(format_text(await gather(days)))
    await store.close_pool()


async def _smoke(live: bool) -> None:
    """Synthetic end-to-end check: fake SDK stream -> stressless rows. --live adds
    one minimal Haiku API call (<$0.001) through wrap_anthropic."""
    from . import run, store, tee_query_stream

    async def fake_stream():
        yield {"type": "system", "subtype": "init", "data": {"model": "claude-sonnet-4-6", "session_id": "smoke-session"}}
        yield {
            "type": "assistant",
            "model": "claude-sonnet-4-6",
            "content": [
                {"type": "text", "text": "checking the item"},
                {"type": "tool_use", "id": "t1", "name": "find_company", "input": {"name": "acme"}},
            ],
        }
        yield {
            "type": "user",
            "content": [{"type": "tool_result", "tool_use_id": "t1", "content": "no match", "is_error": False}],
        }
        yield {
            "type": "result",
            "subtype": "success",
            "num_turns": 2,
            "duration_ms": 1234,
            "duration_api_ms": 900,
            "session_id": "smoke-session",
            "total_cost_usd": 0.0123,
            "usage": {
                "input_tokens": 1000,
                "output_tokens": 50,
                "cache_read_input_tokens": 4000,
                "cache_creation_input_tokens": 500,
            },
            "result": "smoke ok",
        }

    async with run("smoke_test", ref="smoke-1", mode="normal", budget_usd=3.0) as handle:
        async for _ in tee_query_stream(fake_stream()):
            pass
    await asyncio.sleep(0.3)  # let fire-and-forget writers land

    pool = await store.get_pool()
    row = await pool.fetchrow(
        "SELECT status, cost_usd, num_turns, session_id, tracecard,"
        " (SELECT count(*) FROM stressless.steps s WHERE s.run_id = r.id) AS steps"
        " FROM stressless.runs r WHERE id = $1",
        handle.id,
    )
    assert row is not None, "smoke run row not written"
    assert row["status"] == "succeeded", row
    assert float(row["cost_usd"]) == 0.0123, row
    assert row["steps"] == 2, row  # text + tool (tool_result merges into the tool step)
    print(f"smoke ok: run {handle.id} status={row['status']} cost={row['cost_usd']} steps={row['steps']}")
    print(f"tracecard: {row['tracecard']}")

    if live:
        import anthropic as anthropic_sdk

        from . import wrap_anthropic

        client = wrap_anthropic(
            anthropic_sdk.AsyncAnthropic(), kind="smoke_live", meta={"smoke": True}
        )
        response = await client.messages.create(
            model="claude-haiku-4-5",
            max_tokens=8,
            messages=[{"role": "user", "content": "Reply with OK"}],
        )
        await asyncio.sleep(0.3)
        live_row = await pool.fetchrow(
            "SELECT model, cost_usd, input_tokens, output_tokens FROM stressless.runs "
            "WHERE agent_kind = 'smoke_live' ORDER BY created_at DESC LIMIT 1"
        )
        print(f"live smoke: api said {response.content[0].text!r}; recorded {dict(live_row) if live_row else None}")

    await store.close_pool()


async def _ingest_cma(session_ids: list[str], ingest_all: bool, kind: str | None) -> None:
    import anthropic

    from . import store
    from .cma import ingest_session

    client = anthropic.AsyncAnthropic()
    if ingest_all and not session_ids:
        sessions = await client.beta.sessions.list()
        session_ids = [s.id for s in (getattr(sessions, "data", None) or sessions)]
    if not session_ids:
        print("no session ids given (pass ids or --all)")
        return
    for session_id in session_ids:
        run_id = await ingest_session(client, session_id, kind=kind)
        print(f"ingested {session_id} -> run {run_id}")
    await store.close_pool()


async def _judge(days: int, sample: float, limit: int) -> None:
    import json

    import anthropic

    from . import store, wrap_anthropic
    from .judge import judge_recent

    client = wrap_anthropic(anthropic.AsyncAnthropic(), kind="judge")
    pool = await store.get_pool()
    summary = await judge_recent(pool, client, days=days, sample_rate=sample, limit=limit)
    print(json.dumps(summary, indent=2))
    await asyncio.sleep(0.5)
    await store.close_pool()


async def _init() -> None:
    await _init_db()
    print("""
Integration (pick what matches your stack):

  Claude Agent SDK — tee the query stream once at your call site:
      async for message in stressless.tee_query_stream(query(prompt=..., options=...)): ...
  Label logical jobs (repairs/subagents attach automatically):
      async with stressless.run("my_agent", ref=job_id, budget_usd=3.0): ...

  Raw Anthropic API:
      client = stressless.wrap_anthropic(AsyncAnthropic(), kind="classifier")

  Managed Agents (CMA):
      async for ev in stressless.tee_session_stream(stream, kind="researcher", session_id=sid): ...
      python -m stressless ingest-cma <session_id|--all>

Then:
      python -m stressless report --days 7      # spend, latency, failures, cache
      python -m stressless rules  --days 7      # deterministic findings (cron-able)
      python -m stressless judge  --days 1      # sampled LLM judge (cron-able)
Dashboard:  from stressless.web import router; app.include_router(router)  ->  /stressless
Env:        STRESSLESS_ENABLED=0 kill switch · STRESSLESS_CAPTURE_PROMPTS=0 sizes/SHAs only
""")


async def _dataset_add_run(run_id: str, dataset: str, split: str, tier: str) -> None:
    from . import store

    pool = await store.get_pool()
    run = await pool.fetchrow("SELECT * FROM stressless.runs WHERE id = $1::uuid", run_id)
    if run is None:
        print(f"run {run_id} not found")
        return
    steps = await pool.fetch(
        "SELECT kind, name, tool_use_id, input, output, is_error FROM stressless.steps"
        " WHERE run_id = $1::uuid ORDER BY idx", run_id,
    )
    cassette = [dict(s) for s in steps if s["kind"] in ("tool", "tool_result")]
    llm = next((dict(s) for s in steps if s["kind"] == "llm"), None)
    dataset_id = await pool.fetchval(
        """INSERT INTO stressless.datasets (agent_kind, name)
           VALUES ($1, $2)
           ON CONFLICT (agent_kind, name) DO UPDATE SET name = EXCLUDED.name
           RETURNING id""",
        run["agent_kind"], dataset,
    )
    await pool.execute(
        """INSERT INTO stressless.dataset_items
             (dataset_id, input, expected, source_run_id, split, tier, cassette)
           VALUES ($1, $2, $3, $4, $5, $6, $7)""",
        dataset_id,
        {"meta": run["meta"], "request": (llm or {}).get("input"), "ref": run["external_ref"]},
        {"outcome": run["outcome"], "status": run["status"],
         "result": (run["tracecard"] or {}).get("result")},
        run["id"], split, tier, cassette or None,
    )
    print(f"run {run_id} -> dataset '{dataset}' ({split}/{tier}, cassette steps: {len(cassette)})")
    await store.close_pool()


def main() -> None:
    parser = argparse.ArgumentParser(prog="stressless")
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("init-db", help="apply the stressless schema migration")

    backfill_parser = sub.add_parser("backfill", help="import agent_trace.jsonl files")
    backfill_parser.add_argument("paths", nargs="*", help="trace files (default: logs/*.jsonl)")

    rules_parser = sub.add_parser("rules", help="run deterministic rule packs")
    rules_parser.add_argument("--days", type=int, default=30)

    report_parser = sub.add_parser("report", help="print the cost/quality report")
    report_parser.add_argument("--days", type=int, default=7)

    judge_parser = sub.add_parser("judge", help="LLM-judge recent runs (failures 100%, successes sampled)")
    judge_parser.add_argument("--days", type=int, default=1)
    judge_parser.add_argument("--sample", type=float, default=0.10)
    judge_parser.add_argument("--limit", type=int, default=50)

    init_parser = sub.add_parser("init", help="apply the schema and print the integration guide")

    add_run_parser = sub.add_parser("dataset-add-run", help="harvest a run into a dataset (with tool cassette)")
    add_run_parser.add_argument("run_id")
    add_run_parser.add_argument("--dataset", required=True)
    add_run_parser.add_argument("--split", default="dev", choices=["dev", "holdout"])
    add_run_parser.add_argument("--tier", default="capability", choices=["capability", "regression"])

    ingest_parser = sub.add_parser("ingest-cma", help="ingest Managed Agents sessions by id (or --all)")
    ingest_parser.add_argument("session_ids", nargs="*")
    ingest_parser.add_argument("--all", action="store_true", help="ingest every listed session")
    ingest_parser.add_argument("--kind", default=None)

    smoke_parser = sub.add_parser("smoke", help="synthetic end-to-end collector check")
    smoke_parser.add_argument("--live", action="store_true", help="also make one tiny real Haiku call")

    args = parser.parse_args()
    if args.command == "init-db":
        asyncio.run(_init_db())
    elif args.command == "backfill":
        asyncio.run(_backfill(args.paths))
    elif args.command == "rules":
        asyncio.run(_rules(args.days))
    elif args.command == "report":
        asyncio.run(_report(args.days))
    elif args.command == "judge":
        asyncio.run(_judge(args.days, args.sample, args.limit))
    elif args.command == "init":
        asyncio.run(_init())
    elif args.command == "dataset-add-run":
        asyncio.run(_dataset_add_run(args.run_id, args.dataset, args.split, args.tier))
    elif args.command == "ingest-cma":
        asyncio.run(_ingest_cma(args.session_ids, args.all, args.kind))
    elif args.command == "smoke":
        asyncio.run(_smoke(args.live))


if __name__ == "__main__":
    main()
