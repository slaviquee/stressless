# stressless

**Production observability for [Claude Agent SDK](https://code.claude.com/docs/en/agent-sdk/overview) apps — watch every agent run, score it, surface the waste.**

Most teams running agents in production can't answer the most basic question about them: *what does one run cost?* The SDK hands back `total_cost_usd`, per-model token splits, cache stats and turn counts on every result message — and most pipelines iterate straight past it. stressless captures all of it with a few lines of integration, stores it in the Postgres you already run, and turns it into per-agent cost curves, deterministic quality checks, and a findings inbox.

Extracted from a production deployment running 13 agent surfaces on one box.

## What it does

- **Collector** — wrap your SDK query stream once and every run is recorded: tool calls/results as steps (sizes, latency, error flags), cost, cache hit ratio, turns, stop reason, session id. A second wrapper covers raw `AsyncAnthropic().messages.create` call sites with cache-aware cost estimation.
- **TraceCards** — every finished run distills to a ~1.5 kB summary (model, turns, tokens, tools, flags, result preview) that downstream analysis reads instead of raw transcripts.
- **Rule packs** — free, deterministic checks on every run: abnormal stops, budget proximity, semantic tool loops (same tool + same args ≥3×), oversized tool outputs, cache-cold multi-turn runs, tool-error clusters, model-routing waste. Rules write scores and aggregate into fingerprinted, deduped **findings** with impact estimates.
- **Backfill** — import historical agent trace JSONL so the dashboard has data on day one (idempotent, deterministic run ids).
- **Report + dashboard** — a terminal report and a single-file FastAPI page answering "what did today cost, per agent, and where's the waste?"

## Quickstart

```bash
pip install "stressless[web] @ git+https://github.com/slaviquee/stressless"
export DATABASE_URL=postgresql://localhost:5432/yourdb
python -m stressless init-db          # creates the `stressless` schema
```

Instrument the two choke points:

```python
import stressless

# 1. Claude Agent SDK — tee the query stream (zero behavior change):
async for message in stressless.tee_query_stream(query(prompt=..., options=...)):
    ...

# Group one logical job (repairs/subagents attach to it), label it:
async with stressless.run("item_processor", ref=item_id, budget_usd=3.0):
    ...  # any SDK queries inside are captured under this run

# 2. Raw Anthropic API call sites:
client = stressless.wrap_anthropic(AsyncAnthropic(), kind="prefilter")
```

Then:

```bash
python -m stressless report --days 7   # spend by agent kind, p50/p95, fail %, cache hit %
python -m stressless rules --days 7    # rule packs -> scores + findings
python -m stressless smoke             # synthetic end-to-end check (--live adds one tiny Haiku call)
```

Dashboard (mount into any FastAPI app; served localhost-only):

```python
from stressless.web import router as stressless_router
app.include_router(stressless_router)   # GET /stressless
```

## Design constraints

- **Never blocks, never raises into the host.** All writes are fire-and-forget tasks on a dedicated 3-connection pool; payloads truncate client-side (full-payload SHA kept); storage failures log one throttled warning and drop the write. `STRESSLESS_ENABLED=0` is a hard kill switch.
- **Runs on your infra.** A `stressless` schema in your existing Postgres. No vendor, no proxy, no new service.
- **SDK-version safe.** Works against `claude-agent-sdk` 0.1.48+; newer `ResultMessage` fields (`model_usage`, `permission_denials`) are read via `getattr` and populate automatically after an SDK upgrade.
- **Estimates are labeled.** SDK-reported `total_cost_usd` wins; token-derived costs are flagged `cost_estimated`.

## Configuration (env)

| Var | Default | Meaning |
|---|---|---|
| `STRESSLESS_ENABLED` | `1` | Kill switch — `0` disables all capture |
| `STRESSLESS_DATABASE_URL` | host `config.DATABASE_URL` / `DATABASE_URL` | Store location |
| `STRESSLESS_TRUNCATE_CHARS` | `8000` | Step payload truncation |

## Status & roadmap

v0.1 — the **Watch** loop plus the first slice of **Judge** (rule packs + findings), running in production. Next, in order: sampled LLM-as-judge rubrics with human-alignment gating, golden datasets harvested from production runs, cassette replay (record tool results, re-run the real agent offline in CI), and the **Improve** loop — a nightly agent that turns findings into replay-verified, evidence-bearing pull requests. The ladder tops out at "open a PR": never auto-merge, never intercept live runs.

## License

Apache-2.0
