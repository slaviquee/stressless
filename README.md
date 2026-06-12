# stressless

**Production observability for [Claude Agent SDK](https://code.claude.com/docs/en/agent-sdk/overview) apps — watch every agent run, score it, surface the waste.**

## The problem

Agents in production leak money in ways nobody is watching: a "temporary" expensive-model fallback that becomes permanent, an agent retrying the same tool call with the same arguments, multi-turn runs paying full price because the prompt cache never hits, jobs creeping toward their budget cap. The Claude Agent SDK already reports everything needed to catch this — `total_cost_usd`, per-model token splits, cache stats and turn counts on every result message — and most pipelines iterate straight past it, so the evidence is dropped at the moment it's emitted.

You *can* ship that stream to an observability platform (the SDK even exports OpenTelemetry), but the platforms want a backend stack (ClickHouse, Redis, S3), a proxy in front of your API traffic, or a SaaS vendor — at minimum another service to deploy and keep alive. And they stop at traces, dashboards and LLM-judge evals: spotting the waste in them is still your job.

## The solution

stressless is a small library, not a platform. It keeps the whole loop on infrastructure you already run and makes the waste-finding deterministic:

1. **Capture** — wrap your SDK query stream once; every message passes through unchanged while tool calls, tokens, cache hits, turns and cost are recorded per run. A second wrapper covers raw `AsyncAnthropic` call sites.
2. **Store** — fire-and-forget writes into a `stressless` schema in your existing Postgres. No proxy, no new service; it never blocks and never raises into the agent path.
3. **Judge** — free, deterministic rule packs sweep recent runs for the known waste patterns: semantic tool loops, cache-cold runs, budget proximity, oversized tool outputs, tool-error clusters, model-routing waste. Recurring problems dedupe into Sentry-style **findings** with estimated dollar impact.
4. **Report** — a terminal report and a single-file dashboard answer "what did today cost, per agent, and where's the waste?"

Extracted from a production deployment running 13 agent surfaces on one box — where a prefilter pinned to an expensive model "temporarily" sat uninstrumented at ~3× the cost a small model could serve; exactly the kind of thing the rule packs now flag with a monthly-savings estimate.

## What's inside

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
python -m stressless rules --days 7    # rule packs -> scores + findings (cron-able)
python -m stressless judge --days 1    # LLM judge: failures 100%, successes sampled (cron-able)
python -m stressless smoke             # synthetic end-to-end check (--live adds one tiny Haiku call)
python -m stressless dataset-add-run <run_id> --dataset golden   # harvest run + tool cassette
```

New install? `python -m stressless init` applies the schema and prints this whole guide.

### Continuous LLM judge

`stressless judge` grades recent captured calls against per-kind rubrics
(`stressless.judge.RUBRICS` — add your own): all failures, plus a sample of
successes (default 10%). Reasoning-before-verdict, binary verdicts with an
`unknown` escape, a stronger-tier judge model by default
(`STRESSLESS_JUDGE_MODEL`, default Opus), and every verdict stored in
`stressless.scores (source='judge')` with the judge's own calls recorded under
`kind=judge`. A high incorrect-rate auto-opens a finding. Judges are advisory
until you've checked their alignment against your own labels — they corroborate
the deterministic gates, not replace them. Request prompts are captured
truncated for judging; set `STRESSLESS_CAPTURE_PROMPTS=0` to store sizes/SHAs
only.

Dashboard (mount into any FastAPI app; served localhost-only):

```python
from stressless.web import router as stressless_router
app.include_router(stressless_router)   # GET /stressless
```

## Managed Agents (CMA)

Anthropic-hosted agents (beta `managed-agents-2026-04-01`) are captured from the
session **event stream** — tool use/results, per-request token usage from
`span.model_request_end`, stop reasons, outcome-grader verdicts:

```python
# your orchestrator already holds the stream — tee it:
stream = await client.beta.sessions.events.stream(session_id=session.id)
async for event in stressless.tee_session_stream(stream, kind="researcher", session_id=session.id, model="claude-haiku-4-5"):
    ...

# or capture after the fact (CMA retains full event history server-side):
await stressless.ingest_session(client, session_id)        # same run row — deterministic id
```

```bash
python -m stressless ingest-cma <session_id> [...]   # or --all
```

CMA reports tokens, never dollars — costs are estimated from the pricing table
and flagged. Live example: `examples/cma_live_proof.py` (one Haiku session, <$0.01).

## Design constraints

- **Never blocks, never raises into the host.** All writes are fire-and-forget tasks on a dedicated 3-connection pool; payloads truncate client-side (full-payload SHA kept); storage failures log one throttled warning and drop the write. `STRESSLESS_ENABLED=0` is a hard kill switch.
- **Runs on your infra.** A `stressless` schema in your existing Postgres. No vendor, no proxy, no new service.
- **SDK-version safe.** Works against `claude-agent-sdk` 0.1.48+; newer `ResultMessage` fields (`model_usage`, `permission_denials`, `stop_reason`, `errors`, `api_error_status`, `uuid`) are read via `getattr` and populate automatically after an SDK upgrade. Server-side tool calls (`ServerToolUseBlock`/`ServerToolResultBlock`, e.g. web search and web fetch) are recorded as steps just like client tool calls.
- **Estimates are labeled.** SDK-reported `total_cost_usd` wins; token-derived costs are flagged `cost_estimated`.

## Configuration (env)

| Var | Default | Meaning |
|---|---|---|
| `STRESSLESS_ENABLED` | `1` | Kill switch — `0` disables all capture |
| `STRESSLESS_DATABASE_URL` | host `config.DATABASE_URL` / `DATABASE_URL` | Store location |
| `STRESSLESS_TRUNCATE_CHARS` | `8000` | Step payload truncation |

## Status & roadmap

v0.2 — **Watch** (Agent SDK + raw API + Managed Agents) and **Judge** (rule
packs + findings, plus the sampled LLM judge with rubrics) running in
production; datasets harvested from real runs with tool cassettes attached;
experiments + proposals rendered as improvement-loop stories on the dashboard.
Proven end to end on a production deployment: a replay-verified model-routing
change shipped as an evidence-bearing PR (−65% cost at quality parity), and a
cache hypothesis was refuted by measurement — no PR, gate held.

Next: cassette *replay* (re-run the real agent offline in CI against recorded
tool results), judge↔human alignment gating (κ before a judge may gate),
and the **Improve** loop — a nightly agent that turns findings into
replay-verified PRs automatically. The ladder tops out at "open a PR": never
auto-merge, never intercept live runs.

## License

Apache-2.0
