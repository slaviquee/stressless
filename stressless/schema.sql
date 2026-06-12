-- Stressless: agent observability + eval store.
-- Applied via `python -m stressless init-db` (asyncpg) or psql. Idempotent.

CREATE SCHEMA IF NOT EXISTS stressless;

-- One logical job by one agent kind (an item processed end-to-end, a Lens answer, ...).
CREATE TABLE IF NOT EXISTS stressless.runs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  agent_kind TEXT NOT NULL,            -- item_processor | prefilter | lens | dashboard | enrich | ...
  external_ref TEXT,                   -- raw_item_id, thread_id, dashboard_id
  session_id TEXT,                     -- Agent SDK session id (transcript key)
  parent_run_id UUID REFERENCES stressless.runs(id) ON DELETE SET NULL,  -- repairs, subagents
  attempt INT NOT NULL DEFAULT 1,
  mode TEXT NOT NULL DEFAULT 'normal', -- normal | repair_N | replay | experiment | backfill
  model TEXT,
  status TEXT NOT NULL DEFAULT 'running',  -- running|succeeded|failed|timeout|budget_exceeded
  outcome TEXT,                        -- domain outcome (raw_items.status etc.)
  stop_subtype TEXT,                   -- ResultMessage.subtype
  error TEXT,
  num_turns INT,
  duration_ms BIGINT,
  duration_api_ms BIGINT,
  input_tokens BIGINT,
  output_tokens BIGINT,
  cache_read_tokens BIGINT,
  cache_write_tokens BIGINT,
  cost_usd NUMERIC(12,6),
  cost_estimated BOOLEAN NOT NULL DEFAULT false,  -- true when derived from tokens, not SDK-reported
  budget_usd NUMERIC(12,6),
  model_usage JSONB,                   -- per-model breakdown when the SDK provides it
  git_sha TEXT,
  config_hash TEXT,
  meta JSONB NOT NULL DEFAULT '{}',    -- host extras: source, batch id, ...
  tracecard JSONB,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS runs_kind_created ON stressless.runs (agent_kind, created_at DESC);
CREATE INDEX IF NOT EXISTS runs_external_ref ON stressless.runs (external_ref);
CREATE INDEX IF NOT EXISTS runs_session ON stressless.runs (session_id);
CREATE INDEX IF NOT EXISTS runs_parent ON stressless.runs (parent_run_id) WHERE parent_run_id IS NOT NULL;

-- A turn / tool call / subagent / hook event within a run.
CREATE TABLE IF NOT EXISTS stressless.steps (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID NOT NULL REFERENCES stressless.runs(id) ON DELETE CASCADE,
  idx INT NOT NULL,
  kind TEXT NOT NULL,                  -- llm | tool | tool_result | thinking | text | subagent | hook
  name TEXT,                           -- tool name, hook event, model
  tool_use_id TEXT,
  input JSONB,
  output JSONB,
  input_sha TEXT,
  output_sha TEXT,
  input_bytes INT,
  output_bytes INT,
  is_error BOOLEAN NOT NULL DEFAULT false,
  duration_ms BIGINT,
  tokens JSONB,
  ts TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS steps_run ON stressless.steps (run_id, idx);
CREATE INDEX IF NOT EXISTS steps_tool ON stressless.steps (name) WHERE kind = 'tool';

-- One schema for rule, judge AND human verdicts.
CREATE TABLE IF NOT EXISTS stressless.scores (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id UUID REFERENCES stressless.runs(id) ON DELETE CASCADE,
  step_id UUID REFERENCES stressless.steps(id) ON DELETE CASCADE,
  name TEXT NOT NULL,                  -- budget_proximity | semantic_loop | dedup_checked | ...
  source TEXT NOT NULL,                -- rule | judge | human
  data_type TEXT NOT NULL,             -- boolean | numeric | categorical
  value_num DOUBLE PRECISION,
  value_text TEXT,
  reasoning TEXT,
  judge_model TEXT,
  config_id UUID,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS scores_run ON stressless.scores (run_id);
CREATE INDEX IF NOT EXISTS scores_name ON stressless.scores (name, created_at DESC);

CREATE TABLE IF NOT EXISTS stressless.datasets (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  agent_kind TEXT NOT NULL,
  name TEXT NOT NULL,
  description TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (agent_kind, name)
);

-- Append-only; experiments pin an as-of timestamp.
CREATE TABLE IF NOT EXISTS stressless.dataset_items (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dataset_id UUID NOT NULL REFERENCES stressless.datasets(id) ON DELETE CASCADE,
  input JSONB NOT NULL,
  expected JSONB,
  source_run_id UUID,
  split TEXT NOT NULL DEFAULT 'dev',        -- dev | holdout
  tier TEXT NOT NULL DEFAULT 'capability',  -- capability | regression
  cassette JSONB,                           -- recorded tool results for offline replay
  archived_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS dataset_items_ds ON stressless.dataset_items (dataset_id);

CREATE TABLE IF NOT EXISTS stressless.experiments (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  dataset_id UUID NOT NULL REFERENCES stressless.datasets(id),
  dataset_as_of TIMESTAMPTZ NOT NULL DEFAULT now(),
  name TEXT,
  variant JSONB NOT NULL,              -- {prompt_patch | options_patch | skill_patch | model}
  trials_per_item INT NOT NULL DEFAULT 3,
  baseline_experiment_id UUID REFERENCES stressless.experiments(id),
  status TEXT NOT NULL DEFAULT 'running',
  summary JSONB,
  conclusion TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Sentry-style issues; PRs hang off findings, not raw runs.
CREATE TABLE IF NOT EXISTS stressless.findings (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  agent_kind TEXT,
  kind TEXT NOT NULL,                  -- cost_outlier|loop|tool_thrash|cache_miss|failure_cluster|model_routing
  title TEXT NOT NULL,
  detail JSONB,
  severity TEXT NOT NULL DEFAULT 'info',   -- info | low | medium | high
  status TEXT NOT NULL DEFAULT 'open',     -- open|proposed|pr_open|merged|dismissed
  fingerprint TEXT UNIQUE,
  example_run_ids UUID[],
  occurrences INT NOT NULL DEFAULT 1,
  est_impact JSONB,                    -- {usd_per_month, p50_ms, quality_risk}
  first_seen TIMESTAMPTZ NOT NULL DEFAULT now(),
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS findings_status ON stressless.findings (status, severity);

CREATE TABLE IF NOT EXISTS stressless.proposals (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  finding_id UUID REFERENCES stressless.findings(id) ON DELETE CASCADE,
  category TEXT NOT NULL,              -- cheaper|faster|better|reliability|eval
  patch_summary TEXT,
  experiment_id UUID REFERENCES stressless.experiments(id),
  pr_url TEXT,
  pr_state TEXT,                       -- draft|open|merged|closed
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
