"""Deterministic rule packs: free, explainable checks over collected runs.

Every rule writes stressless.scores rows (source='rule') and aggregates into
deduped stressless.findings (fingerprinted, occurrence-counted). Run via
``python -m stressless rules`` or the 30-min cron.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from . import pricing, store

logger = logging.getLogger(__name__)


async def _upsert_finding(
    pool: Any,
    *,
    fingerprint: str,
    kind: str,
    agent_kind: str | None,
    title: str,
    detail: dict[str, Any],
    severity: str,
    occurrences: int,
    example_run_ids: list[Any],
    est_impact: dict[str, Any] | None = None,
) -> None:
    await pool.execute(
        """
        INSERT INTO stressless.findings
          (fingerprint, kind, agent_kind, title, detail, severity, occurrences,
           example_run_ids, est_impact)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
        ON CONFLICT (fingerprint) DO UPDATE SET
          title = EXCLUDED.title,
          detail = EXCLUDED.detail,
          severity = EXCLUDED.severity,
          occurrences = EXCLUDED.occurrences,
          example_run_ids = EXCLUDED.example_run_ids,
          est_impact = EXCLUDED.est_impact,
          last_seen = now()
        """,
        fingerprint,
        kind,
        agent_kind,
        title,
        detail,
        severity,
        occurrences,
        example_run_ids[:5],
        est_impact,
    )


async def sweep(days: int = 30) -> dict[str, int]:
    """Run all rules over the window; idempotent (re-runs replace rule scores)."""
    pool = await store.get_pool()
    window = timedelta(days=days)
    counts = {"scores": 0, "findings": 0}

    await pool.execute(
        "DELETE FROM stressless.scores WHERE source = 'rule' AND run_id IN "
        "(SELECT id FROM stressless.runs WHERE created_at > now() - $1::interval)",
        window,
    )

    score_rows: list[tuple] = []

    # -- Rule: abnormal stop subtypes ------------------------------------------
    abnormal = await pool.fetch(
        "SELECT id, agent_kind, stop_subtype FROM stressless.runs "
        "WHERE created_at > now() - $1::interval AND stop_subtype LIKE 'error%'",
        window,
    )
    for row in abnormal:
        score_rows.append(
            (row["id"], "abnormal_stop", "rule", "categorical", None, row["stop_subtype"])
        )
    clusters: dict[tuple[str, str], list[Any]] = {}
    for row in abnormal:
        clusters.setdefault((row["agent_kind"], row["stop_subtype"]), []).append(row["id"])
    for (agent_kind, subtype), ids in clusters.items():
        await _upsert_finding(
            pool,
            fingerprint=f"failure_cluster:{agent_kind}:{subtype}",
            kind="failure_cluster",
            agent_kind=agent_kind,
            title=f"{agent_kind}: {len(ids)} runs stopped with {subtype} (last {days}d)",
            detail={"stop_subtype": subtype, "window_days": days},
            severity="high" if subtype == "error_max_budget_usd" else "medium",
            occurrences=len(ids),
            example_run_ids=ids,
        )
        counts["findings"] += 1

    # -- Rule: failed/timeout run clusters (works for backfilled runs too) ------
    failed_clusters = await pool.fetch(
        """
        SELECT agent_kind, status, count(*) AS n, (array_agg(id))[1:5] AS examples
        FROM stressless.runs
        WHERE created_at > now() - $1::interval
          AND status NOT IN ('succeeded', 'running')
        GROUP BY agent_kind, status
        HAVING count(*) >= 3
        """,
        window,
    )
    for row in failed_clusters:
        await _upsert_finding(
            pool,
            fingerprint=f"failure_cluster:{row['agent_kind']}:status:{row['status']}",
            kind="failure_cluster",
            agent_kind=row["agent_kind"],
            title=f"{row['agent_kind']}: {row['n']} runs ended {row['status']} (last {days}d)",
            detail={"status": row["status"], "window_days": days},
            severity="high" if row["status"] == "budget_exceeded" else "medium",
            occurrences=row["n"],
            example_run_ids=list(row["examples"] or []),
        )
        counts["findings"] += 1

    # -- Rule: near budget ------------------------------------------------------
    near_budget = await pool.fetch(
        "SELECT id FROM stressless.runs WHERE created_at > now() - $1::interval "
        "AND budget_usd IS NOT NULL AND cost_usd IS NOT NULL "
        "AND cost_usd >= 0.8 * budget_usd",
        window,
    )
    for row in near_budget:
        score_rows.append((row["id"], "budget_proximity", "rule", "boolean", 1.0, None))

    # -- Rule: semantic loop (same tool + same args >= 3x in one run) -----------
    loops = await pool.fetch(
        """
        SELECT s.run_id, s.name, count(*) AS reps, r.agent_kind
        FROM stressless.steps s
        JOIN stressless.runs r ON r.id = s.run_id
        WHERE s.kind = 'tool' AND s.input_sha IS NOT NULL
          AND r.created_at > now() - $1::interval
        GROUP BY s.run_id, s.name, s.input_sha, r.agent_kind
        HAVING count(*) >= 3
        """,
        window,
    )
    for row in loops:
        score_rows.append(
            (row["run_id"], "semantic_loop", "rule", "numeric", float(row["reps"]), row["name"])
        )
    if loops:
        by_kind: dict[str, list[Any]] = {}
        for row in loops:
            by_kind.setdefault(row["agent_kind"], []).append(row["run_id"])
        for agent_kind, ids in by_kind.items():
            await _upsert_finding(
                pool,
                fingerprint=f"loop:{agent_kind}",
                kind="loop",
                agent_kind=agent_kind,
                title=f"{agent_kind}: semantic tool loops in {len(ids)} runs (same tool+args ≥3x)",
                detail={"window_days": days},
                severity="medium",
                occurrences=len(ids),
                example_run_ids=ids,
            )
            counts["findings"] += 1

    # -- Rule: oversized tool outputs -------------------------------------------
    oversized = await pool.fetch(
        """
        SELECT s.name, count(*) AS hits, max(s.output_bytes) AS max_bytes,
               (array_agg(s.run_id))[1:5] AS examples, r.agent_kind
        FROM stressless.steps s
        JOIN stressless.runs r ON r.id = s.run_id
        WHERE s.output_bytes > $2 AND r.created_at > now() - $1::interval
        GROUP BY s.name, r.agent_kind
        ORDER BY hits DESC
        """,
        window,
        store.TRUNCATE_CHARS,
    )
    for row in oversized:
        await _upsert_finding(
            pool,
            fingerprint=f"oversized_tool_output:{row['agent_kind']}:{row['name']}",
            kind="tool_thrash",
            agent_kind=row["agent_kind"],
            title=(
                f"{row['agent_kind']}: tool {row['name']} returned >"
                f"{store.TRUNCATE_CHARS // 1000}kB output {row['hits']}x "
                f"(max {row['max_bytes']} bytes) — head+tail truncation candidate"
            ),
            detail={"tool": row["name"], "hits": row["hits"], "max_bytes": row["max_bytes"]},
            severity="low",
            occurrences=row["hits"],
            example_run_ids=list(row["examples"] or []),
        )
        counts["findings"] += 1

    # -- Rule: cache-cold SDK runs ----------------------------------------------
    cache_cold = await pool.fetch(
        """
        SELECT id, agent_kind FROM stressless.runs
        WHERE created_at > now() - $1::interval
          AND num_turns >= 3 AND COALESCE(cache_read_tokens, 0) = 0
          AND COALESCE(input_tokens, 0) + COALESCE(cache_write_tokens, 0) > 20000
        """,
        window,
    )
    for row in cache_cold:
        score_rows.append((row["id"], "cache_cold", "rule", "boolean", 1.0, None))
    if cache_cold:
        by_kind = {}
        for row in cache_cold:
            by_kind.setdefault(row["agent_kind"], []).append(row["id"])
        for agent_kind, ids in by_kind.items():
            await _upsert_finding(
                pool,
                fingerprint=f"cache_miss:{agent_kind}",
                kind="cache_miss",
                agent_kind=agent_kind,
                title=f"{agent_kind}: {len(ids)} multi-turn runs with zero cache reads",
                detail={"window_days": days},
                severity="medium",
                occurrences=len(ids),
                example_run_ids=ids,
            )
            counts["findings"] += 1

    # -- Rule: tool error clusters ------------------------------------------------
    tool_errors = await pool.fetch(
        """
        SELECT s.name, count(*) AS errs, (array_agg(s.run_id))[1:5] AS examples,
               r.agent_kind
        FROM stressless.steps s
        JOIN stressless.runs r ON r.id = s.run_id
        WHERE s.is_error AND s.kind IN ('tool', 'tool_result')
          AND r.created_at > now() - $1::interval
        GROUP BY s.name, r.agent_kind
        HAVING count(*) >= 3
        ORDER BY errs DESC
        """,
        window,
    )
    for row in tool_errors:
        await _upsert_finding(
            pool,
            fingerprint=f"tool_errors:{row['agent_kind']}:{row['name']}",
            kind="tool_thrash",
            agent_kind=row["agent_kind"],
            title=f"{row['agent_kind']}: tool {row['name']} errored {row['errs']}x — retry/backoff candidate",
            detail={"tool": row["name"], "errors": row["errs"]},
            severity="medium",
            occurrences=row["errs"],
            example_run_ids=list(row["examples"] or []),
        )
        counts["findings"] += 1

    # -- Finding: prefilter still on Sonnet (model routing) -----------------------
    prefilter = await pool.fetchrow(
        """
        SELECT count(*) AS calls, sum(cost_usd) AS cost,
               sum(input_tokens) AS in_tok, sum(output_tokens) AS out_tok,
               (array_agg(id))[1:5] AS examples
        FROM stressless.runs
        WHERE agent_kind = 'prefilter' AND model LIKE 'claude-sonnet%'
          AND created_at > now() - $1::interval
        """,
        window,
    )
    if not prefilter or not (prefilter["calls"] or 0):
        # Prefilter was never instrumented (the historical telemetry gap) —
        # derive the finding from static config + raw_items volume instead.
        await _prefilter_config_finding(pool, days)
        counts["findings"] += 1
    if prefilter and (prefilter["calls"] or 0) > 0 and prefilter["cost"]:
        haiku_cost = pricing.estimate_cost_usd(
            "claude-haiku-4-5",
            input_tokens=int(prefilter["in_tok"] or 0),
            output_tokens=int(prefilter["out_tok"] or 0),
        )
        saving = float(prefilter["cost"]) - (haiku_cost or 0.0)
        monthly = saving / days * 30
        await _upsert_finding(
            pool,
            fingerprint="model_routing:prefilter:sonnet",
            kind="model_routing",
            agent_kind="prefilter",
            title=(
                f"prefilter runs on Sonnet (config says 'temporarily') — "
                f"{prefilter['calls']} calls, ${float(prefilter['cost']):.2f} in {days}d; "
                f"Haiku at same tokens ≈ ${haiku_cost:.2f} (−{(saving / float(prefilter['cost'])) * 100:.0f}%)"
            ),
            detail={
                "calls": prefilter["calls"],
                "cost_usd": float(prefilter["cost"]),
                "haiku_cost_usd": haiku_cost,
                "config_ref": "config.PREFILTER_MODEL (config.py:118-121)",
                "verification": "replay prefilter inputs through both models before switching",
            },
            severity="high",
            occurrences=int(prefilter["calls"]),
            example_run_ids=list(prefilter["examples"] or []),
            est_impact={"usd_per_month": round(monthly, 2)},
        )
        counts["findings"] += 1

    # -- Finding: repair rate -------------------------------------------------------
    repair = await pool.fetchrow(
        """
        SELECT count(*) FILTER (WHERE mode LIKE 'repair%') AS repairs,
               count(*) FILTER (WHERE mode IN ('normal', 'structured')) AS normals
        FROM stressless.runs
        WHERE agent_kind = 'item_processor' AND created_at > now() - $1::interval
        """,
        window,
    )
    if repair and (repair["repairs"] or 0) > 0:
        rate = repair["repairs"] / max(repair["normals"] or 1, 1) * 100
        await _upsert_finding(
            pool,
            fingerprint="failure_cluster:item_processor:repair_rate",
            kind="failure_cluster",
            agent_kind="item_processor",
            title=f"item_processor: repair continuations fired {repair['repairs']}x ({rate:.1f}% of runs, {days}d)",
            detail={"repairs": repair["repairs"], "normals": repair["normals"]},
            severity="medium" if rate > 5 else "low",
            occurrences=int(repair["repairs"]),
            example_run_ids=[],
        )
        counts["findings"] += 1

    if score_rows:
        await pool.executemany(
            "INSERT INTO stressless.scores (run_id, name, source, data_type, value_num, value_text) "
            "VALUES ($1,$2,$3,$4,$5,$6)",
            score_rows,
        )
    counts["scores"] = len(score_rows)
    return counts


async def _prefilter_config_finding(pool: Any, days: int) -> None:
    """Static-config finding: prefilter pinned to Sonnet 'temporarily'.

    No prefilter run was ever recorded (the pre-Stressless telemetry gap), so the
    impact estimate is derived from raw_items volume and the prompt-size cap in
    config — explicitly labeled estimated until live runs refine it.
    """
    try:
        import config

        model = getattr(config, "PREFILTER_MODEL", "")
        max_chars = int(getattr(config, "PREFILTER_MAX_TEXT_CHARS", 1500))
    except Exception:
        return
    if not model.startswith("claude-sonnet"):
        return

    volume = await pool.fetchval(
        "SELECT count(*) FROM raw_items WHERE created_at > now() - $1::interval",
        timedelta(days=days),
    )
    volume = int(volume or 0)
    if not volume:
        return
    # ~4 chars/token on prompt text + ~250-token template, ~60 output tokens.
    est_in = max_chars // 4 + 250
    est_out = 60
    per_call_sonnet = pricing.estimate_cost_usd(model, input_tokens=est_in, output_tokens=est_out) or 0
    per_call_haiku = (
        pricing.estimate_cost_usd("claude-haiku-4-5", input_tokens=est_in, output_tokens=est_out)
        or 0
    )
    monthly_saving = (per_call_sonnet - per_call_haiku) * volume / days * 30
    await _upsert_finding(
        pool,
        fingerprint="model_routing:prefilter:sonnet",
        kind="model_routing",
        agent_kind="prefilter",
        title=(
            f"prefilter pinned to {model} — config comment says 'temporarily', "
            f"~{volume:,} candidate items/{days}d; Haiku would cut the call cost ~67% "
            f"(volume-based estimate — uninstrumented until Stressless wiring ships)"
        ),
        detail={
            "model": model,
            "config_ref": "config.PREFILTER_MODEL (config.py:118-121)",
            "items_in_window": volume,
            "est_per_call_sonnet_usd": round(per_call_sonnet, 5),
            "est_per_call_haiku_usd": round(per_call_haiku, 5),
            "verification": "replay prefilter inputs through both models before switching",
        },
        severity="high",
        occurrences=volume,
        example_run_ids=[],
        est_impact={"usd_per_month": round(monthly_saving, 2), "estimated": True},
    )
