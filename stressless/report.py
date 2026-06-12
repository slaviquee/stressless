"""Aggregate queries + terminal report. The /stressless dashboard reuses gather()."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from . import store


async def gather(days: int = 7) -> dict[str, Any]:
    pool = await store.get_pool()
    window = timedelta(days=days)

    by_kind = await pool.fetch(
        """
        SELECT agent_kind,
               count(*) AS runs,
               sum(cost_usd) AS cost_usd,
               bool_or(cost_estimated) AS any_estimated,
               count(*) FILTER (WHERE cost_usd IS NULL) AS cost_unknown,
               percentile_cont(0.5) WITHIN GROUP (ORDER BY duration_ms) AS p50_ms,
               percentile_cont(0.95) WITHIN GROUP (ORDER BY duration_ms) AS p95_ms,
               count(*) FILTER (WHERE status NOT IN ('succeeded', 'running')) AS failed,
               sum(cache_read_tokens) AS cache_read,
               sum(input_tokens) AS input_tokens,
               sum(cache_write_tokens) AS cache_write,
               sum(output_tokens) AS output_tokens
        FROM stressless.runs
        WHERE created_at > now() - $1::interval
        GROUP BY agent_kind
        ORDER BY cost_usd DESC NULLS LAST, runs DESC
        """,
        window,
    )

    daily = await pool.fetch(
        """
        SELECT date_trunc('day', created_at)::date AS day,
               count(*) AS runs, sum(cost_usd) AS cost_usd
        FROM stressless.runs
        WHERE created_at > now() - $1::interval
        GROUP BY 1 ORDER BY 1 DESC
        """,
        window,
    )

    by_source = await pool.fetch(
        """
        SELECT COALESCE(meta->>'source', '?') AS source,
               count(*) AS runs,
               sum(cost_usd) AS cost_usd,
               count(*) FILTER (WHERE outcome = 'completed') AS completed,
               count(*) FILTER (WHERE outcome = 'not_relevant') AS not_relevant,
               count(*) FILTER (WHERE outcome = 'duplicate') AS duplicate,
               count(*) FILTER (WHERE status NOT IN ('succeeded', 'running')) AS failed
        FROM stressless.runs
        WHERE agent_kind = 'item_processor' AND created_at > now() - $1::interval
        GROUP BY 1 ORDER BY runs DESC LIMIT 15
        """,
        window,
    )

    findings = await pool.fetch(
        """
        SELECT kind, agent_kind, title, severity, occurrences, est_impact, last_seen
        FROM stressless.findings
        WHERE status = 'open'
        ORDER BY CASE severity WHEN 'high' THEN 0 WHEN 'medium' THEN 1
                 WHEN 'low' THEN 2 ELSE 3 END, occurrences DESC
        LIMIT 20
        """
    )

    recent = await pool.fetch(
        """
        SELECT id, agent_kind, external_ref, mode, model, status, outcome,
               num_turns, duration_ms, cost_usd, cost_estimated, created_at
        FROM stressless.runs
        ORDER BY created_at DESC LIMIT 50
        """
    )

    experiments = await pool.fetch(
        """
        SELECT e.id, e.name, e.status, e.trials_per_item, e.summary, e.created_at,
               d.name AS dataset, (SELECT count(*) FROM stressless.dataset_items i
                                   WHERE i.dataset_id = e.dataset_id) AS items
        FROM stressless.experiments e
        JOIN stressless.datasets d ON d.id = e.dataset_id
        ORDER BY e.created_at DESC LIMIT 10
        """
    )

    proposals = await pool.fetch(
        """
        SELECT p.category, p.patch_summary, p.pr_url, p.pr_state, p.created_at,
               f.title AS finding_title
        FROM stressless.proposals p
        LEFT JOIN stressless.findings f ON f.id = p.finding_id
        ORDER BY p.created_at DESC LIMIT 10
        """
    )

    return {
        "days": days,
        "by_kind": [dict(row) for row in by_kind],
        "daily": [dict(row) for row in daily],
        "by_source": [dict(row) for row in by_source],
        "findings": [dict(row) for row in findings],
        "recent": [dict(row) for row in recent],
        "experiments": [dict(row) for row in experiments],
        "proposals": [dict(row) for row in proposals],
    }


def _fmt_usd(value: Any) -> str:
    return f"${float(value):.2f}" if value is not None else "—"


def _fmt_ms(value: Any) -> str:
    if value is None:
        return "—"
    seconds = float(value) / 1000
    return f"{seconds:.1f}s" if seconds < 120 else f"{seconds / 60:.1f}m"


def experiment_digest(summary: Any) -> str:
    """One-line human digest of an experiment summary JSONB."""
    if not isinstance(summary, dict):
        return ""
    parts: list[str] = []
    if summary.get("agreement_pct") is not None:
        parts.append(f"agreement {summary['agreement_pct']}%")
    if summary.get("cost_delta_pct") is not None:
        parts.append(f"cost Δ {summary['cost_delta_pct']:+}%")
    for model, stats in (summary.get("per_model") or {}).items():
        short = model.split("-4-")[0].replace("claude-", "")
        parts.append(
            f"{short}: ${stats.get('cost_per_call_usd')}/call, "
            f"p50 {stats.get('p50_latency_ms')}ms, "
            f"false-skips {stats.get('false_skips_on_signal_items')}"
        )
    for variant, stats in (summary.get("per_variant") or {}).items():
        parts.append(
            f"{variant}: ${stats.get('mean_cost_usd')}/run, "
            f"cache {stats.get('cache_read_share_pct')}%, "
            f"match {stats.get('decision_match_prod')}"
        )
    if summary.get("cross_variant_decision_agreement"):
        parts.append(
            f"cross-variant agreement {summary['cross_variant_decision_agreement']}"
        )
    return " · ".join(parts)[:400]


def _cache_pct(row: dict[str, Any]) -> str:
    read = row.get("cache_read") or 0
    total = (row.get("input_tokens") or 0) + read + (row.get("cache_write") or 0)
    if not total:
        return "—"
    return f"{read / total * 100:.0f}%"


def format_text(data: dict[str, Any]) -> str:
    lines: list[str] = []
    days = data["days"]
    lines.append(f"Stressless report — last {days} day(s)")
    lines.append("")

    lines.append("SPEND BY AGENT KIND")
    lines.append(
        f"{'kind':<22}{'runs':>6}{'cost':>10}{'?':>3}{'p50':>8}{'p95':>8}{'fail':>6}{'cache':>7}"
    )
    for row in data["by_kind"]:
        unknown = f"{row['cost_unknown']}" if row["cost_unknown"] else ""
        cost = _fmt_usd(row["cost_usd"]) + ("~" if row["any_estimated"] else "")
        lines.append(
            f"{row['agent_kind']:<22}{row['runs']:>6}{cost:>10}{unknown:>3}"
            f"{_fmt_ms(row['p50_ms']):>8}{_fmt_ms(row['p95_ms']):>8}"
            f"{row['failed']:>6}{_cache_pct(row):>7}"
        )
    lines.append("  (~ = estimated from tokens; '?' column = runs with no cost data —")
    lines.append("   historical SDK runs predate cost capture)")
    lines.append("")

    lines.append("DAILY")
    for row in data["daily"][:14]:
        lines.append(f"  {row['day']}  {row['runs']:>5} runs  {_fmt_usd(row['cost_usd'])}")
    lines.append("")

    if data["by_source"]:
        lines.append("ITEM PROCESSOR BY SOURCE")
        lines.append(
            f"{'source':<22}{'runs':>6}{'cost':>10}{'done':>6}{'not_rel':>8}{'dup':>5}{'fail':>6}"
        )
        for row in data["by_source"]:
            lines.append(
                f"{row['source']:<22}{row['runs']:>6}{_fmt_usd(row['cost_usd']):>10}"
                f"{row['completed']:>6}{row['not_relevant']:>8}{row['duplicate']:>5}{row['failed']:>6}"
            )
        lines.append("")

    if data.get("experiments"):
        lines.append("EXPERIMENTS")
        for row in data["experiments"]:
            lines.append(
                f"  [{row['status']:<7}] {row['name']} — {row['items']} items × "
                f"{row['trials_per_item']} trial(s) ({row['dataset']})"
            )
            digest = experiment_digest(row["summary"])
            if digest:
                lines.append(f"            {digest}")
        lines.append("")

    if data.get("proposals"):
        lines.append("PROPOSALS")
        for row in data["proposals"]:
            state = f" [{row['pr_state']}]" if row["pr_state"] else ""
            url = f" {row['pr_url']}" if row["pr_url"] else ""
            lines.append(f"  [{row['category']:<8}]{state} {row['patch_summary']}{url}")
        lines.append("")

    if data["findings"]:
        lines.append("OPEN FINDINGS")
        for row in data["findings"]:
            impact = ""
            if row["est_impact"]:
                est = row["est_impact"]
                if isinstance(est, dict) and est.get("usd_per_month"):
                    impact = f"  [≈${est['usd_per_month']}/mo]"
            lines.append(f"  [{row['severity']:<6}] {row['title']}{impact}")
    else:
        lines.append("OPEN FINDINGS: none — run `python -m stressless rules`")

    return "\n".join(lines)
