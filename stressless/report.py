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
        SELECT kind, agent_kind, title, severity, status, occurrences, est_impact, last_seen
        FROM stressless.findings
        WHERE status IN ('open', 'proposed', 'pr_open')
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

    return {
        "days": days,
        "by_kind": [dict(row) for row in by_kind],
        "daily": [dict(row) for row in daily],
        "by_source": [dict(row) for row in by_source],
        "findings": [dict(row) for row in findings],
        "recent": [dict(row) for row in recent],
        "stories": await gather_stories(pool),
    }


async def gather_stories(pool: Any) -> list[dict[str, Any]]:
    """Thread the improvement loop into stories: finding -> experiments ->
    conclusion -> action (PR opened / none). Experiments on the same dataset
    belong to one investigation; proposals attach via their experiment."""
    experiments = await pool.fetch(
        """
        SELECT e.id, e.name, e.status, e.summary, e.conclusion, e.created_at,
               e.trials_per_item, d.id AS dataset_id, d.name AS dataset,
               (SELECT count(*) FROM stressless.dataset_items i
                WHERE i.dataset_id = d.id) AS items
        FROM stressless.experiments e
        JOIN stressless.datasets d ON d.id = e.dataset_id
        ORDER BY e.created_at
        """
    )
    proposals = await pool.fetch(
        """
        SELECT p.id, p.category, p.patch_summary, p.pr_url, p.pr_state,
               p.created_at, p.experiment_id,
               f.title AS finding_title, f.severity, f.first_seen,
               f.est_impact, f.status AS finding_status
        FROM stressless.proposals p
        LEFT JOIN stressless.findings f ON f.id = p.finding_id
        ORDER BY p.created_at
        """
    )

    threads: dict[Any, dict[str, Any]] = {}
    experiment_thread: dict[Any, Any] = {}
    for row in experiments:
        thread = threads.setdefault(
            row["dataset_id"],
            {"dataset": row["dataset"], "title": None, "finding": None,
             "experiments": [], "proposals": [], "started": row["created_at"]},
        )
        experiment = dict(row)
        # An experiment may use a subset of its dataset — the summary knows.
        if isinstance(experiment.get("summary"), dict) and experiment["summary"].get("items"):
            experiment["items"] = experiment["summary"]["items"]
        thread["experiments"].append(experiment)
        experiment_thread[row["id"]] = row["dataset_id"]

    orphans: list[dict[str, Any]] = []
    for row in proposals:
        thread_key = experiment_thread.get(row["experiment_id"])
        if thread_key is None:
            orphans.append(dict(row))
            continue
        thread = threads[thread_key]
        thread["proposals"].append(dict(row))
        if row["finding_title"] and thread["finding"] is None:
            thread["finding"] = {
                "title": row["finding_title"],
                "severity": row["severity"],
                "first_seen": row["first_seen"],
                "est_impact": row["est_impact"],
                "status": row["finding_status"],
            }

    stories: list[dict[str, Any]] = []
    for thread in threads.values():
        if thread["finding"]:
            thread["title"] = thread["finding"]["title"]
        else:
            thread["title"] = thread["experiments"][0]["name"]
        done = all(e["status"] == "done" for e in thread["experiments"])
        if thread["proposals"]:
            last = thread["proposals"][-1]
            thread["action"] = {
                "kind": "pr",
                "state": last["pr_state"],
                "url": last["pr_url"],
                "summary": last["patch_summary"],
                "category": last["category"],
            }
        elif done:
            refuted = any(
                "refuted" in (e["conclusion"] or "").lower()
                or "no win" in (e["conclusion"] or "").lower()
                for e in thread["experiments"]
            )
            thread["action"] = {
                "kind": "no_pr" if refuted else "none",
                "summary": (
                    "no PR — the hypothesis did not survive measurement"
                    if refuted
                    else "no proposal yet"
                ),
            }
        else:
            thread["action"] = {"kind": "running", "summary": "experiment running…"}
        stories.append(thread)
    for orphan in orphans:
        stories.append(
            {"dataset": None, "title": orphan["finding_title"] or orphan["patch_summary"],
             "finding": None, "experiments": [],
             "proposals": [orphan], "started": orphan["created_at"],
             "action": {"kind": "pr", "state": orphan["pr_state"],
                        "url": orphan["pr_url"], "summary": orphan["patch_summary"],
                        "category": orphan["category"]}}
        )
    stories.sort(key=lambda s: s["started"], reverse=True)
    return stories


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
    if summary.get("pointwise_correctness"):
        for model, stats in summary["pointwise_correctness"].items():
            parts.append(
                f"{model} judged correct {stats.get('correct_pct')}% "
                f"({stats.get('correct')}✓/{stats.get('incorrect')}✗/{stats.get('unknown')}?)"
            )
    if summary.get("pairwise_on_disagreements") is not None:
        pw = summary["pairwise_on_disagreements"]
        if pw:
            parts.append(
                f"{summary.get('disagreements', '?')} disagreements → "
                + ", ".join(f"{k} {v}" for k, v in pw.items())
            )
    if summary.get("judge_model"):
        parts.append(f"judge {summary['judge_model']}")
    return " · ".join(parts)[:600]


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

    for story in data.get("stories", []):
        if not lines or lines[-1] != "":
            lines.append("")
        lines.append(f"IMPROVEMENT LOOP — {story['title']}")
        if story.get("finding"):
            finding = story["finding"]
            seen = finding["first_seen"].strftime("%m-%d") if finding.get("first_seen") else ""
            lines.append(f"  {seen}  finding     [{finding.get('severity')}] {finding['title']}")
        for experiment in story["experiments"]:
            when = experiment["created_at"].strftime("%m-%d")
            lines.append(
                f"  {when}  experiment  {experiment['name']} — {experiment['items']} items × "
                f"{experiment['trials_per_item']} trial(s) [{experiment['status']}]"
            )
            digest = experiment_digest(experiment["summary"])
            if digest:
                lines.append(f"               {digest}")
            if experiment.get("conclusion"):
                lines.append(f"               ⇒ {experiment['conclusion']}")
        action = story["action"]
        if action["kind"] == "pr":
            lines.append(f"  ACTION: PR [{action['state']}] {action['summary']}")
            if action.get("url"):
                lines.append(f"          {action['url']}")
        else:
            lines.append(f"  ACTION: {action['summary']}")
    if data.get("stories"):
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
