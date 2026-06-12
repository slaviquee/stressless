"""LLM-as-judge (Judge layer 2): sampled, rubric-based scoring.

Hygiene (mid-2026 consensus, see the spec §3.4):
- binary / low-cardinality verdicts, one isolated call each (no Likert)
- reasoning emitted *before* the verdict (schema order + instruction)
- an explicit "unknown" escape so the judge can decline
- a stronger / different model than the agent under test (default Opus, while
  the agents run on Sonnet/Haiku) — note the same-family self-preference caveat
- pairwise comparisons run in BOTH orderings by the caller to cancel position bias

Verdicts are written to stressless.scores with source='judge'. Pass a
stressless-wrapped client (kind="judge") so the judge's own calls are recorded.
"""

from __future__ import annotations

import json
import os
import random
from dataclasses import dataclass
from typing import Any

DEFAULT_JUDGE_MODEL = os.environ.get("STRESSLESS_JUDGE_MODEL", "claude-opus-4-8")

# Per-agent-kind rubrics. `system` defines what a correct decision is; it is the
# only place domain knowledge lives, so a host can override these for its agents.
RUBRICS: dict[str, str] = {
    "prefilter": (
        "You are grading a binary prefilter that sits in front of an AI-startup "
        "intelligence pipeline. The prefilter sees one scraped item and decides "
        "KEEP (send to the analyst agent) or DROP (discard as noise).\n\n"
        "A correct KEEP: the item is a concrete, recent event for an AI / "
        "AI-enabled / adjacent deep-tech, frontier, or technical health-science "
        "startup — a funding round, fundraising-open, grant/award, launch, "
        "acquisition, partnership, key hire, accelerator cohort, or a quantified "
        "traction/ARR/users milestone.\n"
        "A correct DROP: opinion/commentary, tutorials, general news, listicles, "
        "non-AI and non-adjacent companies, or items with no concrete startup "
        "event.\n\n"
        "Dropping a real signal is the costly error; keeping borderline noise is "
        "cheap (the analyst filters it). Judge the DECISION on the item's own "
        "evidence, not on what later happened in the database."
    ),
}

POINTWISE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "reasoning": {"type": "string"},
        "verdict": {"type": "string", "enum": ["correct", "incorrect", "unknown"]},
    },
    "required": ["reasoning", "verdict"],
}

PAIRWISE_SCHEMA = {
    "type": "object",
    "additionalProperties": False,
    "properties": {
        "reasoning": {"type": "string"},
        "verdict": {"type": "string", "enum": ["A", "B", "tie", "unknown"]},
    },
    "required": ["reasoning", "verdict"],
}


@dataclass
class Verdict:
    label: str
    reasoning: str
    judge_model: str
    raw: str | None = None


def _format_item(item: dict[str, Any]) -> str:
    keep = {k: item.get(k) for k in ("source", "title", "url", "raw_text") if item.get(k)}
    text = keep.get("raw_text") or ""
    if len(text) > 2000:
        keep["raw_text"] = text[:2000] + " …[truncated]"
    return json.dumps(keep, ensure_ascii=False, indent=2)


async def _ask(client: Any, *, model: str, system: str, user: str, schema: dict) -> dict:
    response = await client.messages.create(
        model=model,
        max_tokens=1024,
        system=system,
        messages=[{"role": "user", "content": user}],
        output_config={"format": {"type": "json_schema", "schema": schema}},
    )
    text = next(
        (b.text for b in getattr(response, "content", []) if getattr(b, "text", "")),
        "",
    )
    try:
        return json.loads(text)
    except Exception:
        return {"verdict": "unknown", "reasoning": f"unparseable judge output: {text[:200]}"}


async def judge_pointwise(
    client: Any,
    *,
    agent_kind: str,
    item: dict[str, Any],
    decision_label: str,
    judge_model: str = DEFAULT_JUDGE_MODEL,
) -> Verdict:
    """Was a single KEEP/DROP-style decision correct? -> correct|incorrect|unknown."""
    system = RUBRICS.get(agent_kind, RUBRICS["prefilter"])
    user = (
        f"ITEM:\n{_format_item(item)}\n\n"
        f"The prefilter decided: {decision_label}.\n\n"
        "First reason step by step about whether that decision is correct given "
        "the item's evidence. Then return your verdict. Use 'unknown' only if the "
        "item text is genuinely too sparse to judge."
    )
    data = await _ask(client, model=judge_model, system=system, user=user, schema=POINTWISE_SCHEMA)
    return Verdict(
        label=str(data.get("verdict", "unknown")),
        reasoning=str(data.get("reasoning", "")),
        judge_model=judge_model,
    )


async def judge_pairwise(
    client: Any,
    *,
    agent_kind: str,
    item: dict[str, Any],
    option_a: str,
    option_b: str,
    judge_model: str = DEFAULT_JUDGE_MODEL,
) -> Verdict:
    """Which of two decisions is better? -> A|B|tie|unknown. Labels A/B are
    blind; the caller maps them to models and runs both orderings."""
    system = RUBRICS.get(agent_kind, RUBRICS["prefilter"])
    user = (
        f"ITEM:\n{_format_item(item)}\n\n"
        f"Two prefilters judged this item:\n"
        f"  Decision A: {option_a}\n"
        f"  Decision B: {option_b}\n\n"
        "First reason about which decision better serves a prefilter that must "
        "not drop real AI-startup signals but should drop noise. Then return your "
        "verdict: 'A', 'B', 'tie' (equally good — usually when both agree), or "
        "'unknown'."
    )
    data = await _ask(client, model=judge_model, system=system, user=user, schema=PAIRWISE_SCHEMA)
    return Verdict(
        label=str(data.get("verdict", "unknown")),
        reasoning=str(data.get("reasoning", "")),
        judge_model=judge_model,
    )


async def record_score(
    pool: Any,
    *,
    run_id: Any | None,
    name: str,
    verdict: Verdict,
    step_id: Any | None = None,
) -> None:
    """Persist a judge verdict to stressless.scores (source='judge')."""
    await pool.execute(
        """INSERT INTO stressless.scores
             (run_id, step_id, name, source, data_type, value_text, reasoning, judge_model)
           VALUES ($1, $2, $3, 'judge', 'categorical', $4, $5, $6)""",
        run_id, step_id, name, verdict.label, verdict.reasoning[:2000], verdict.judge_model,
    )


def rubric_for(agent_kind: str) -> str | None:
    if agent_kind in RUBRICS:
        return RUBRICS[agent_kind]
    base = agent_kind.split("_experiment")[0].split(":")[0]
    return RUBRICS.get(base)


async def judge_response(
    client: Any,
    *,
    agent_kind: str,
    request: Any,
    response_text: str,
    judge_model: str = DEFAULT_JUDGE_MODEL,
) -> Verdict:
    """Grade one captured call: given the request the agent saw and the response
    it produced, was the decision correct? Generic over any rubric'd kind."""
    system = rubric_for(agent_kind) or RUBRICS["prefilter"]
    request_text = request if isinstance(request, str) else json.dumps(
        request, ensure_ascii=False, default=str
    )
    user = (
        f"REQUEST the agent received:\n{request_text[:6000]}\n\n"
        f"RESPONSE the agent produced:\n{response_text[:2000]}\n\n"
        "First reason step by step about whether the response's decision is "
        "correct given the request's evidence and the rubric. Then return your "
        "verdict. Use 'unknown' only if the request is too sparse to judge."
    )
    data = await _ask(client, model=judge_model, system=system, user=user, schema=POINTWISE_SCHEMA)
    return Verdict(
        label=str(data.get("verdict", "unknown")),
        reasoning=str(data.get("reasoning", "")),
        judge_model=judge_model,
    )


async def judge_recent(
    pool: Any,
    client: Any,
    *,
    days: int = 1,
    sample_rate: float = 0.10,
    limit: int = 50,
    judge_model: str = DEFAULT_JUDGE_MODEL,
    rng: random.Random | None = None,
) -> dict[str, Any]:
    """Continuous Judge layer 2: judge all recent failures + a sample of
    successes for every rubric'd agent kind, skipping runs already judged.
    Designed for cron: ``python -m stressless judge``."""
    from datetime import timedelta

    rng = rng or random.Random()
    rows = await pool.fetch(
        """
        SELECT r.id, r.agent_kind, r.status, s.input AS request, s.output AS response
        FROM stressless.runs r
        JOIN stressless.steps s ON s.run_id = r.id AND s.kind = 'llm' AND s.input IS NOT NULL
        WHERE r.created_at > now() - $1::interval
          AND r.agent_kind <> 'judge'
          AND NOT EXISTS (SELECT 1 FROM stressless.scores sc
                          WHERE sc.run_id = r.id AND sc.source = 'judge')
        ORDER BY r.created_at DESC
        LIMIT $2
        """,
        timedelta(days=days), limit * 4,
    )
    picked = []
    for row in rows:
        if rubric_for(row["agent_kind"]) is None:
            continue
        if row["status"] != "succeeded" or rng.random() < sample_rate:
            picked.append(row)
        if len(picked) >= limit:
            break

    counts: dict[str, dict[str, int]] = {}
    for row in picked:
        response_text = row["response"] if isinstance(row["response"], str) else json.dumps(
            row["response"], ensure_ascii=False, default=str
        )
        verdict = await judge_response(
            client, agent_kind=row["agent_kind"], request=row["request"],
            response_text=response_text or "", judge_model=judge_model,
        )
        await record_score(
            pool, run_id=row["id"], name=f"{row['agent_kind']}_correct", verdict=verdict
        )
        bucket = counts.setdefault(row["agent_kind"], {"correct": 0, "incorrect": 0, "unknown": 0})
        bucket[verdict.label if verdict.label in bucket else "unknown"] += 1

    for kind, bucket in counts.items():
        graded = bucket["correct"] + bucket["incorrect"]
        if bucket["incorrect"] >= 3 and graded and bucket["incorrect"] / graded > 0.10:
            await pool.execute(
                """INSERT INTO stressless.findings
                     (fingerprint, kind, agent_kind, title, detail, severity, occurrences)
                   VALUES ($1, 'failure_cluster', $2, $3, $4, 'high', $5)
                   ON CONFLICT (fingerprint) DO UPDATE SET
                     title = EXCLUDED.title, detail = EXCLUDED.detail,
                     occurrences = EXCLUDED.occurrences, last_seen = now()""",
                f"judge_quality:{kind}", kind,
                f"{kind}: LLM judge marked {bucket['incorrect']}/{graded} recent decisions incorrect",
                {"window_days": days, "judge_model": judge_model, **bucket},
                bucket["incorrect"],
            )
    return {"judged": len(picked), "by_kind": counts}
