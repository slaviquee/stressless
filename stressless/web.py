"""The /stressless dashboard, mounted into the host FastAPI app.

Reads top-to-bottom as a story: what the agents cost (watch), what was
investigated (the improvement loop: finding → experiments → conclusion →
PR / no PR), and what needs attention (findings inbox, recent runs).

Localhost-only (same guard as /internal). 0noise design language: Geist Mono,
13px, hairline #e5e5e5 borders, square corners, white, indigo #5b5bd6 accent,
green/red strictly semantic.
"""

from __future__ import annotations

import html
from typing import Any

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import HTMLResponse

_LOOPBACK_HOSTS = {"127.0.0.1", "::1", "localhost", "testclient"}


async def _require_localhost(request: Request) -> None:
    """Reject requests not originating from loopback (mirrors the host guard:
    checks the connecting client IP, never X-Forwarded-For)."""
    client = request.client
    if client is None or client.host not in _LOOPBACK_HOSTS:
        raise HTTPException(status_code=403, detail="Forbidden")

from . import store
from .report import _cache_pct, _fmt_ms, _fmt_usd, experiment_digest, gather

router = APIRouter(prefix="/stressless", dependencies=[Depends(_require_localhost)])

_PAGE = """<!doctype html>
<html><head><meta charset="utf-8"><title>stressless</title>
<meta name="robots" content="noindex">
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: 'Geist Mono', ui-monospace, 'SF Mono', monospace;
         font-size: 13px; color: #1a1a1a; background: #fff;
         margin: 0; padding: 28px 32px 48px; line-height: 1.7; }}
  .wrap {{ max-width: 1060px; }}
  h1 {{ font-size: 14px; font-weight: 600; margin: 0; }}
  h1 .accent {{ color: #5b5bd6; }}
  .tagline {{ color: #9a9a9a; }}
  .sub {{ color: #6b6b6b; margin: 2px 0 22px; }}
  .sub a {{ color: #5b5bd6; text-decoration: none; }}
  h2 {{ font-size: 12px; font-weight: 600; text-transform: uppercase;
        letter-spacing: .07em; color: #6b6b6b; margin: 34px 0 10px; }}
  h2 .n {{ color: #c2c2c2; font-weight: 400; margin-right: 8px; }}
  table {{ border-collapse: collapse; width: 100%; }}
  th, td {{ border-bottom: 1px solid #efefef; text-align: left;
            padding: 5px 14px 5px 0; font-weight: 400; white-space: nowrap; }}
  tr:last-child td {{ border-bottom: 1px solid #e5e5e5; }}
  th {{ color: #9a9a9a; font-size: 11px; text-transform: uppercase;
        letter-spacing: .05em; border-bottom: 1px solid #e5e5e5; }}
  td.num, th.num {{ text-align: right; }}
  .ok {{ color: #1a7f37; }} .bad {{ color: #cf222e; }} .dim {{ color: #9a9a9a; }}
  .sev-high {{ color: #cf222e; }} .sev-medium {{ color: #b35900; }} .sev-low {{ color: #6b6b6b; }}
  a {{ color: #5b5bd6; }}
  .cards {{ display: flex; gap: 14px; margin: 18px 0 0; flex-wrap: wrap; }}
  .card {{ border: 1px solid #e5e5e5; padding: 10px 18px 8px; min-width: 148px; }}
  .card .v {{ font-size: 20px; line-height: 1.4; }}
  .card .k {{ color: #6b6b6b; font-size: 11px; text-transform: uppercase; letter-spacing: .05em; }}
  .story {{ border: 1px solid #e5e5e5; padding: 14px 20px 12px; margin: 0 0 14px; }}
  .story-head {{ display: flex; justify-content: space-between; gap: 16px;
                 align-items: baseline; margin-bottom: 10px; }}
  .story-title {{ font-weight: 600; }}
  .chip {{ font-size: 11px; padding: 1px 10px; border: 1px solid; white-space: nowrap; }}
  .chip-pr {{ color: #5b5bd6; border-color: #c6c6f0; }}
  .chip-gate {{ color: #6b6b6b; border-color: #d6d6d6; }}
  .chip-run {{ color: #b35900; border-color: #ecd2b3; }}
  .steps {{ border-left: 1px solid #e5e5e5; margin-left: 3px; padding-left: 18px; }}
  .step {{ position: relative; padding: 3px 0 9px; }}
  .step .marker {{ position: absolute; left: -22px; top: 11px; width: 7px; height: 7px;
                   background: #fff; border: 1px solid #b3b3b3; }}
  .step.is-finding .marker {{ background: #b35900; border-color: #b35900; }}
  .step.is-exp .marker {{ background: #5b5bd6; border-color: #5b5bd6; }}
  .step-meta {{ color: #9a9a9a; font-size: 11px; text-transform: uppercase;
                letter-spacing: .05em; }}
  .step-text {{ white-space: normal; }}
  .digest {{ color: #6b6b6b; white-space: normal; }}
  .concl {{ white-space: normal; padding: 2px 0 0; }}
  .concl b {{ font-weight: 600; }}
  .action {{ margin-top: 8px; padding-top: 10px; border-top: 1px solid #efefef;
             white-space: normal; }}
  .action .arr {{ color: #9a9a9a; }}
  .daily {{ color: #9a9a9a; font-size: 12px; margin: -4px 0 10px; }}
  .wrapcell {{ white-space: normal; }}
  .foot {{ color: #9a9a9a; margin-top: 30px; font-size: 12px; }}
</style></head><body><div class="wrap">
<h1><span class="accent">stressless</span> — agent telemetry
    <span class="tagline">· watch → judge → improve</span></h1>
<div class="sub">last {days} day(s) ·
 <a href="/stressless?days=1">1d</a> · <a href="/stressless?days=7">7d</a> ·
 <a href="/stressless?days=30">30d</a> · <a href="/stressless?days=365">all</a></div>
<div class="cards">{cards}</div>
<h2><span class="n">01</span>improvement loop — what ran, what we learned, what shipped</h2>
{stories}
<h2><span class="n">02</span>spend by agent kind</h2>
<div class="daily">{daily}</div>
{by_kind}
<h2><span class="n">03</span>item processor by source</h2>
{by_source}
<h2><span class="n">04</span>findings inbox</h2>
{findings}
<h2><span class="n">05</span>recent runs</h2>
{recent}
<div class="foot">~ cost estimated from tokens · “?” = runs with no cost data
 (historical SDK runs predate cost capture) · all times local</div>
</div></body></html>"""


def _esc(value: Any) -> str:
    return html.escape(str(value)) if value is not None else "—"


def _table(headers: list[tuple[str, bool]], rows: list[list[str]]) -> str:
    if not rows:
        return '<div class="dim">no data in this window</div>'
    head = "".join(
        f'<th class="num">{h}</th>' if num else f"<th>{h}</th>" for h, num in headers
    )
    body = []
    for row in rows:
        cells = []
        for (_, num), cell in zip(headers, row):
            cells.append(f'<td class="num">{cell}</td>' if num else f"<td>{cell}</td>")
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f"<table><tr>{head}</tr>{''.join(body)}</table>"


def _short_title(title: str) -> str:
    head = title.split(" — ")[0]
    return head if len(head) <= 80 else head[:77] + "…"


def _story_html(story: dict[str, Any]) -> str:
    action = story["action"]
    if action["kind"] == "pr":
        chip = f'<span class="chip chip-pr">PR {_esc(action.get("state") or "open")}</span>'
    elif action["kind"] == "no_pr":
        chip = '<span class="chip chip-gate">no PR — gate held</span>'
    elif action["kind"] == "running":
        chip = '<span class="chip chip-run">running</span>'
    else:
        chip = '<span class="chip chip-gate">no proposal yet</span>'

    steps: list[str] = []
    finding = story.get("finding")
    if finding:
        seen = finding["first_seen"].strftime("%b %d %H:%M") if finding.get("first_seen") else ""
        impact = ""
        est = finding.get("est_impact")
        if isinstance(est, dict) and est.get("usd_per_month"):
            impact = f' · est ≈ ${est["usd_per_month"]}/mo'
        steps.append(
            f'<div class="step is-finding"><span class="marker"></span>'
            f'<span class="step-meta">{seen} · finding · {_esc(finding.get("severity"))}{impact}</span>'
            f'<div class="step-text">{_esc(finding["title"])}</div></div>'
        )
    for experiment in story["experiments"]:
        when = experiment["created_at"].strftime("%b %d %H:%M")
        digest = experiment_digest(experiment["summary"])
        conclusion = experiment.get("conclusion")
        concl_html = ""
        if conclusion:
            lowered = conclusion.lower()
            if "refuted" in lowered or "no win" in lowered:
                verdict_class = "bad"
            elif "verified" in lowered or "corroborat" in lowered or "parity" in lowered:
                verdict_class = "ok"
            else:
                verdict_class = ""
            concl_html = (
                f'<div class="concl"><b class="{verdict_class}">⇒</b> {_esc(conclusion)}</div>'
            )
        steps.append(
            f'<div class="step is-exp"><span class="marker"></span>'
            f'<span class="step-meta">{when} · experiment · '
            f'{experiment["items"]} items × {experiment["trials_per_item"]} trial(s) · '
            f'{_esc(experiment["status"])}</span>'
            f'<div class="step-text">{_esc(experiment["name"])}</div>'
            + (f'<div class="digest">{_esc(digest)}</div>' if digest else "")
            + concl_html
            + "</div>"
        )

    if action["kind"] == "pr" and action.get("url"):
        action_html = (
            f'<div class="action"><span class="arr">action →</span> '
            f'<a href="{_esc(action["url"])}">pull request · {_esc(action.get("state") or "open")}</a> '
            f'— {_esc(action.get("summary"))}</div>'
        )
    else:
        action_html = (
            f'<div class="action"><span class="arr">action →</span> '
            f'{_esc(action.get("summary"))}</div>'
        )

    return (
        f'<div class="story"><div class="story-head">'
        f'<span class="story-title">{_esc(_short_title(story["title"]))}</span>{chip}</div>'
        f'<div class="steps">{"".join(steps)}</div>{action_html}</div>'
    )


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def overview(days: int = 7) -> str:
    data = await gather(days)

    total_cost = sum(float(r["cost_usd"] or 0) for r in data["by_kind"])
    total_runs = sum(int(r["runs"]) for r in data["by_kind"])
    unknown = sum(int(r["cost_unknown"] or 0) for r in data["by_kind"])
    open_findings = len(data["findings"])
    prs_open = sum(
        1 for s in data["stories"]
        if s["action"]["kind"] == "pr" and (s["action"].get("state") or "") == "open"
    )
    cards = "".join(
        f'<div class="card"><div class="v">{value}</div><div class="k">{key}</div></div>'
        for key, value in [
            ("spend (known)", f"${total_cost:.2f}"),
            ("runs", f"{total_runs:,}"),
            ("runs w/o cost", f"{unknown:,}"),
            ("open findings", open_findings),
            ("PRs open", prs_open),
        ]
    )

    stories = "".join(_story_html(s) for s in data["stories"]) or (
        '<div class="dim">no investigations yet — run `python -m stressless rules`'
        " and the experiment scripts</div>"
    )

    daily = " · ".join(
        f'{row["day"].strftime("%b %d")} {_fmt_usd(row["cost_usd"])}'
        for row in data["daily"][:10]
    )

    by_kind = _table(
        [("kind", False), ("runs", True), ("cost", True), ("?", True),
         ("p50", True), ("p95", True), ("fail", True), ("cache", True)],
        [
            [
                _esc(r["agent_kind"]),
                f"{r['runs']:,}",
                _fmt_usd(r["cost_usd"]) + ("~" if r["any_estimated"] else ""),
                str(r["cost_unknown"] or ""),
                _fmt_ms(r["p50_ms"]),
                _fmt_ms(r["p95_ms"]),
                f'<span class="bad">{r["failed"]}</span>' if r["failed"] else "0",
                _cache_pct(r),
            ]
            for r in data["by_kind"]
        ],
    )

    by_source = _table(
        [("source", False), ("runs", True), ("cost", True), ("done", True),
         ("not_rel", True), ("dup", True), ("fail", True)],
        [
            [
                _esc(r["source"]),
                f"{r['runs']:,}",
                _fmt_usd(r["cost_usd"]),
                str(r["completed"]),
                str(r["not_relevant"]),
                str(r["duplicate"]),
                f'<span class="bad">{r["failed"]}</span>' if r["failed"] else "0",
            ]
            for r in data["by_source"]
        ],
    )

    findings = _table(
        [("sev", False), ("status", False), ("finding", False), ("n", True), ("impact", True)],
        [
            [
                f'<span class="sev-{_esc(r["severity"])}">{_esc(r["severity"])}</span>',
                _esc(r.get("status", "open")) if isinstance(r, dict) else "open",
                f'<span class="wrapcell">{_esc(r["title"])}</span>',
                str(r["occurrences"]),
                f'${r["est_impact"]["usd_per_month"]}/mo'
                if isinstance(r["est_impact"], dict) and r["est_impact"].get("usd_per_month")
                else "",
            ]
            for r in data["findings"]
        ],
    )

    recent = _table(
        [("when", False), ("kind", False), ("mode", False), ("ref", False),
         ("status", False), ("outcome", False), ("turns", True), ("wall", True), ("cost", True)],
        [
            [
                r["created_at"].strftime("%m-%d %H:%M"),
                _esc(r["agent_kind"]),
                _esc(r["mode"]),
                _esc((r["external_ref"] or "")[:12]),
                f'<span class="{"ok" if r["status"] == "succeeded" else "bad"}">{_esc(r["status"])}</span>',
                _esc(r["outcome"]),
                _esc(r["num_turns"]),
                _fmt_ms(r["duration_ms"]),
                _fmt_usd(r["cost_usd"]) + ("~" if r["cost_estimated"] else ""),
            ]
            for r in data["recent"][:25]
        ],
    )

    return _PAGE.format(
        days=data["days"],
        cards=cards,
        stories=stories,
        daily=daily or '<span class="dim">—</span>',
        by_kind=by_kind,
        by_source=by_source,
        findings=findings,
        recent=recent,
    )
