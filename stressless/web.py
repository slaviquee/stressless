"""Minimal /stressless dashboard, mounted into the host FastAPI app.

Localhost-only (same guard as /internal). Styled to the 0noise design
language: Geist Mono, 13px, hairline #e5e5e5 borders, square corners,
white background, indigo #5b5bd6 accent.
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
  body {{ font-family: 'Geist Mono', ui-monospace, 'SF Mono', monospace;
         font-size: 13px; color: #1a1a1a; background: #fff;
         margin: 0; padding: 24px; line-height: 1.7; }}
  h1 {{ font-size: 14px; font-weight: 600; margin: 0 0 4px; }}
  h1 a {{ color: #5b5bd6; text-decoration: none; }}
  h2 {{ font-size: 12px; font-weight: 600; text-transform: uppercase;
        letter-spacing: .06em; color: #6b6b6b; margin: 28px 0 8px; }}
  .sub {{ color: #6b6b6b; margin-bottom: 20px; }}
  table {{ border-collapse: collapse; width: 100%; max-width: 1100px; }}
  th, td {{ border-bottom: 1px solid #e5e5e5; text-align: left;
            padding: 4px 14px 4px 0; font-weight: 400; white-space: nowrap; }}
  th {{ color: #6b6b6b; font-size: 11px; text-transform: uppercase; letter-spacing: .05em; }}
  td.num, th.num {{ text-align: right; }}
  .ok {{ color: #1a7f37; }} .bad {{ color: #cf222e; }} .dim {{ color: #9a9a9a; }}
  .sev-high {{ color: #cf222e; }} .sev-medium {{ color: #b35900; }} .sev-low {{ color: #6b6b6b; }}
  .accent {{ color: #5b5bd6; }}
  .cards {{ display: flex; gap: 24px; margin: 16px 0 4px; flex-wrap: wrap; }}
  .card {{ border: 1px solid #e5e5e5; padding: 10px 16px; min-width: 150px; }}
  .card .v {{ font-size: 18px; }} .card .k {{ color: #6b6b6b; font-size: 11px;
       text-transform: uppercase; letter-spacing: .05em; }}
</style></head><body>
<h1><span class="accent">stressless</span> — agent telemetry</h1>
<div class="sub">last {days} day(s) · <a class="accent" href="/stressless?days=1">1d</a> ·
 <a class="accent" href="/stressless?days=7">7d</a> ·
 <a class="accent" href="/stressless?days=30">30d</a> ·
 <a class="accent" href="/stressless?days=365">all</a></div>
<div class="cards">{cards}</div>
<h2>spend by agent kind</h2>{by_kind}
<h2>item processor by source</h2>{by_source}
<h2>experiments</h2>{experiments}
<h2>proposals</h2>{proposals}
<h2>open findings</h2>{findings}
<h2>recent runs</h2>{recent}
<div class="sub" style="margin-top:24px">cost marked ~ is estimated from tokens ·
 “?” counts runs with no cost data (historical SDK runs predate cost capture)</div>
</body></html>"""


def _esc(value: Any) -> str:
    return html.escape(str(value)) if value is not None else "—"


def _table(headers: list[tuple[str, bool]], rows: list[list[str]]) -> str:
    if not rows:
        return '<div class="dim">no data</div>'
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


@router.get("", response_class=HTMLResponse)
@router.get("/", response_class=HTMLResponse)
async def overview(days: int = 7) -> str:
    data = await gather(days)

    total_cost = sum(float(r["cost_usd"] or 0) for r in data["by_kind"])
    total_runs = sum(int(r["runs"]) for r in data["by_kind"])
    unknown = sum(int(r["cost_unknown"] or 0) for r in data["by_kind"])
    open_findings = len(data["findings"])
    cards = "".join(
        f'<div class="card"><div class="v">{value}</div><div class="k">{key}</div></div>'
        for key, value in [
            ("spend (known)", f"${total_cost:.2f}"),
            ("runs", f"{total_runs:,}"),
            ("runs w/o cost", f"{unknown:,}"),
            ("open findings", open_findings),
        ]
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

    experiments = _table(
        [("when", False), ("experiment", False), ("status", False),
         ("items×trials", True), ("result", False)],
        [
            [
                r["created_at"].strftime("%m-%d %H:%M"),
                _esc(r["name"]),
                f'<span class="{"ok" if r["status"] == "done" else "dim"}">{_esc(r["status"])}</span>',
                f"{r['items']}×{r['trials_per_item']}",
                f'<span style="white-space:normal">{_esc(experiment_digest(r["summary"]))}</span>',
            ]
            for r in data.get("experiments", [])
        ],
    )

    proposals = _table(
        [("category", False), ("state", False), ("proposal", False), ("pr", False)],
        [
            [
                _esc(r["category"]),
                _esc(r["pr_state"] or "—"),
                f'<span style="white-space:normal">{_esc(r["patch_summary"])}</span>',
                f'<a class="accent" href="{_esc(r["pr_url"])}">{_esc(r["pr_url"]).split("/")[-1] if r["pr_url"] else ""}</a>'
                if r["pr_url"] else "—",
            ]
            for r in data.get("proposals", [])
        ],
    )

    findings = _table(
        [("sev", False), ("finding", False), ("n", True), ("impact", True)],
        [
            [
                f'<span class="sev-{_esc(r["severity"])}">{_esc(r["severity"])}</span>',
                _esc(r["title"]),
                str(r["occurrences"]),
                _esc((r["est_impact"] or {}).get("usd_per_month", "")) + "/mo"
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
            for r in data["recent"]
        ],
    )

    return _PAGE.format(
        days=data["days"],
        cards=cards,
        by_kind=by_kind,
        by_source=by_source,
        experiments=experiments,
        proposals=proposals,
        findings=findings,
        recent=recent,
    )
