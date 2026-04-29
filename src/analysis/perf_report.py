"""Render `outputs/performance.jsonl` → `DATA_REPORTS/performance_report.html`.

Self-contained: a masthead, KPI band, and one per-run section showing a
horizontal bar chart of stage durations plus a detail table. Styling matches
the Porsche 911 Amethyst / Bronze / Concrete paper theme used by the
data-analysis report so the two outputs read as one publication.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import plotly.graph_objects as go  # type: ignore[import-untyped]

from pipeline.observability.logging import get_logger
from pipeline.observability.perf import read_perf_log

log = get_logger(__name__)

DATA_REPORT_DIR = Path("DATA_REPORTS")

# Shared theme tokens — kept in lockstep with src/analysis/html_report.py.
INK = "#15131A"
INK_2 = "#3B3540"
PAPER_0 = "#F3F1EC"
PAPER_2 = "#F0EDE7"
LINE = "#D4CFC7"
AXIS_LINE = "#B8B3AB"
TICK_COLOR = "#5A4A45"
ORANGE = "#5E2548"  # amethyst
AMBER = "#D4B072"  # bronze
RASPBERRY = "#B84A9A"
NEUTRAL = "#7E7885"
PLUM = "#E07DB2"

# Stage prefix → hue. Keeps the bar chart legible when many stages share a
# family: ingest reads amethyst, dbt bronze, analyze raspberry, storage plum.
_STAGE_COLOURS = {
    "ingest": ORANGE,
    "dbt": AMBER,
    "analyze": RASPBERRY,
    "storage": PLUM,
}
_DEFAULT_COLOUR = NEUTRAL

# Known labels → one-line description. Rendered under the run title so the
# reader knows what kind of run each section represents without grep-ing the
# commit log. Unknown labels fall back to a generic caption.
_INCREMENTAL_DESCRIPTION = (
    "Warm incremental <code>make run</code> on an already-seeded warehouse — the "
    "next day's cron run. Seed stays an idempotent no-op (fingerprints match); "
    "ingest only pulls dates newer than <code>max(silver.date)</code>; dbt runs "
    "incrementally. On same-day re-runs this is a <em>no-op</em> because there's no "
    "new data yet."
)
_LABEL_DESCRIPTIONS = {
    "initial_run": (
        "Cold full pipeline from a wiped warehouse — first <code>make init</code> after "
        "<code>make clean</code>. Bootstrap builds schema + seeds + fingerprints; ingest "
        "fetches the full 365-day window from live APIs; dbt rebuilds gold end-to-end."
    ),
    "incremental_run": _INCREMENTAL_DESCRIPTION,
    # Legacy alias — older perf records used clean_baseline for the same
    # scenario. Keep the mapping so historical jsonl still renders correctly.
    "clean_baseline": _INCREMENTAL_DESCRIPTION,
    "backfill_run": (
        "Narrow-window backfill via <code>pipeline ingest --start … --end …</code>. No dbt / "
        "analyze stages — just fetch → bronze → silver MERGE + DQ assertions. Proves the "
        "upsert path is idempotent under repeated invocations."
    ),
}
_LABEL_FALLBACK = "Unlabeled run — no <code>PIPELINE_PERF_LABEL</code> set at invocation."

# Auto-generated daily label — matches the default in
# `pipeline.observability.perf.timed` when no `PIPELINE_PERF_LABEL` is set.
_DAILY_LABEL_DESCRIPTION = (
    "Daily scheduled run — <code>make run</code> (ingest → transform → analyze) against "
    "the already-seeded warehouse. Same-day re-runs merge into this section because "
    "records share the <code>daily-YYYY-MM-DD</code> label."
)

# Historical perf records used the old label `clean_baseline`; render them
# under the current canonical name without rewriting the jsonl.
_LABEL_ALIASES = {
    "clean_baseline": "incremental_run",
}


def _display_label(label: str) -> str:
    if label.startswith("daily-"):
        return f"Daily run · {label.removeprefix('daily-')}"
    return _LABEL_ALIASES.get(label, label)


def _description_for(label: str) -> str:
    if label.startswith("daily-"):
        return _DAILY_LABEL_DESCRIPTION
    return _LABEL_DESCRIPTIONS.get(label, _LABEL_FALLBACK)


def _colour_for(stage: str) -> str:
    prefix = stage.split(":", 1)[0]
    return _STAGE_COLOURS.get(prefix, _DEFAULT_COLOUR)


def _fmt_duration(s: float) -> str:
    if s < 1:
        return f"{s * 1000:.0f} ms"
    if s < 60:
        return f"{s:.2f} s"
    m, sec = divmod(s, 60)
    return f"{int(m)}m {sec:.1f}s"


def _fmt_throughput(rows: int | None, duration: float) -> str:
    if not rows or duration <= 0:
        return "—"
    return f"{rows / duration:,.0f} rows/s"


def _fmt_mb(mb: float | None) -> str:
    """Format MB with sign and unit. None → em-dash for graceful degradation
    on legacy jsonl rows written before memory tracking landed.
    """
    if mb is None:
        return "—"
    if abs(mb) < 0.05:
        return "0 MB"
    sign = "+" if mb > 0 else ""
    return f"{sign}{mb:.1f} MB"


def _aggregate(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group by stage name. One row per stage."""
    buckets: dict[str, list[dict[str, Any]]] = {}
    for r in records:
        buckets.setdefault(r["stage"], []).append(r)

    rows: list[dict[str, Any]] = []
    for stage, recs in buckets.items():
        durations = [r["duration_s"] for r in recs]
        total = sum(durations)
        meta_totals: dict[str, int] = {}
        for r in recs:
            for k, v in (r.get("meta") or {}).items():
                if isinstance(v, int | float) and not isinstance(v, bool):
                    meta_totals[k] = meta_totals.get(k, 0) + int(v)
        any_error = any(r.get("status") == "error" for r in recs)

        # Memory: max-across-invocations for peak (worst case wins) and
        # sum-of-deltas for net change (so a stage that allocates 50 MB
        # twice shows +100 MB total movement). None when no records carry
        # the new fields — old jsonl files predate memory tracking.
        peak_vals = [r["rss_mb_peak"] for r in recs if "rss_mb_peak" in r]
        delta_vals = [
            r["rss_mb_end"] - r["rss_mb_start"]
            for r in recs
            if "rss_mb_end" in r and "rss_mb_start" in r
        ]
        peak_rss_mb = max(peak_vals) if peak_vals else None
        delta_rss_mb = round(sum(delta_vals), 1) if delta_vals else None

        rows.append(
            {
                "stage": stage,
                "count": len(recs),
                "total_s": total,
                "mean_s": total / len(recs),
                "max_s": max(durations),
                "meta": meta_totals,
                "status": "error" if any_error else "ok",
                "peak_rss_mb": peak_rss_mb,
                "delta_rss_mb": delta_rss_mb,
                # Earliest start is the reference point for "when this stage
                # first ran" — used by the per-stage table to order rows
                # chronologically so a reader can trace execution top-to-bottom.
                "first_started_at": min(r["started_at"] for r in recs),
            }
        )
    rows.sort(key=lambda r: r["total_s"], reverse=True)
    return rows


def _e2e_seconds(records: list[dict[str, Any]]) -> float:
    """Wall-clock span between first stage start and last stage end."""
    if not records:
        return 0.0
    starts = [datetime.fromisoformat(r["started_at"]) for r in records]
    ends = [
        datetime.fromisoformat(r["started_at"]) + timedelta(seconds=float(r["duration_s"]))
        for r in records
    ]
    return (max(ends) - min(starts)).total_seconds()


def _bar_chart(rows: list[dict[str, Any]]) -> go.Figure:
    labels = [r["stage"] for r in rows][::-1]  # reverse so top = longest
    values = [r["total_s"] for r in rows][::-1]
    colours = [_colour_for(r["stage"]) for r in rows][::-1]
    hover = [
        f"<b>{r['stage']}</b><br>total {_fmt_duration(r['total_s'])} · "
        f"{r['count']}× · max {_fmt_duration(r['max_s'])}"
        for r in rows
    ][::-1]

    fig = go.Figure(
        go.Bar(
            x=values,
            y=labels,
            orientation="h",
            marker_color=colours,
            hovertemplate="%{customdata}<extra></extra>",
            customdata=hover,
            text=[_fmt_duration(v) for v in values],
            textposition="outside",
            textfont={"family": "JetBrains Mono, monospace", "size": 10.5, "color": INK},
        )
    )
    fig.update_layout(
        height=max(260, 28 * len(labels) + 140),
        margin={"t": 30, "b": 40, "l": 240, "r": 56},
        plot_bgcolor=PAPER_2,
        paper_bgcolor=PAPER_2,
        font={
            "family": "Manrope, -apple-system, BlinkMacSystemFont, sans-serif",
            "size": 12,
            "color": INK,
        },
        hoverlabel={
            "bgcolor": PAPER_0,
            "bordercolor": LINE,
            "font": {"family": "JetBrains Mono, monospace", "size": 11, "color": INK},
        },
        showlegend=False,
    )
    fig.update_xaxes(
        title={
            "text": "seconds",
            "font": {"family": "Manrope, sans-serif", "size": 11.5, "color": INK_2},
        },
        showgrid=True,
        gridcolor=LINE,
        zerolinecolor=AXIS_LINE,
        linecolor=AXIS_LINE,
        tickfont={"family": "JetBrains Mono, monospace", "size": 10.5, "color": TICK_COLOR},
    )
    fig.update_yaxes(
        automargin=True,
        linecolor=AXIS_LINE,
        tickfont={"family": "JetBrains Mono, monospace", "size": 10.5, "color": TICK_COLOR},
    )
    return fig


def _kpi_band(
    records: list[dict[str, Any]],
    aggregated: list[dict[str, Any]],
    window_label: str | None,
    dq: dict[str, int] | None,
) -> str:
    """One horizontal stats strip per run — five facts, one row. Replaces the
    earlier four-panel "KPI band" + redundant run-meta line. Everything a
    reviewer needs to triage a run at a glance sits in one place.
    """
    e2e = _e2e_seconds(records)
    silver_rows = next(
        (
            r["meta"].get("silver_rows_merged", 0)
            for r in aggregated
            if r["stage"] == "ingest:total"
        ),
        0,
    )
    errors = sum(1 for r in aggregated if r["status"] == "error")

    # Errors (stage exceptions) and DQ (assertion results) are separate
    # signals: a crashed ingest call isn't the same thing as duplicate keys
    # on a silver table. Show them in separate cells so one can't mask the
    # other.
    if errors:
        err_value = f"{errors}"
        err_class = "neg"
    else:
        err_value = "0"
        err_class = "pos"

    if dq is None:
        dq_value = "—"
        dq_class = "muted"
    else:
        total_issues = dq["duplicate_keys"] + dq["null_close_rows"] + dq["out_of_range_rows"]
        if total_issues == 0:
            dq_value = "clean"
            dq_class = "pos"
        else:
            dq_value = (
                f"{dq['duplicate_keys']}d · {dq['null_close_rows']}n · {dq['out_of_range_rows']}b"
            )
            dq_class = "neg"

    silver_display = f"{silver_rows:,}" if silver_rows else "0"
    window_display = window_label or "—"

    # Peak RSS = worst high-water across every stage in the run. None when
    # records predate memory tracking — render as em-dash, no class.
    peaks = [r["peak_rss_mb"] for r in aggregated if r.get("peak_rss_mb") is not None]
    if peaks:
        peak_display = f"{max(peaks):.0f} MB"
        peak_class = ""
    else:
        peak_display = "—"
        peak_class = "muted"

    return f"""
<div class="stats-strip">
  <div class="stat">
    <div class="s-label">Wall clock</div>
    <div class="s-value">{_fmt_duration(e2e)}</div>
  </div>
  <div class="stat">
    <div class="s-label">Rows → silver</div>
    <div class="s-value">{silver_display}</div>
  </div>
  <div class="stat">
    <div class="s-label">Data window</div>
    <div class="s-value">{window_display}</div>
  </div>
  <div class="stat">
    <div class="s-label">Stages</div>
    <div class="s-value">{len(aggregated)}</div>
  </div>
  <div class="stat">
    <div class="s-label">Peak RSS</div>
    <div class="s-value {peak_class}">{peak_display}</div>
  </div>
  <div class="stat">
    <div class="s-label">Errors</div>
    <div class="s-value {err_class}">{err_value}</div>
  </div>
  <div class="stat">
    <div class="s-label">DQ</div>
    <div class="s-value {dq_class}">{dq_value}</div>
  </div>
</div>
"""


def _table(rows: list[dict[str, Any]]) -> str:
    # Meta cell highlights only the row-count style signals a reviewer cares
    # about (rows moved, rows merged, DQ flag counts). Drops bookkeeping noise
    # (bounds arrays, stale-symbol lists) that belongs in logs, not a summary.
    movement_keys = {
        "rows",
        "silver_rows_merged",
        "duplicate_keys",
        "null_close_rows",
        "out_of_range_rows",
        "outputs",
    }

    def _meta_cell(meta: dict[str, int]) -> str:
        if not meta:
            return "—"
        interesting = [(k, v) for k, v in meta.items() if k in movement_keys]
        if not interesting:
            return "—"
        return ", ".join(f"{k}={v:,}" for k, v in interesting)

    # Bar chart wants longest-first so the eye lands on the slowest stage; the
    # table wants execution order so a reader can trace the pipeline
    # top-to-bottom. Re-sort locally rather than fighting _aggregate().
    ordered = sorted(rows, key=lambda r: r["first_started_at"])

    body = "\n".join(
        f"<tr>"
        f'<td><span class="stage-chip" style="background:{_colour_for(r["stage"])};"></span>'
        f"<code>{r['stage']}</code></td>"
        f"<td>{r['count']}</td>"
        f"<td>{_fmt_duration(r['total_s'])}</td>"
        f"<td>{_fmt_mb(r.get('peak_rss_mb'))}</td>"
        f"<td>{_fmt_mb(r.get('delta_rss_mb'))}</td>"
        f"<td>{_fmt_throughput(r['meta'].get('rows'), r['total_s'])}</td>"
        f"<td>{_meta_cell(r['meta'])}</td>"
        f'<td class="{"neg" if r["status"] == "error" else "pos"}">{r["status"]}</td>'
        f"</tr>"
        for r in ordered
    )
    return f"""
<table class="perf">
  <thead>
    <tr><th>stage</th><th>count</th><th>total</th>
        <th>peak rss</th><th>Δ rss</th>
        <th>throughput</th><th>rows</th><th>status</th></tr>
  </thead>
  <tbody>
    {body}
  </tbody>
</table>
"""


_FONTS = (
    '<link rel="preconnect" href="https://fonts.googleapis.com">'
    '<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>'
    '<link href="https://fonts.googleapis.com/css2?family=Manrope:wght@300;400;500;600;700;800'
    '&family=JetBrains+Mono:wght@400;500;600&display=swap" rel="stylesheet">'
)

_CSS = """
<style>
  :root {
    --paper-0:  #F3F1EC;
    --paper-1:  #E9E6E0;
    --paper-2:  #F0EDE7;
    --paper-3:  #DAD6CF;
    --paper-4:  #B8B3AB;
    --ink:      #15131A;
    --ink-2:    #3B3540;
    --muted:    #7E7885;
    --orange:   #5E2548;
    --orange-2: #3E1530;
    --orange-3: #B84A9A;
    --carbon:   #15131A;
    --amber:    #D4B072;
    --sage:     #D4B072;
    --success:  #4B8E5E;
    --rust:     #3E1530;
    --line:     #D4CFC7;
    --shadow-paper:
      0 1px 1px rgba(90, 65, 40, 0.05),
      0 2px 4px rgba(90, 65, 40, 0.04),
      0 14px 30px -10px rgba(90, 65, 40, 0.12),
      inset 0 1px 0 rgba(255, 250, 240, 0.7);
    --paper-grain: url("data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='240' height='240'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='2' stitchTiles='stitch'/%3E%3CfeColorMatrix values='0 0 0 0 0.45 0 0 0 0 0.33 0 0 0 0 0.2 0 0 0 0.06 0'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E");
  }

  * { box-sizing: border-box; }
  html, body { margin: 0; padding: 0; }
  body {
    background:
      radial-gradient(1500px 850px at 8% -8%, #DDDAD3 0%, transparent 55%),
      radial-gradient(1400px 750px at 92% 3%, #EAE6DF 0%, transparent 55%),
      radial-gradient(1600px 900px at 60% 100%, #C9C5BE 0%, transparent 60%),
      radial-gradient(1400px 800px at 10% 90%, #D6D2CB 0%, transparent 55%),
      linear-gradient(178deg, #E4E0D9 0%, #DAD6CF 50%, #C9C5BD 100%);
    background-attachment: fixed;
    color: var(--ink);
    font-family: 'Manrope', -apple-system, BlinkMacSystemFont, sans-serif;
    font-size: 15px;
    line-height: 1.6;
    font-weight: 400;
    -webkit-font-smoothing: antialiased;
    letter-spacing: -0.005em;
  }
  body::before {
    content: '';
    position: fixed;
    inset: 0;
    background:
      radial-gradient(1400px 900px at 85% -10%, rgba(94,37,72,0.08), transparent 60%),
      radial-gradient(1100px 700px at -10% 100%, rgba(168,133,84,0.06), transparent 55%);
    pointer-events: none;
    z-index: 0;
  }

  .wrap { position: relative; z-index: 1; max-width: 1240px; margin: 0 auto; padding: 48px 40px 80px; }

  /* ---------- Masthead ---------- */
  .hero {
    position: relative;
    background: var(--paper-grain), var(--paper-0);
    border: 1px solid var(--line);
    border-radius: 10px;
    overflow: hidden;
    margin-bottom: 48px;
    box-shadow: var(--shadow-paper);
  }
  .hero::before {
    content: '';
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 5px;
    background: linear-gradient(180deg, #B84A9A 0%, #5E2548 45%, #3E1530 100%);
  }
  .hero-inner {
    position: relative; z-index: 2;
    display: grid;
    grid-template-columns: 1fr 300px;
    gap: 48px;
    align-items: start;
    padding: 40px 48px 36px 52px;
  }
  .eyebrow {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10.5px;
    letter-spacing: 0.28em;
    text-transform: uppercase;
    color: var(--orange);
    margin-bottom: 18px;
    display: flex; align-items: center; gap: 12px;
    font-weight: 500;
  }
  .eyebrow .dot { width: 6px; height: 6px; border-radius: 50%; background: var(--orange); }
  .eyebrow .brk { color: var(--paper-4); }
  .eyebrow .chip {
    font-size: 9.5px; padding: 3px 8px;
    border: 1px solid var(--paper-4);
    border-radius: 2px;
    color: var(--ink-2);
    letter-spacing: 0.22em;
    background: var(--paper-1);
  }
  .hero h1 {
    font-family: 'Manrope', sans-serif;
    font-weight: 600;
    font-size: 34px;
    line-height: 1.08;
    letter-spacing: -0.028em;
    margin: 0 0 14px;
    color: var(--ink);
  }
  .hero .dek {
    font-size: 14.5px;
    max-width: 620px;
    color: var(--ink-2);
    line-height: 1.6;
  }
  .hero .dek strong { color: var(--ink); font-weight: 600; }
  .hero-meta {
    display: grid; gap: 10px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10.5px; color: var(--muted);
  }
  .hero-meta .m-row {
    display: grid; grid-template-columns: 84px 1fr; gap: 14px;
    align-items: center;
    padding: 8px 0 8px 12px;
    border-left: 1px solid var(--line);
  }
  .hero-meta .m-label { letter-spacing: 0.18em; text-transform: uppercase; font-size: 9.5px; }
  .hero-meta .m-val {
    color: var(--ink);
    background: var(--paper-1);
    border: 1px solid var(--line);
    padding: 4px 9px;
    border-radius: 3px;
    font-size: 11px;
    font-variant-numeric: tabular-nums;
    text-align: center;
  }

  /* ---------- Stats strip (one row of facts per run) ---------- */
  .stats-strip {
    display: grid;
    grid-template-columns: repeat(7, 1fr);
    background: var(--paper-grain), var(--paper-2);
    border: 1px solid var(--line);
    border-left: 3px solid var(--orange);
    border-radius: 6px;
    overflow: hidden;
    box-shadow: var(--shadow-paper);
    margin-bottom: 24px;
  }
  .stat {
    padding: 18px 20px;
    border-right: 1px solid var(--line);
    display: flex; flex-direction: column; gap: 6px;
    min-width: 0;
  }
  .stat:last-child { border-right: none; }
  .stat .s-label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 9.5px;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--muted);
    font-weight: 500;
  }
  .stat .s-value {
    font-family: 'Manrope', sans-serif;
    font-weight: 600;
    font-size: 18px;
    line-height: 1.2;
    font-variant-numeric: tabular-nums;
    color: var(--ink);
    letter-spacing: -0.015em;
    word-break: break-word;
  }
  .stat .s-value.pos { color: var(--success, #4B8E5E); }
  .stat .s-value.neg { color: var(--orange); }
  .stat .s-value.muted { color: var(--muted); font-weight: 500; }

  /* ---------- Run sections ---------- */
  .section { margin-bottom: 64px; }
  .section-head {
    display: flex;
    gap: 32px;
    margin-bottom: 24px;
    align-items: flex-start;
  }
  .section-head .q-col { padding-top: 10px; flex: 1; }
  .section-head .section-num { flex-shrink: 0; width: 100px; }
  .section-num {
    font-family: 'JetBrains Mono', monospace;
    font-weight: 600;
    font-size: 40px;
    line-height: 1;
    color: var(--orange);
    letter-spacing: -0.03em;
  }
  .section-head h2 {
    font-family: 'Manrope', sans-serif;
    font-weight: 600;
    font-size: 24px;
    line-height: 1.25;
    letter-spacing: -0.022em;
    margin: 0 0 12px;
    color: var(--ink);
    text-transform: lowercase;
  }
  .run-meta {
    color: var(--muted);
    font-family: 'JetBrains Mono', monospace;
    font-size: 10.5px;
    display: flex; flex-wrap: wrap; gap: 18px;
    letter-spacing: 0.1em;
    text-transform: uppercase;
  }
  .run-dek {
    color: var(--ink-2);
    font-size: 13.5px;
    line-height: 1.55;
    margin: 0 0 14px;
    max-width: 760px;
  }
  .run-dek code {
    background: var(--paper-1);
    border: 1px solid var(--line);
    padding: 1px 6px;
    border-radius: 3px;
    color: var(--ink);
    font-family: 'JetBrains Mono', monospace;
    font-size: 11.5px;
  }
  .run-meta code {
    background: var(--paper-1);
    border: 1px solid var(--line);
    padding: 2px 7px;
    border-radius: 3px;
    color: var(--ink);
    margin-left: 4px;
    font-size: 10.5px;
    letter-spacing: 0;
    text-transform: none;
  }

  .panel {
    background: var(--paper-grain), var(--paper-2);
    border: 1px solid var(--line);
    box-shadow: var(--shadow-paper);
    border-radius: 6px;
    padding: 28px 32px 24px;
    position: relative;
    margin-top: 18px;
  }
  .panel::before {
    content: '';
    position: absolute;
    top: 0; left: 32px; right: 32px;
    height: 2px;
    background: linear-gradient(90deg, var(--orange) 0%, var(--orange) 30%, transparent 30%);
  }
  .panel h3 {
    margin: 0 0 14px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10.5px;
    letter-spacing: 0.2em;
    text-transform: uppercase;
    color: var(--orange);
    font-weight: 500;
  }
  .chart { width: 100%; }

  /* ---------- Table ---------- */
  table.perf {
    width: 100%;
    border-collapse: collapse;
    background: transparent;
    font-size: 12.5px;
    margin-top: 6px;
  }
  table.perf th, table.perf td {
    border-bottom: 1px solid var(--line);
    padding: 10px 10px;
    text-align: left;
    font-variant-numeric: tabular-nums;
    vertical-align: middle;
  }
  table.perf thead th {
    background: var(--paper-1);
    color: var(--muted);
    font-family: 'JetBrains Mono', monospace;
    font-weight: 500;
    text-transform: uppercase;
    font-size: 9.5px;
    letter-spacing: 0.18em;
    border-bottom: 1px solid var(--line);
    padding: 10px 10px;
  }
  table.perf tbody tr:hover { background: rgba(94,37,72,0.03); }
  table.perf code {
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    color: var(--ink);
    background: transparent;
    padding: 0;
  }
  .stage-chip {
    display: inline-block;
    width: 8px; height: 8px;
    border-radius: 2px;
    margin-right: 8px;
    vertical-align: middle;
  }
  /* Status cells use proper success-green — bronze reads as warning, not ok. */
  .pos { color: var(--success); font-weight: 600; }
  .neg { color: var(--orange); font-weight: 700; }

  /* ---------- Empty / footer ---------- */
  .empty {
    padding: 3rem 2rem;
    text-align: center;
    color: var(--muted);
    background: var(--paper-grain), var(--paper-2);
    border: 1px solid var(--line);
    border-radius: 6px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 12px;
    letter-spacing: 0.08em;
  }
  .empty code {
    background: var(--paper-1);
    border: 1px solid var(--line);
    padding: 1px 7px;
    border-radius: 3px;
    color: var(--ink);
  }

  footer {
    margin-top: 80px;
    padding-top: 32px;
    border-top: 1px solid var(--line);
    display: grid;
    grid-template-columns: 1fr auto;
    gap: 24px;
    font-size: 12px;
    color: var(--muted);
    font-family: 'JetBrains Mono', monospace;
    letter-spacing: 0.06em;
  }

  .js-plotly-plot .plotly .modebar { display: none !important; }

  @media (max-width: 900px) {
    .wrap { padding: 24px 20px 60px; }
    .hero-inner { grid-template-columns: 1fr; padding: 32px 28px 36px; }
    .hero h1 { font-size: 26px; }
    .kpi-band { grid-template-columns: 1fr; }
    .kpi-group { grid-template-columns: 1fr; }
    .kpi { border-right: none; border-bottom: 1px solid var(--line); }
    .section-head { flex-direction: column; gap: 10px; }
    .section-head .section-num { width: auto; }
    .panel { padding: 24px 20px 18px; }
    table.perf { font-size: 11.5px; }
    table.perf th, table.perf td { padding: 8px 6px; }
  }
</style>
"""


def write_perf_report() -> Path:
    """Render `DATA_REPORTS/performance_report.html`. Returns its path."""
    DATA_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    out_path = DATA_REPORT_DIR / "performance_report.html"

    records = read_perf_log()
    generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

    if not records:
        out_path.write_text(
            "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
            f"<title>Pipeline performance</title>{_FONTS}{_CSS}</head><body>"
            "<div class='wrap'>"
            "<div class='hero'><div class='hero-inner'><div>"
            "<div class='eyebrow'><span class='dot'></span>"
            "<span>Observability</span><span class='brk'>/</span>"
            "<span>Pipeline performance</span></div>"
            "<h1>No runs recorded yet.</h1>"
            "<p class='dek'>Run <code>make run</code> to generate timings. Records land in "
            "<code>outputs/performance.jsonl</code> and this report rebuilds on every "
            "<code>make analyze</code>.</p></div>"
            "<div class='hero-meta'>"
            f"<div class='m-row'><span class='m-label'>Generated</span>"
            f"<span class='m-val'>{generated_at}</span></div>"
            "</div></div></div>"
            "<div class='empty'>No performance records found.</div>"
            "</div></body></html>"
        )
        log.info("perf_report.written", path=str(out_path), records=0)
        return out_path

    groups = _group_by_label(records)
    sections_html = "\n".join(
        _render_section(i + 1, label, group) for i, (label, group) in enumerate(groups)
    )

    total_records = len(records)
    total_runs = len(groups)

    # "Data window" = union of every ingest stage's (start_date, end_date).
    # Skips no-op ranges where start > end (incremental caught up to today).
    # Falls back to the wall-clock capture date when no ingest meta is present.
    starts: list[str] = []
    ends: list[str] = []
    for r in records:
        if r["stage"] != "ingest:total":
            continue
        m = r.get("meta") or {}
        s, e = m.get("start_date"), m.get("end_date")
        if isinstance(s, str) and isinstance(e, str) and s <= e:
            starts.append(s)
            ends.append(e)
    if starts and ends:
        data_window = min(starts) if min(starts) == max(ends) else f"{min(starts)} → {max(ends)}"
    else:
        earliest = min(r["started_at"] for r in records)[:10]
        latest = max(r["started_at"] for r in records)[:10]
        data_window = earliest if earliest == latest else f"{earliest} → {latest}"

    doc = f"""<!doctype html><html lang='en'><head><meta charset='utf-8'>
<title>Pipeline performance — runtime review</title>
{_FONTS}{_CSS}</head><body>
<div class="wrap">
  <div class="hero">
    <div class="hero-inner">
      <div>
        <div class="eyebrow">
          <span class="dot"></span>
          <span>Observability</span>
          <span class="brk">/</span>
          <span>Pipeline performance</span>
          <span class="chip">{total_runs} RUN{"S" if total_runs != 1 else ""}</span>
        </div>
        <h1>Runtime review — stage timings, throughput, and errors.</h1>
        <p class="dek">A per-run breakdown of the <strong>{total_records}</strong> stage records written to
          <code style="background:var(--paper-1);border:1px solid var(--line);padding:1px 6px;border-radius:3px;font-size:12px;">outputs/performance.jsonl</code>.
          Each section below is one labelled run; stages are sorted by total time, colour-coded by
          pipeline phase (ingest, dbt, analyze).</p>
      </div>
      <div class="hero-meta">
        <div class="m-row"><span class="m-label">Generated</span><span class="m-val">{generated_at}</span></div>
        <div class="m-row"><span class="m-label">Runs</span><span class="m-val">{total_runs}</span></div>
        <div class="m-row"><span class="m-label">Records</span><span class="m-val">{total_records}</span></div>
        <div class="m-row"><span class="m-label">Data window</span><span class="m-val">{data_window}</span></div>
      </div>
    </div>
  </div>

  {sections_html}

  <footer>
    <div>Pipeline performance · runtime review · {datetime.now(UTC).year}</div>
    <div>./outputs/performance.jsonl · plotly</div>
  </footer>
</div>
</body></html>"""

    out_path.write_text(doc)
    log.info("perf_report.written", path=str(out_path), records=len(records), runs=len(groups))
    return out_path


def _group_by_label(
    records: list[dict[str, Any]],
) -> list[tuple[str, list[dict[str, Any]]]]:
    """Group records by `run_label`, ordered by earliest `started_at` so
    sections render chronologically.

    Fresh records always have a label (``timed()`` auto-falls-back to
    ``daily-YYYY-MM-DD`` when ``PIPELINE_PERF_LABEL`` is unset), so the
    ``unlabeled`` bucket only shows up for historical jsonl rows written
    before that fallback existed. When it does appear alongside real labels
    we drop it — those records are one-off noise, not a narrative run. If
    *all* records are unlabeled we keep the bucket so the report isn't empty.
    """
    buckets: dict[str, list[dict[str, Any]]] = {}
    for r in records:
        label = r.get("run_label") or "unlabeled"
        buckets.setdefault(label, []).append(r)
    if "unlabeled" in buckets and len(buckets) > 1:
        del buckets["unlabeled"]
    return sorted(buckets.items(), key=lambda kv: min(x["started_at"] for x in kv[1]))


def _extract_ingest_window(records: list[dict[str, Any]]) -> tuple[str, str] | None:
    """Pull the effective (start_date, end_date) the ingest stage actually wrote.
    Used to caption *what* the run moved — especially load-bearing for backfills,
    where the only interesting signal is which window got filled.
    """
    for r in records:
        if r["stage"] != "ingest:total":
            continue
        meta = r.get("meta") or {}
        start = meta.get("start_date")
        end = meta.get("end_date")
        if start and end:
            return str(start), str(end)
    return None


def _extract_dq(records: list[dict[str, Any]]) -> dict[str, int] | None:
    """Aggregate DQ assertion results written to the `ingest:dq_assertions` meta."""
    for r in records:
        if r["stage"] != "ingest:dq_assertions":
            continue
        meta = r.get("meta") or {}
        return {
            "duplicate_keys": int(meta.get("duplicate_keys", 0) or 0),
            "null_close_rows": int(meta.get("null_close_rows", 0) or 0),
            "out_of_range_rows": int(meta.get("out_of_range_rows", 0) or 0),
        }
    return None


def _render_section(index: int, label: str, records: list[dict[str, Any]]) -> str:
    aggregated = _aggregate(records)
    fig = _bar_chart(aggregated)
    chart_html = fig.to_html(
        full_html=False,
        include_plotlyjs="cdn" if index == 1 else False,
        config={"displaylogo": False, "responsive": True},
    )
    started_short = min(r["started_at"] for r in records)[:19].replace("T", " ")
    description = _description_for(label)

    window = _extract_ingest_window(records)
    if window:
        start, end = window
        if start == end:
            win_label = start
        elif start > end:
            # Incremental caught up: start = max(silver.date)+1, end = yesterday.
            # When the warehouse is already fresh, start overshoots end and the
            # ingest is a no-op. Show that explicitly rather than a reversed range.
            win_label = f"no-op · up to {end}"
        else:
            win_label = f"{start} → {end}"
    else:
        win_label = None
    dq = _extract_dq(records)

    return f"""
<section class="section">
  <div class="section-head">
    <div class="section-num">R{index:02d}</div>
    <div class="q-col">
      <h2>{_display_label(label)}</h2>
      <p class="run-dek">{description}</p>
      <div class="run-meta">
        <span>Started<code>{started_short}</code></span>
      </div>
    </div>
  </div>
  {_kpi_band(records, aggregated, win_label, dq)}
  <div class="panel">
    <h3>Stage durations · total time spent</h3>
    <div class="chart">{chart_html}</div>
  </div>
  <div class="panel">
    <h3>Per-stage detail</h3>
    {_table(aggregated)}
  </div>
</section>
"""
