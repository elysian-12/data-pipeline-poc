"""Self-contained HTML data-analysis panel.

Chart-first visualisations answering Q1–Q4, each paired with a short
data-storytelling paragraph pulling concrete numbers from the frames.
"""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import plotly.graph_objects as go  # type: ignore[import-untyped]
import polars as pl

from analysis.dca import dca, lump_sum
from pipeline.config import AnalysisConfig
from pipeline.observability.logging import get_logger

log = get_logger(__name__)

DATA_REPORT_DIR = Path("DATA_REPORTS")

# Visual language — Porsche 911 Amethyst / Bronze / Concrete paper theme.
# BTC is the focal asset, so it takes the deep amethyst (--orange in design tokens);
# gains read bronze, losses read deep amethyst so the palette only ever uses two
# semantic hues for direction.
BTC_COLOUR = "#5E2548"  # deep amethyst — BTC always
NEUTRAL = "#7E7885"  # grey-plum tertiary text
POSITIVE = "#D4B072"  # bronze — gains
NEGATIVE = "#3E1530"  # deeper amethyst — losses
INK = "#15131A"
INK_2 = "#3B3540"
PAPER_0 = "#F3F1EC"
PAPER_1 = "#E9E6E0"
PAPER_2 = "#F0EDE7"  # card / chart paper
PAPER_3 = "#DAD6CF"
LINE = "#D4CFC7"
AXIS_LINE = "#B8B3AB"
TICK_COLOR = "#5A4A45"

# Per-symbol assignment keeps the legend and every chart in lockstep.
# Canonical Porsche Amethyst asset palette from Claude Design — do not
# "optimise" saturation, this is the authoritative spec.
SYMBOL_COLOUR = {
    "AAPL": "#B84A9A",  # bright magenta-plum
    "GOOGL": "#D4B072",  # metallic gold
    "MSFT": "#7AA3C4",  # slate blue
    "SPY": "#3D4A57",  # deep steel
    # FX tickers arrive from Massive with the 'C:' prefix (see CLAUDE.md).
    # Keep both keys so display-name trimming doesn't break colour lookup.
    "EURUSD": "#E6B84A",  # amber gold
    "C:EURUSD": "#E6B84A",
    "GBPUSD": "#E07DB2",  # rose
    "C:GBPUSD": "#E07DB2",
    "USD": "#1A1A1C",  # true black — stablecoin anchor
}
PALETTE = [
    "#B84A9A",
    "#D4B072",
    "#7AA3C4",
    "#3D4A57",
    "#E6B84A",
    "#E07DB2",
    "#1A1A1C",
]


def pct(x: float | None) -> str:
    """Percent formatter shared with the markdown report writer."""
    return "n/a" if x is None else f"{x * 100:.2f}%"


def _signed_pct(x: float) -> str:
    """Percent with an explicit leading sign, for hero delta pills."""
    return f"{x * 100:+.2f}%"


def _symbol_colour(symbol: str, btc_symbol: str, others: list[str]) -> str:
    if symbol == btc_symbol:
        return BTC_COLOUR
    if symbol in SYMBOL_COLOUR:
        return SYMBOL_COLOUR[symbol]
    idx = others.index(symbol) if symbol in others else 0
    return PALETTE[idx % len(PALETTE)]


_SORT_HOVER_JS = """
<script>
(function () {
  // Extract the last signed number (optionally $-prefixed) from a hover row.
  // Handles "+41.72%", "$1,234.56", "-2.81%", etc.
  function parseNum(txt) {
    var m = txt.match(/[+-]?\\$?[\\d,]+(?:\\.\\d+)?/g);
    if (!m) return NaN;
    return parseFloat(m[m.length - 1].replace(/[$,]/g, ''));
  }

  // Plotly 3.x unified hover renders as a legend-style box:
  //   g.hoverlayer > g.legend > g.scrollbox > g.groups > g.traces × N
  // Each g.traces has its own transform="translate(x, y)" that stacks it
  // vertically inside the tooltip. To sort by value descending, we keep
  // the y-slots fixed and reassign them so the highest-value trace's
  // g.traces takes the topmost (smallest-y) slot.
  function sortHover(gd) {
    var traces = gd.querySelectorAll(
      'g.hoverlayer g.legend g.groups g.traces'
    );
    if (traces.length < 2) return;

    var rows = [];
    for (var i = 0; i < traces.length; i++) {
      var g = traces[i];
      var tf = g.getAttribute('transform') || '';
      var m = tf.match(/translate\\(\\s*([-\\d.eE+]+)[,\\s]+([-\\d.eE+]+)\\s*\\)/);
      if (!m) continue;
      var tx = parseFloat(m[1]);
      var ty = parseFloat(m[2]);
      // text.textContent includes both the first tspan (trace name) and
      // the trailing text node (": +37.80%"). Full row = "AAPL: +37.80%".
      var text = g.querySelector('text');
      var plain = text ? text.textContent : '';
      rows.push({ g: g, tx: tx, ty: ty, num: parseNum(plain) });
    }
    if (rows.length < 2) return;

    // Fixed y-slots (plotly's own layout), top-to-bottom.
    var slots = rows.map(function (r) { return { tx: r.tx, ty: r.ty }; })
                    .sort(function (a, b) { return a.ty - b.ty; });

    // Desired order: descending numeric value. NaN sinks to bottom.
    var ordered = rows.slice().sort(function (a, b) {
      if (isNaN(a.num) && isNaN(b.num)) return 0;
      if (isNaN(a.num)) return 1;
      if (isNaN(b.num)) return -1;
      return b.num - a.num;
    });

    // Already sorted? Bail so we don't trigger observers / churn.
    var already = true;
    for (var j = 0; j < ordered.length; j++) {
      if (Math.abs(ordered[j].ty - slots[j].ty) > 0.5) { already = false; break; }
    }
    if (already) return;

    // Reassign transforms.
    for (var k = 0; k < ordered.length; k++) {
      var target = 'translate(' + slots[k].tx + ',' + slots[k].ty + ')';
      if (ordered[k].g.getAttribute('transform') !== target) {
        ordered[k].g.setAttribute('transform', target);
      }
    }
  }

  // Hovertext elements live only as long as the hover is active. Plotly
  // may run multiple positioning passes per hover, so sort in a short
  // retry burst: immediate, next rAF, +16ms, +50ms, +150ms. Idempotent
  // because sortHover bails when already ordered.
  function sortBurst(gd) {
    sortHover(gd);
    requestAnimationFrame(function () { sortHover(gd); });
    setTimeout(function () { sortHover(gd); }, 16);
    setTimeout(function () { sortHover(gd); }, 50);
    setTimeout(function () { sortHover(gd); }, 150);
  }

  function attach(gd) {
    if (gd.__sortHookAttached) return;
    gd.__sortHookAttached = true;

    // Primary trigger: plotly_hover event fires after plotly commits
    // the hover label DOM. Fallback: MutationObserver for edge cases
    // where the event doesn't fire (e.g. synthetic hover, other libs).
    var onHover = function () { sortBurst(gd); };
    if (typeof gd.on === 'function') {
      gd.on('plotly_hover', onHover);
      gd.on('plotly_afterplot', onHover);
    } else {
      // gd not yet plotly-initialised — poll briefly.
      var tries = 0;
      var t = setInterval(function () {
        tries++;
        if (typeof gd.on === 'function') {
          clearInterval(t);
          gd.on('plotly_hover', onHover);
          gd.on('plotly_afterplot', onHover);
        } else if (tries > 40) {
          clearInterval(t);
        }
      }, 50);
    }

    // Fallback observer — catches any hover DOM mutation we miss.
    var obs = new MutationObserver(function () { sortBurst(gd); });
    obs.observe(gd, {
      childList: true, subtree: true,
      attributes: true, attributeFilter: ['transform'],
    });
  }

  function init() {
    document.querySelectorAll('.plotly-graph-div').forEach(attach);
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
</script>
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
    margin-bottom: 56px;
    box-shadow: var(--shadow-paper);
  }
  .hero::before {
    content: '';
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 5px;
    background: linear-gradient(180deg, #B84A9A 0%, #5E2548 45%, #3E1530 100%);
  }
  .hero::after {
    content: '';
    position: absolute;
    inset: 0;
    background: radial-gradient(900px 400px at 100% 0%, rgba(94,37,72,0.06), transparent 55%);
    pointer-events: none;
  }
  .hero-inner {
    position: relative; z-index: 2;
    display: grid;
    grid-template-columns: 1fr 300px;
    gap: 56px;
    align-items: start;
    padding: 44px 48px 44px 52px;
  }
  .eyebrow {
    font-family: 'JetBrains Mono', ui-monospace, monospace;
    font-size: 10.5px;
    letter-spacing: 0.28em;
    text-transform: uppercase;
    color: var(--orange);
    margin-bottom: 22px;
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
    font-size: 42px;
    line-height: 1.06;
    letter-spacing: -0.03em;
    margin: 0;
    color: var(--ink);
  }
  .hero h1 .l1 { display: block; }
  .hero h1 .l2 {
    display: flex; align-items: center; gap: 14px;
    font-family: 'JetBrains Mono', monospace;
    color: var(--orange);
    font-size: 9.5px;
    letter-spacing: 0.34em;
    text-transform: uppercase;
    margin: 12px 0;
    line-height: 1;
    font-weight: 500;
  }
  .hero h1 .l2::after {
    content: ''; flex: 1; height: 1px;
    background: linear-gradient(90deg, rgba(94,37,72,0.4), transparent);
  }
  .hero h1 .l3 { display: block; font-weight: 700; color: var(--carbon); }
  .hero .dek {
    margin-top: 22px;
    font-size: 15px;
    max-width: 560px;
    color: var(--ink-2);
    line-height: 1.6;
  }
  .hero .dek strong { color: var(--ink); font-weight: 600; }

  .hero-delta {
    display: inline-flex; align-items: stretch;
    margin-top: 22px;
    border: 1px solid var(--line);
    border-radius: 4px;
    overflow: hidden;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    background: var(--paper-2);
  }
  .hero-delta .seg {
    padding: 9px 14px;
    display: flex; align-items: center; gap: 8px;
    border-right: 1px solid var(--line);
    color: var(--ink-2);
  }
  .hero-delta .seg:last-child { border-right: none; }
  .hero-delta .seg .n { color: var(--ink); font-weight: 600; font-size: 11px; font-variant-numeric: tabular-nums; }
  .hero-delta .seg.neg .n,
  .hero-delta .seg.neg .tri { color: var(--orange); }
  .hero-delta .seg.pos .n,
  .hero-delta .seg.pos .tri { color: var(--sage); }

  .hero-coord {
    margin-top: 28px;
    display: inline-flex; gap: 20px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    color: var(--muted);
    letter-spacing: 0.14em;
    text-transform: uppercase;
  }
  .hero-coord span { display: inline-flex; align-items: center; gap: 6px; }
  .hero-coord span::before {
    content: ''; width: 4px; height: 4px; border-radius: 50%;
    background: var(--orange);
  }

  .hero-meta {
    display: grid;
    gap: 10px;
    font-family: 'JetBrains Mono', ui-monospace, monospace;
    font-size: 10.5px;
    color: var(--muted);
  }
  .hero-meta .m-row {
    display: grid; grid-template-columns: 84px 1fr; gap: 14px;
    align-items: center;
    padding: 8px 0 8px 12px;
    border-left: 1px solid var(--line);
  }
  .hero-meta .m-label { letter-spacing: 0.18em; text-transform: uppercase; font-size: 9.5px; color: var(--muted); }
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

  /* ---------- KPI band ---------- */
  .kpi-band {
    display: grid;
    grid-template-columns: 3fr 1.25fr;
    gap: 14px;
    margin-bottom: 64px;
  }
  .kpi-group {
    display: grid;
    grid-template-columns: repeat(3, 1fr);
    background: var(--paper-grain), var(--paper-2);
    border: 1px solid var(--line);
    border-left: 3px solid var(--orange);
    border-radius: 6px;
    overflow: hidden;
    position: relative;
    box-shadow: var(--shadow-paper);
  }
  .kpi-group::before {
    content: 'Outcome · $1,000 proposition';
    position: absolute; top: 12px; left: 20px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 9.5px; letter-spacing: 0.22em; text-transform: uppercase;
    color: var(--orange);
    font-weight: 500;
  }
  .kpi-standalone {
    background: var(--paper-grain), var(--paper-2);
    border: 1px solid var(--line);
    border-left: 3px solid var(--amber);
    border-radius: 6px;
    overflow: hidden;
    position: relative;
    box-shadow: var(--shadow-paper);
  }
  .kpi-standalone::before {
    content: 'Volatility profile';
    position: absolute; top: 12px; left: 20px;
    font-family: 'JetBrains Mono', monospace;
    font-size: 9.5px; letter-spacing: 0.22em; text-transform: uppercase;
    color: var(--amber);
    font-weight: 500;
  }
  .kpi {
    padding: 40px 22px 24px;
    border-right: 1px solid var(--line);
    position: relative;
    display: flex; flex-direction: column;
    min-height: 132px;
  }
  .kpi:last-child { border-right: none; }
  .kpi .k-label {
    font-family: 'JetBrains Mono', monospace;
    font-size: 10px;
    letter-spacing: 0.16em;
    text-transform: uppercase;
    color: var(--muted);
    margin-bottom: 10px;
  }
  .kpi .k-value {
    font-family: 'Manrope', sans-serif;
    font-weight: 600;
    font-size: 30px;
    line-height: 1;
    font-variant-numeric: tabular-nums;
    color: var(--ink);
    letter-spacing: -0.025em;
    display: flex; align-items: baseline; gap: 6px;
  }
  .kpi .k-value .arrow { font-size: 0.65em; font-weight: 500; opacity: 0.9; }
  .kpi .k-value.neg { color: var(--orange); }
  .kpi .k-value.pos { color: var(--sage); }
  .kpi .k-sub {
    margin-top: 6px;
    font-size: 11.5px;
    color: var(--ink-2);
    line-height: 1.4;
  }

  /* ---------- Palette legend ---------- */
  .palette-legend {
    display: grid;
    grid-template-columns: repeat(var(--legend-cols, 8), 1fr);
    background: var(--paper-grain), var(--paper-2);
    border: 1px solid var(--line);
    border-radius: 6px;
    overflow: hidden;
    margin-bottom: 64px;
    box-shadow: var(--shadow-paper);
  }
  .legend-item {
    padding: 18px 16px;
    border-right: 1px solid var(--line);
    display: grid;
    gap: 10px;
  }
  .legend-item:last-child { border-right: none; }
  .legend-swatch { height: 4px; border-radius: 2px; }
  .legend-ticker {
    font-family: 'JetBrains Mono', monospace;
    font-size: 11.5px;
    font-weight: 500;
    letter-spacing: 0.06em;
    color: var(--ink);
  }
  .legend-name { font-size: 11px; color: var(--muted); line-height: 1.3; }

  /* ---------- Section ---------- */
  .section { margin-bottom: 72px; }
  .section-head {
    display: flex;
    gap: 32px;
    margin-bottom: 28px;
    align-items: flex-start;
  }
  .section-head .q-col { padding-top: 10px; flex: 1; }
  .section-head .section-num { flex-shrink: 0; width: 100px; }
  .section-num {
    font-family: 'JetBrains Mono', ui-monospace, monospace;
    font-weight: 600;
    font-size: 44px;
    line-height: 1;
    color: var(--orange);
    letter-spacing: -0.03em;
  }
  .section-head h2 {
    font-family: 'Manrope', sans-serif;
    font-weight: 600;
    font-size: 26px;
    line-height: 1.25;
    letter-spacing: -0.022em;
    margin: 0 0 16px;
    color: var(--ink);
    max-width: 780px;
  }
  .section-head .q-sub {
    margin: 0;
    font-size: 14.5px;
    color: var(--ink-2);
    max-width: 780px;
    line-height: 1.55;
  }
  .section-head .q-sub strong { color: var(--orange); font-weight: 500; }

  .panel {
    background: var(--paper-grain), var(--paper-2);
    border: 1px solid var(--line);
    box-shadow: var(--shadow-paper);
    border-radius: 6px;
    padding: 32px 32px 28px;
    position: relative;
  }
  .panel::before {
    content: '';
    position: absolute;
    top: 0; left: 32px; right: 32px;
    height: 2px;
    background: linear-gradient(90deg, var(--orange) 0%, var(--orange) 30%, transparent 30%);
  }
  .chart { width: 100%; }

  .story {
    margin-top: 22px;
    padding: 22px 26px;
    background: linear-gradient(135deg, rgba(94,37,72,0.04), rgba(168,133,84,0.05));
    border: 1px solid var(--paper-3);
    border-left: 3px solid var(--orange);
    border-radius: 4px;
    font-size: 14.5px;
    line-height: 1.6;
    color: var(--ink-2);
  }
  .story strong { color: var(--ink); font-weight: 600; }
  .story .label {
    display: block;
    font-family: 'JetBrains Mono', monospace;
    font-size: 10.5px;
    letter-spacing: 0.18em;
    text-transform: uppercase;
    color: var(--orange);
    margin-bottom: 14px;
  }
  .story code {
    background: var(--paper-1);
    border: 1px solid var(--line);
    padding: 1px 6px;
    border-radius: 3px;
    font-size: 12px;
    color: var(--ink);
  }

  /* ---------- Footer ---------- */
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
  footer .mark {
    font-family: 'Manrope', sans-serif;
    font-weight: 600;
    font-size: 13px;
    color: var(--orange);
    letter-spacing: 0.02em;
    text-transform: uppercase;
  }

  .js-plotly-plot .plotly .modebar { display: none !important; }

  @media (max-width: 900px) {
    .wrap { padding: 24px 20px 60px; }
    .hero-inner { grid-template-columns: 1fr; padding: 32px 28px 40px; }
    .hero h1 { font-size: 34px; }
    .kpi-band { grid-template-columns: 1fr; }
    .kpi-group { grid-template-columns: repeat(2, 1fr); }
    .kpi { border-bottom: 1px solid var(--line); }
    .palette-legend { grid-template-columns: repeat(2, 1fr) !important; }
    .section-head { flex-direction: column; gap: 12px; }
    .section-head .section-num { width: auto; }
    .section-num { font-size: 44px; }
    .section-head h2 { font-size: 22px; }
    .panel { padding: 24px 20px 20px; }
  }
</style>
"""


def _fig_html(fig: go.Figure, *, plotly_js: str | bool) -> str:
    """Render a plotly Figure as a div.

    `plotly_js` is the `include_plotlyjs` argument for `Figure.to_html`:
    - ``"cdn"`` — emits a ``<script src=…plotly…>``; small HTML, needs internet
    - ``"inline"`` — embeds plotly.js in the page; ~5 MB HTML, self-contained/static
    - ``False`` — omit the library (use on every figure after the first)
    """
    return str(
        fig.to_html(
            full_html=False,
            include_plotlyjs=plotly_js,
            config={"displaylogo": False, "responsive": True},
        )
    )


def _apply_chart_theme(fig: go.Figure, height: int = 420) -> go.Figure:
    """Paper-feel Plotly theme that blends into the panel card.

    Titles render in Manrope to match the page; tick labels stay in JetBrains
    Mono so numbers line up (tabular-nums) and feel like a precision readout.
    """
    fig.update_layout(
        height=height,
        margin={"t": 40, "b": 56, "l": 64, "r": 28},
        plot_bgcolor=PAPER_2,
        paper_bgcolor=PAPER_2,
        font={
            "family": "Manrope, -apple-system, BlinkMacSystemFont, sans-serif",
            "size": 12,
            "color": INK,
        },
        legend={
            "orientation": "h",
            "yanchor": "bottom",
            "y": 1.02,
            "xanchor": "left",
            "x": 0,
            "font": {"family": "JetBrains Mono, monospace", "size": 10.5, "color": INK_2},
            "bgcolor": "rgba(0,0,0,0)",
        },
        hovermode="x unified",
        hoverlabel={
            "bgcolor": PAPER_0,
            "bordercolor": LINE,
            "font": {"family": "JetBrains Mono, monospace", "size": 11, "color": INK},
        },
        colorway=[BTC_COLOUR, *PALETTE],
    )
    fig.update_xaxes(
        showgrid=False,
        showline=True,
        linecolor=AXIS_LINE,
        ticks="outside",
        tickcolor=AXIS_LINE,
        tickfont={"family": "JetBrains Mono, monospace", "size": 10.5, "color": TICK_COLOR},
        title_standoff=12,
        title_font={"family": "Manrope, sans-serif", "size": 11.5, "color": INK_2},
    )
    fig.update_yaxes(
        gridcolor=LINE,
        zerolinecolor=AXIS_LINE,
        linecolor=AXIS_LINE,
        tickfont={"family": "JetBrains Mono, monospace", "size": 10.5, "color": TICK_COLOR},
        title_font={"family": "Manrope, sans-serif", "size": 11.5, "color": INK_2},
    )
    return fig


# ---------- Q1 ----------


def _q1_chart(prices: pl.DataFrame, btc_symbol: str, as_of: date) -> go.Figure:
    """% change from 1Y ago, per symbol. Every line starts at 0%."""
    start = as_of - timedelta(days=365)
    window = prices.filter((pl.col("date") >= start) & (pl.col("date") <= as_of)).sort(
        ["symbol", "date"]
    )
    if window.is_empty():
        return go.Figure()

    others = [s for s in sorted(window["symbol"].unique().to_list()) if s != btc_symbol]
    fig = go.Figure()

    # Plot non-BTC first so BTC draws on top.
    for sym in [*others, btc_symbol]:
        s = window.filter(pl.col("symbol") == sym)
        if s.is_empty():
            continue
        base = float(s["close"][0])
        if base == 0:
            continue
        pct_change = ((s["close"] / base - 1.0) * 100.0).to_list()
        is_btc = sym == btc_symbol
        hover_strs = [f"<b>{sym}</b>: {v:+.2f}%" for v in pct_change]
        fig.add_trace(
            go.Scatter(
                x=s["date"].to_list(),
                y=pct_change,
                mode="lines",
                name=sym,
                line={
                    "color": _symbol_colour(sym, btc_symbol, others),
                    "width": 3.2 if is_btc else 1.6,
                },
                opacity=1.0 if is_btc else 0.85,
                customdata=hover_strs,
                hovertemplate="%{customdata}<extra></extra>",
            )
        )
    fig.add_hline(y=0, line_dash="dot", line_color=AXIS_LINE, opacity=0.5)
    fig.update_yaxes(title="% change from 1 year ago", ticksuffix="%")
    return _apply_chart_theme(fig, height=460)


def _q1_winners_chart(returns_df: pl.DataFrame, btc_symbol: str) -> go.Figure:
    """Grouped bar: all assets' return per window, BTC in gold for direct comparison."""
    if returns_df.is_empty():
        return go.Figure()

    window_order = ["7d", "1m", "3m", "6m", "ytd", "1y"]
    windows = [w for w in window_order if w in returns_df["window"].unique().to_list()]
    if not windows:
        return go.Figure()

    symbols = sorted(returns_df["symbol"].unique().to_list())
    others = [s for s in symbols if s != btc_symbol]

    fig = go.Figure()
    for sym in [*others, btc_symbol]:
        vals: list[float | None] = []
        for w in windows:
            cell = returns_df.filter((pl.col("window") == w) & (pl.col("symbol") == sym)).select(
                "return"
            )
            if cell.is_empty() or cell[0, 0] is None:
                vals.append(None)
            else:
                vals.append(float(cell[0, 0]) * 100)
        is_btc = sym == btc_symbol
        hover_strs = [
            f"<b>{sym}</b> · {w.upper()}<br>{v:+.2f}%"
            if v is not None
            else f"<b>{sym}</b> · {w.upper()}<br>n/a"
            for w, v in zip(windows, vals, strict=True)
        ]
        fig.add_trace(
            go.Bar(
                x=[w.upper() for w in windows],
                y=vals,
                name=sym,
                marker_color=(BTC_COLOUR if is_btc else _symbol_colour(sym, btc_symbol, others)),
                customdata=hover_strs,
                hovertemplate="%{customdata}<extra></extra>",
            )
        )
    fig.add_hline(y=0, line_dash="dot", line_color=AXIS_LINE, opacity=0.5)
    fig.update_layout(barmode="group", bargroupgap=0.05)
    fig.update_xaxes(title="Lookback window")
    fig.update_yaxes(title="Return over window", ticksuffix="%")
    fig = _apply_chart_theme(fig, height=420)
    fig.update_layout(hovermode="closest")  # per-bar tooltip for grouped bars
    return fig


def _q1_story(returns_df: pl.DataFrame, btc_symbol: str) -> str:
    if returns_df.is_empty():
        return "<em>No return data available.</em>"

    window_order = ["7d", "1m", "3m", "6m", "ytd", "1y"]
    windows = [w for w in window_order if w in returns_df["window"].unique().to_list()]

    beats = 0
    total = 0
    long_window_winner: tuple[str, str, float, float] | None = None
    for w in windows:
        slc = returns_df.filter(
            (pl.col("window") == w) & (pl.col("symbol") != btc_symbol)
        ).drop_nulls("return")
        if slc.is_empty():
            continue
        winner = slc.sort("return", descending=True).row(0, named=True)
        if winner["beats_btc"]:
            beats += 1
        total += 1
        if w == "1y":
            long_window_winner = (
                w,
                str(winner["symbol"]),
                float(winner["return"]),
                float(slc["btc_return"][0]) if slc["btc_return"][0] is not None else 0.0,
            )

    summary = (
        f"Across the {total} windows measured, {btc_symbol} was beaten in <strong>{beats}</strong>."
    )
    if long_window_winner is not None:
        _, sym, ret, btc_ret = long_window_winner
        lede = (
            f"Over the full <strong>1-year</strong> window, <strong>{sym}</strong> "
            f"returned <strong>{pct(ret)}</strong> vs {btc_symbol}'s "
            f"<strong>{pct(btc_ret)}</strong>."
        )
    else:
        lede = ""
    return (
        "The line chart above is <strong>% change from one year ago</strong> — every "
        "symbol starts at 0% and the distance from the dashed zero line is the total "
        f"return to date. Lines above the gold {btc_symbol} curve outperformed it, lines "
        f"below did not. {lede} {summary} The grouped bars beneath read the same story "
        "window-by-window: each cluster is one lookback period, one bar per asset, BTC in gold."
    )


# ---------- Q2 ----------


def _q2_growth_chart(
    prices: pl.DataFrame, lump_df: pl.DataFrame, btc_symbol: str, as_of: date
) -> go.Figure:
    """Growth of $1,000 over the 1Y window, per asset. USD is the 1.0 stablecoin anchor."""
    start = as_of - timedelta(days=365)
    window = prices.filter((pl.col("date") >= start) & (pl.col("date") <= as_of)).sort(
        ["symbol", "date"]
    )
    if window.is_empty() or lump_df.is_empty():
        return go.Figure()

    principal = float(lump_df["principal_usd"][0])
    others = [s for s in sorted(window["symbol"].unique().to_list()) if s != btc_symbol]
    fig = go.Figure()
    for sym in [*others, btc_symbol]:
        s = window.filter(pl.col("symbol") == sym)
        if s.is_empty():
            continue
        base = float(s["close"][0])
        if base == 0:
            continue
        values = (s["close"] / base * principal).to_list()
        is_btc = sym == btc_symbol
        hover_strs = [f"<b>{sym}</b>: ${v:,.2f}" for v in values]
        fig.add_trace(
            go.Scatter(
                x=s["date"].to_list(),
                y=values,
                mode="lines",
                name=sym,
                line={
                    "color": _symbol_colour(sym, btc_symbol, others),
                    "width": 3.2 if is_btc else 1.5,
                },
                opacity=1.0 if is_btc else 0.8,
                customdata=hover_strs,
                hovertemplate="%{customdata}<extra></extra>",
            )
        )
    fig.add_hline(
        y=principal,
        line_dash="dot",
        line_color=AXIS_LINE,
        annotation_text=f"Principal ${principal:,.0f}",
        annotation_position="top left",
        annotation_font_size=11,
    )
    fig.update_yaxes(title="Portfolio value (USD)", tickprefix="$", tickformat=",.0f")
    return _apply_chart_theme(fig, height=440)


def _q2_chart(lump_df: pl.DataFrame, btc_symbol: str) -> go.Figure:
    """Horizontal ranked bar of current USD value — one year on $1k."""
    if lump_df.is_empty():
        return go.Figure()

    sorted_df = lump_df.sort("total_return", descending=False)
    all_syms = sorted_df["symbol"].to_list()
    others = [s for s in all_syms if s != btc_symbol]
    # Per-asset colours — consistent with Q1 lines, the palette legend, and the
    # Q4 scatter. Gains/losses are obvious from the bar length and the principal
    # dashed line; we don't need to duplicate that signal in colour.
    colours = [_symbol_colour(sym, btc_symbol, others) for sym in all_syms]
    hover = [
        f"<b>{sym}</b><br>Return: {pct(r)}<br>PnL: ${pnl:,.0f}<br>Principal: ${p:,.0f}"
        for sym, r, pnl, p in zip(
            sorted_df["symbol"].to_list(),
            sorted_df["total_return"].to_list(),
            sorted_df["pnl_usd"].to_list(),
            sorted_df["principal_usd"].to_list(),
            strict=True,
        )
    ]
    fig = go.Figure(
        data=[
            go.Bar(
                x=sorted_df["current_value_usd"].to_list(),
                y=sorted_df["symbol"].to_list(),
                orientation="h",
                marker_color=colours,
                text=[f"${v:,.0f}" for v in sorted_df["current_value_usd"].to_list()],
                textposition="outside",
                customdata=hover,
                hovertemplate="%{customdata}<extra></extra>",
            )
        ]
    )
    principal = (
        float(lump_df["principal_usd"][0]) if not lump_df["principal_usd"].is_empty() else 1000.0
    )
    fig.add_vline(x=principal, line_dash="dot", line_color=AXIS_LINE)
    fig.add_annotation(
        x=principal,
        y=1.05,
        yref="paper",
        text=f"Principal ${principal:,.0f}",
        showarrow=False,
        font={"size": 11, "color": NEUTRAL},
    )
    fig.update_xaxes(title="Current value (USD)", tickprefix="$", tickformat=",.0f")
    fig = _apply_chart_theme(fig, height=360)
    fig.update_layout(hovermode="closest")  # anchor tooltip to bar, not cursor edge
    return fig


def _q2_story(lump_df: pl.DataFrame, btc_symbol: str) -> str:
    if lump_df.is_empty():
        return "<em>No lump-sum data available.</em>"
    sorted_df = lump_df.sort("total_return", descending=True)
    top = sorted_df.row(0, named=True)
    bottom = sorted_df.row(-1, named=True)
    btc_row = lump_df.filter(pl.col("symbol") == btc_symbol)
    btc_line = ""
    if not btc_row.is_empty():
        r = btc_row.row(0, named=True)
        btc_line = (
            f" {btc_symbol} ended at <strong>${r['current_value_usd']:,.0f}</strong> "
            f"({pct(r['total_return'])})."
        )
    return (
        f"A $1,000 ticket one year ago grew best in <strong>{top['symbol']}</strong>, "
        f"worth <strong>${top['current_value_usd']:,.0f}</strong> today "
        f"({pct(top['total_return'])}). The weakest was "
        f"<strong>{bottom['symbol']}</strong> at "
        f"<strong>${bottom['current_value_usd']:,.0f}</strong> ({pct(bottom['total_return'])})."
        f"{btc_line} The upper line chart shows each asset's portfolio value through the "
        "year — the dotted horizontal line at $1,000 is the principal, so any curve ending "
        "above it made money. A USD-pegged stablecoin such as USDT or USDC would trace the "
        "dotted line almost exactly: we model that anchor with the <code>USD</code> series "
        "at a constant $1.00, so the USD line <em>is</em> the stablecoin benchmark."
    )


# ---------- Q3 ----------


def _q3_timeseries(prices: pl.DataFrame, analysis: AnalysisConfig, as_of: date) -> go.Figure:
    """Portfolio value over time: DCA (step buys) vs lump (all-in at day 1)."""
    start = as_of - timedelta(days=365)
    dca_res = dca(
        prices,
        symbol=analysis.dca.btc_symbol,
        monthly_amount_usd=analysis.dca.monthly_amount_usd,
        months=analysis.dca.months,
        start=start,
        as_of=as_of,
        buy_day_of_month=analysis.dca.buy_day_of_month,
    )
    lump_principal = analysis.dca.monthly_amount_usd * analysis.dca.months
    lump_res = lump_sum(
        prices,
        symbol=analysis.dca.btc_symbol,
        principal_usd=lump_principal,
        start=start,
        end=as_of,
    )
    if dca_res is None or lump_res is None:
        return go.Figure()

    btc = (
        prices.filter(
            (pl.col("symbol") == analysis.dca.btc_symbol)
            & (pl.col("date") >= dca_res.buys[0][0])
            & (pl.col("date") <= as_of)
        )
        .sort("date")
        .select(["date", "close"])
    )
    if btc.is_empty():
        return go.Figure()

    buy_dates = [b[0] for b in dca_res.buys]
    buy_units = [b[2] for b in dca_res.buys]

    dca_values: list[float] = []
    dca_invested: list[float] = []
    for d, close in zip(btc["date"].to_list(), btc["close"].to_list(), strict=True):
        units_to_date = sum(u for bd, u in zip(buy_dates, buy_units, strict=True) if bd <= d)
        invested = sum(analysis.dca.monthly_amount_usd for bd in buy_dates if bd <= d)
        dca_values.append(units_to_date * float(close))
        dca_invested.append(invested)

    lump_units = lump_res.units
    lump_values = [lump_units * float(c) for c in btc["close"].to_list()]

    lump_hover = [f"<b>Lump</b>: ${v:,.2f}" for v in lump_values]
    dca_hover = [f"<b>DCA</b>: ${v:,.2f}" for v in dca_values]
    invested_hover = [f"<b>Cost basis</b>: ${v:,.2f}" for v in dca_invested]

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=btc["date"].to_list(),
            y=lump_values,
            mode="lines",
            name=f"Lump sum ${lump_principal:,.0f} day 1",
            line={"color": BTC_COLOUR, "width": 3},
            customdata=lump_hover,
            hovertemplate="%{customdata}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=btc["date"].to_list(),
            y=dca_values,
            mode="lines",
            name=f"DCA ${analysis.dca.monthly_amount_usd:,.0f}/mo × {analysis.dca.months}",
            line={"color": "#B84A9A", "width": 3},
            customdata=dca_hover,
            hovertemplate="%{customdata}<extra></extra>",
        )
    )
    fig.add_trace(
        go.Scatter(
            x=btc["date"].to_list(),
            y=dca_invested,
            mode="lines",
            name="DCA principal (cost basis)",
            line={"color": NEUTRAL, "width": 1.6, "dash": "dot"},
            customdata=invested_hover,
            hovertemplate="%{customdata}<extra></extra>",
        )
    )
    fig.update_yaxes(title="Portfolio value (USD)", tickprefix="$", tickformat=",.0f")
    return _apply_chart_theme(fig, height=440)


def _q3_story(dca_df: pl.DataFrame, btc_symbol: str) -> str:
    if dca_df.is_empty():
        return "<em>No DCA data available.</em>"
    by_strategy = {row["strategy"]: row for row in dca_df.to_dicts()}
    dca_row = by_strategy.get("dca")
    lump_row = by_strategy.get("lump_sum")
    if dca_row is None or lump_row is None:
        return "<em>Partial DCA result — both strategies required.</em>"

    diff = float(lump_row["current_value_usd"]) - float(dca_row["current_value_usd"])
    winner = "lump sum" if diff > 0 else "DCA"
    return (
        f"On {btc_symbol}, a <strong>lump-sum</strong> of "
        f"${lump_row['principal_usd']:,.0f} on day 1 is worth "
        f"<strong>${lump_row['current_value_usd']:,.0f}</strong> today "
        f"({pct(lump_row['total_return'])}), while the same principal drip-fed monthly "
        f"ended at <strong>${dca_row['current_value_usd']:,.0f}</strong> "
        f"({pct(dca_row['total_return'])}). "
        f"The <strong>{winner}</strong> approach is ahead by "
        f"<strong>${abs(diff):,.0f}</strong>. "
        "The gap between the solid DCA line and its dotted cost-basis shows the "
        "portfolio's running PnL; the gold line is what an all-in buyer has."
    )


# ---------- Q4 ----------


def _q4_risk_return_chart(
    vol_df: pl.DataFrame, lump_df: pl.DataFrame, btc_symbol: str
) -> go.Figure:
    """Risk-return scatter: annualised stdev (Y) vs 1Y total return (X). Ties Q4 to Q1."""
    if vol_df.is_empty() or lump_df.is_empty():
        return go.Figure()

    merged = vol_df.join(lump_df.select(["symbol", "total_return"]), on="symbol", how="inner")
    if merged.is_empty():
        return go.Figure()

    others = [s for s in merged["symbol"].to_list() if s != btc_symbol]
    fig = go.Figure()
    for row in merged.to_dicts():
        sym = row["symbol"]
        is_btc = sym == btc_symbol
        x_pct = float(row["total_return"]) * 100
        y_ann = float(row["daily_return_stdev"]) * (252**0.5) * 100
        hover_str = f"<b>{sym}</b><br>1Y return: {x_pct:+.2f}%<br>Annualised vol: {y_ann:.2f}%"
        fig.add_trace(
            go.Scatter(
                x=[x_pct],
                y=[y_ann],
                mode="markers+text",
                name=sym,
                marker={
                    "size": 18 if is_btc else 14,
                    "color": BTC_COLOUR if is_btc else _symbol_colour(sym, btc_symbol, others),
                    "line": {"color": INK, "width": 1},
                },
                text=[sym],
                textposition="top center",
                textfont={"size": 11},
                customdata=[hover_str],
                hovertemplate="%{customdata}<extra></extra>",
                showlegend=False,
            )
        )
    fig.add_vline(x=0, line_dash="dot", line_color=AXIS_LINE, opacity=0.5)
    fig.update_xaxes(title="1Y total return", ticksuffix="%")
    fig.update_yaxes(title="Annualised volatility", ticksuffix="%")
    fig = _apply_chart_theme(fig, height=420)
    fig.update_layout(hovermode="closest")  # per-point tooltip for the scatter
    return fig


def _q4_rolling_chart(metrics: pl.DataFrame, btc_symbol: str, as_of: date) -> go.Figure:
    """30-day rolling stdev of daily returns per symbol, annualised — reads
    `gold.fact_daily_metrics.rolling_vol_30d` directly. Annualisation
    (× √252 × 100) stays inline because it's a presentation choice."""
    start = as_of - timedelta(days=365)
    windowed = metrics.filter((pl.col("date") >= start) & (pl.col("date") <= as_of)).sort(
        ["symbol", "date"]
    )
    if windowed.is_empty():
        return go.Figure()

    others = [s for s in sorted(windowed["symbol"].unique().to_list()) if s != btc_symbol]
    fig = go.Figure()
    for sym in [*others, btc_symbol]:
        s = windowed.filter(pl.col("symbol") == sym).drop_nulls("rolling_vol_30d")
        if s.is_empty():
            continue
        is_btc = sym == btc_symbol
        annualised = (s["rolling_vol_30d"] * (252**0.5) * 100).to_list()
        hover_strs = [f"<b>{sym}</b>: {v:.2f}%" for v in annualised]
        fig.add_trace(
            go.Scatter(
                x=s["date"].to_list(),
                y=annualised,
                mode="lines",
                name=sym,
                line={
                    "color": _symbol_colour(sym, btc_symbol, others),
                    "width": 3 if is_btc else 1.5,
                },
                opacity=1.0 if is_btc else 0.8,
                customdata=hover_strs,
                hovertemplate="%{customdata}<extra></extra>",
            )
        )
    fig.update_yaxes(title="Annualised volatility (%)", ticksuffix="%")
    return _apply_chart_theme(fig, height=420)


def _q4_story(vol_df: pl.DataFrame, btc_symbol: str) -> str:
    if vol_df.is_empty():
        return "<em>No volatility data available.</em>"
    sorted_df = vol_df.sort("daily_return_stdev", descending=True)
    top = sorted_df.row(0, named=True)
    bottom = sorted_df.row(-1, named=True)
    btc_row = vol_df.filter(pl.col("symbol") == btc_symbol)
    btc_rank = None
    if not btc_row.is_empty():
        ranking = sorted_df["symbol"].to_list()
        btc_rank = ranking.index(btc_symbol) + 1
    rank_line = (
        f" {btc_symbol} ranks <strong>#{btc_rank}</strong> of "
        f"{sorted_df.height} by daily-return stdev."
        if btc_rank is not None
        else ""
    )
    top_ann = float(top["daily_return_stdev"]) * (252**0.5) * 100
    bottom_ann = float(bottom["daily_return_stdev"]) * (252**0.5) * 100
    return (
        "Volatility is measured the same way as in the Q1 panel: the standard deviation of "
        "daily returns, annualised as <code>stdev × √252</code> so it reads as a yearly "
        f"percentage. <strong>{top['symbol']}</strong> is the most volatile at "
        f"<strong>{top_ann:.1f}%</strong> annualised, <strong>{bottom['symbol']}</strong> "
        f"the calmest at <strong>{bottom_ann:.1f}%</strong>.{rank_line} The risk-return "
        "scatter plots this volatility (Y) against the 1Y total return from Q1 (X), so you "
        "can read each symbol's reward-per-unit-risk at a glance: points in the upper-left "
        "delivered loss with high volatility, lower-right means steady gains."
    )


# ---------- Correlation ----------


def _corr_chart(corr_df: pl.DataFrame) -> go.Figure:
    if corr_df.is_empty():
        return go.Figure()
    symbols = corr_df["symbol"].to_list()
    value_cols = [c for c in corr_df.columns if c != "symbol"]
    z = corr_df.select(value_cols).to_numpy()
    # Canonical Claude Design diverging scale — steel → gold → white →
    # magenta → amethyst. Negative relationships read cool/metallic, positive
    # ones pull toward the 911 pearl.
    corr_scale = [
        [0.00, "#3D4A57"],  # -1.0: deep steel
        [0.25, "#D4B072"],  # -0.5: metallic gold
        [0.50, "#F3F1EC"],  #  0.0: concrete white
        [0.75, "#B84A9A"],  # +0.5: bright amethyst
        [1.00, "#5E2548"],  # +1.0: deep amethyst
    ]
    fig = go.Figure(
        data=go.Heatmap(
            z=z,
            x=value_cols,
            y=symbols,
            colorscale=corr_scale,
            zmin=-1,
            zmax=1,
            text=[[f"{v:.2f}" for v in row] for row in z],
            texttemplate="%{text}",
            textfont={"family": "JetBrains Mono, monospace", "size": 11, "color": INK},
            hovertemplate="%{y} vs %{x}: %{z:.2f}<extra></extra>",
            colorbar={
                "title": {"text": "Pearson r", "font": {"family": "Manrope", "size": 11}},
                "tickfont": {
                    "family": "JetBrains Mono, monospace",
                    "size": 10,
                    "color": TICK_COLOR,
                },
                "outlinecolor": LINE,
                "outlinewidth": 1,
                "thickness": 12,
            },
        )
    )
    fig.update_layout(
        height=460,
        margin={"t": 30, "b": 60, "l": 80, "r": 40},
        plot_bgcolor=PAPER_2,
        paper_bgcolor=PAPER_2,
        font={"family": "Manrope, sans-serif", "size": 12, "color": INK},
    )
    fig.update_xaxes(
        tickfont={"family": "JetBrains Mono, monospace", "size": 10.5, "color": TICK_COLOR},
    )
    fig.update_yaxes(
        autorange="reversed",
        tickfont={"family": "JetBrains Mono, monospace", "size": 10.5, "color": TICK_COLOR},
    )
    return fig


def _corr_story(corr_df: pl.DataFrame, btc_symbol: str) -> str:
    if corr_df.is_empty() or btc_symbol not in corr_df.columns:
        return ""
    btc_row = corr_df.filter(pl.col("symbol") != btc_symbol).select(["symbol", btc_symbol])
    if btc_row.is_empty():
        return ""
    sorted_pairs = sorted(
        ((r["symbol"], float(r[btc_symbol])) for r in btc_row.to_dicts()),
        key=lambda kv: abs(kv[1]),
        reverse=True,
    )
    strongest = sorted_pairs[0]
    weakest = sorted_pairs[-1]
    return (
        f"Bitcoin's Pearson correlation against other assets ranges from "
        f"<strong>{strongest[1]:+.2f}</strong> with <strong>{strongest[0]}</strong> "
        f"(strongest link) down to <strong>{weakest[1]:+.2f}</strong> with "
        f"<strong>{weakest[0]}</strong>. Values near 0 imply independent behaviour; "
        "near ±1 implies the two assets move together (or inversely)."
    )


# ---------- KPIs / hero ----------


def _hero_block(
    *,
    lump_df: pl.DataFrame,
    btc_symbol: str,
    as_of: date,
    generated_at: str,
    n_symbols: int,
    n_trading_days: int,
    n_windows: int,
) -> str:
    """Masthead with eyebrow, title, dek, delta pills, coords, and meta sidebar.

    The delta pills summarise BTC vs the non-BTC basket at a glance —
    these are the numbers a reader wants before looking at any chart.
    """
    btc_return = None
    basket_avg = None
    spread_pts = None
    if not lump_df.is_empty():
        btc_row = lump_df.filter(pl.col("symbol") == btc_symbol)
        if not btc_row.is_empty():
            btc_return = float(btc_row.row(0, named=True)["total_return"])
        others = lump_df.filter(pl.col("symbol") != btc_symbol).drop_nulls("total_return")
        if not others.is_empty():
            mean_val = others["total_return"].mean()
            if isinstance(mean_val, int | float):
                basket_avg = float(mean_val)
        if btc_return is not None and basket_avg is not None:
            spread_pts = (basket_avg - btc_return) * 100

    def _pill(seg_class: str, tri: str, label: str, value: str) -> str:
        return (
            f'<div class="seg {seg_class}"><span class="tri">{tri}</span>'
            f'<span>{label}</span><span class="n">{value}</span></div>'
        )

    pills: list[str] = []
    if btc_return is not None:
        tri = "▼" if btc_return < 0 else "▲"
        sign_class = "neg" if btc_return < 0 else "pos"
        pills.append(_pill(sign_class, tri, btc_symbol, _signed_pct(btc_return)))
    if basket_avg is not None:
        tri = "▲" if basket_avg >= 0 else "▼"
        sign_class = "pos" if basket_avg >= 0 else "neg"
        pills.append(_pill(sign_class, tri, "Basket avg", _signed_pct(basket_avg)))
    if spread_pts is not None:
        pills.append(
            '<div class="seg"><span>Spread</span>'
            f'<span class="n" style="color:var(--orange);">{spread_pts:+.1f} pts</span></div>'
        )
    pills_html = f'<div class="hero-delta">{"".join(pills)}</div>' if pills else ""

    year_chip = f"FY {as_of.year}"

    return f"""
<div class="hero">
  <div class="hero-inner">
    <div>
      <div class="eyebrow">
        <span class="dot"></span>
        <span>Volume 01</span>
        <span class="brk">/</span>
        <span>Markets in review</span>
        <span class="chip">{year_chip}</span>
      </div>
      <h1>
        <span class="l1">Traditional assets</span>
        <span class="l2">versus</span>
        <span class="l3">Bitcoin.</span>
      </h1>
      <p class="dek">A one-year retrospective across <strong>{n_symbols} assets</strong> — equities, currencies, and the benchmark — measured against the reference date below.</p>
      {pills_html}
      <div class="hero-coord">
        <span>{n_symbols} assets</span>
        <span>{n_trading_days} trading days</span>
        <span>{n_windows} lookbacks</span>
      </div>
    </div>
    <div class="hero-meta">
      <div class="m-row"><span class="m-label">Reference</span><span class="m-val">{as_of.isoformat()}</span></div>
      <div class="m-row"><span class="m-label">Generated</span><span class="m-val">{generated_at}</span></div>
      <div class="m-row"><span class="m-label">Sources</span><span class="m-val">Massive · CoinGecko</span></div>
      <div class="m-row"><span class="m-label">Warehouse</span><span class="m-val">DuckDB</span></div>
    </div>
  </div>
</div>
"""


def _kpi_band(
    *,
    lump_df: pl.DataFrame,
    dca_df: pl.DataFrame,
    vol_df: pl.DataFrame,
    btc_symbol: str,
) -> str:
    """3+1 KPI band — outcome proposition group on the left, vol standalone on the right."""
    # Card 1 — BTC 1Y return
    btc_card = ""
    btc_lump_row = lump_df.filter(pl.col("symbol") == btc_symbol)
    if not btc_lump_row.is_empty():
        r = btc_lump_row.row(0, named=True)
        ret = float(r["total_return"])
        pnl = float(r["current_value_usd"])
        neg = ret < 0
        arrow = "▼" if neg else "▲"
        value_class = "neg" if neg else "pos"
        pnl_color = "var(--orange)" if neg else "var(--sage)"
        btc_card = f"""
      <div class="kpi">
        <div class="k-label">{btc_symbol} · 1Y return</div>
        <div class="k-value {value_class}"><span class="arrow">{arrow}</span>{abs(ret) * 100:.2f}<span style="font-size:.55em;">%</span></div>
        <div class="k-sub">$1,000 → <strong style="color:{pnl_color};">${pnl:,.0f}</strong></div>
      </div>"""

    # Card 2 — Best $1K → today
    best_card = ""
    if not lump_df.is_empty():
        best = lump_df.sort("total_return", descending=True).row(0, named=True)
        best_value = float(best["current_value_usd"])
        best_ret = float(best["total_return"])
        best_card = f"""
      <div class="kpi">
        <div class="k-label">Best $1K → today</div>
        <div class="k-value pos" style="color:var(--sage);"><span class="arrow">▲</span>${best_value:,.0f}</div>
        <div class="k-sub"><strong>{best["symbol"]}</strong> · {pct(best_ret)}</div>
      </div>"""

    # Card 3 — DCA vs Lump
    dca_card = ""
    if not dca_df.is_empty():
        row_map = {r["strategy"]: r for r in dca_df.to_dicts()}
        dca_row = row_map.get("dca")
        lump_row = row_map.get("lump_sum")
        if dca_row is not None and lump_row is not None:
            diff = float(lump_row["current_value_usd"]) - float(dca_row["current_value_usd"])
            winner = "Lump sum" if diff > 0 else "DCA"
            loser = "DCA" if diff > 0 else "lump"
            principal = float(lump_row["principal_usd"])
            pct_edge = abs(diff) / principal * 100 if principal else 0
            arrow_color = "var(--sage)"
            dca_card = f"""
      <div class="kpi" style="border-right:none;">
        <div class="k-label">DCA vs Lump ({btc_symbol})</div>
        <div class="k-value pos" style="color:{arrow_color};"><span class="arrow">+</span>${abs(diff):,.0f}</div>
        <div class="k-sub"><strong>{winner}</strong> edged {loser} by {pct_edge:.1f}%</div>
      </div>"""

    # Card 4 - BTC annualised sigma
    vol_card = ""
    if not vol_df.is_empty():
        btc_vol_row = vol_df.filter(pl.col("symbol") == btc_symbol)
        if not btc_vol_row.is_empty():
            btc_daily_sigma = float(btc_vol_row.row(0, named=True)["daily_return_stdev"])
            # rank 1..N by daily stdev (largest = most volatile = #1)
            sorted_syms = vol_df.sort("daily_return_stdev", descending=True)["symbol"].to_list()
            total = len(sorted_syms)
            # Compare to SPY if present, else show just the BTC rank
            spy_row = vol_df.filter(pl.col("symbol") == "SPY")
            compare_line = f"highest of the {total}"
            if not spy_row.is_empty():
                spy_sigma = float(spy_row.row(0, named=True)["daily_return_stdev"])
                if spy_sigma:
                    multiple = btc_daily_sigma / spy_sigma
                    compare_line = (
                        f'<strong style="color:var(--amber);">{multiple:.1f}×</strong> '
                        f"the S&amp;P 500 · highest of the {total}"
                    )
            vol_card = f"""
      <div class="kpi" style="border-right:none;">
        <div class="k-label">{btc_symbol} · daily σ</div>
        <div class="k-value" style="color:var(--amber);">{btc_daily_sigma * 100:.2f}<span style="font-size:.55em;">%</span><span style="font-size:.38em; color:var(--muted); margin-left:4px; letter-spacing:0.12em;">DAILY</span></div>
        <div class="k-sub">{compare_line}</div>
      </div>"""

    group_cards = "".join([c for c in (btc_card, best_card, dca_card) if c])
    if not group_cards and not vol_card:
        return ""

    return f"""
<div class="kpi-band">
  <div class="kpi-group">
    {group_cards}
  </div>
  <div class="kpi-standalone">
    {vol_card}
  </div>
</div>
"""


def _palette_legend(
    prices: pl.DataFrame,
    *,
    btc_symbol: str,
    assets: list[tuple[str, str]],
) -> str:
    """Strip of coloured swatches — single source of truth for symbol→colour.

    Ordered: BTC first, then whatever is present in ``prices`` in the given
    ``assets`` order. Only symbols that actually appear in the warehouse are
    rendered, so a skipped ingest doesn't leave dangling swatches.
    """
    present = set(prices["symbol"].unique().to_list())
    # BTC swatch uses the gradient from the hero accent bar.
    btc_swatch = "background:linear-gradient(90deg,#B84A9A 0%,#5E2548 50%,#3E1530 100%);"
    name_by_symbol = dict(assets)

    ordered: list[tuple[str, str, str]] = []
    if btc_symbol in present:
        ordered.append(
            (
                btc_symbol,
                name_by_symbol.get(btc_symbol, "Bitcoin"),
                btc_swatch,
            )
        )
    for sym, name in assets:
        if sym == btc_symbol or sym not in present:
            continue
        ordered.append((sym, name, f"background:{SYMBOL_COLOUR.get(sym, PALETTE[0])};"))

    if not ordered:
        return ""

    items = "".join(
        f'<div class="legend-item">'
        f'<div class="legend-swatch" style="{swatch}"></div>'
        f'<div class="legend-ticker">{sym}</div>'
        f'<div class="legend-name">{name}</div>'
        f"</div>"
        for sym, name, swatch in ordered
    )
    cols = len(ordered)
    return f'<div class="palette-legend" style="--legend-cols: {cols};">{items}</div>'


def _section_head(number: str, title: str, sub_html: str) -> str:
    return f"""
<div class="section-head">
  <div class="section-num">{number}</div>
  <div class="q-col">
    <h2>{title}</h2>
    <p class="q-sub">{sub_html}</p>
  </div>
</div>
"""


# ---------- Orchestration ----------


def write_html_report(
    *,
    analysis: AnalysisConfig,
    as_of: date,
    row_counts: dict[str, int],
    prices: pl.DataFrame,
    metrics: pl.DataFrame,
    returns_df: pl.DataFrame,
    lump_df: pl.DataFrame,
    dca_df: pl.DataFrame,
    vol_df: pl.DataFrame,
    corr_df: pl.DataFrame,
    assets: list[tuple[str, str]] | None = None,
) -> Path:
    """Render two variants in `DATA_REPORTS/`:

    - ``data_analysis.html`` — loads plotly.js from a CDN; small (~50 KB),
      diff-friendly, reviewer-friendly on GitHub. Requires internet.
    - ``data_analysis_static.html`` — plotly.js inlined (~5 MB); fully
      self-contained, works without network. Fallback for air-gapped review.

    ``assets`` is a list of ``(symbol, display_name)`` tuples used solely for
    the palette legend strip. If omitted, symbols are used as names.

    Returns the path to the CDN variant (the primary artifact).
    """
    DATA_REPORT_DIR.mkdir(parents=True, exist_ok=True)
    del row_counts  # not surfaced in the visualization panel

    btc_symbol = analysis.btc_symbol

    q1_main = _q1_chart(prices, btc_symbol, as_of)
    q1_winners = _q1_winners_chart(returns_df, btc_symbol)
    q1_story = _q1_story(returns_df, btc_symbol)

    q2_growth = _q2_growth_chart(prices, lump_df, btc_symbol, as_of)
    q2_fig = _q2_chart(lump_df, btc_symbol)
    q2_story = _q2_story(lump_df, btc_symbol)

    q3_fig = _q3_timeseries(prices, analysis, as_of)
    q3_story = _q3_story(dca_df, btc_symbol)

    q4_fig = _q4_rolling_chart(metrics, btc_symbol, as_of)
    q4_scatter = _q4_risk_return_chart(vol_df, lump_df, btc_symbol)
    q4_story = _q4_story(vol_df, btc_symbol)

    corr_fig = _corr_chart(corr_df)
    corr_story = _corr_story(corr_df, btc_symbol)

    generated_at = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

    # Hero stats — trading days & lookback-window count pulled from the data.
    btc_prices = prices.filter(
        (pl.col("symbol") == btc_symbol)
        & (pl.col("date") >= (as_of - timedelta(days=365)))
        & (pl.col("date") <= as_of)
    )
    n_trading_days = btc_prices.height or prices.filter(pl.col("symbol") != "USD").height
    n_symbols = prices["symbol"].unique().len()
    n_windows = returns_df["window"].unique().len() if not returns_df.is_empty() else 0

    assets_list = assets or [(s, s) for s in sorted(prices["symbol"].unique().to_list())]

    hero_html = _hero_block(
        lump_df=lump_df,
        btc_symbol=btc_symbol,
        as_of=as_of,
        generated_at=generated_at,
        n_symbols=n_symbols,
        n_trading_days=n_trading_days,
        n_windows=n_windows,
    )
    kpi_html = _kpi_band(
        lump_df=lump_df,
        dca_df=dca_df,
        vol_df=vol_df,
        btc_symbol=btc_symbol,
    )
    legend_html = _palette_legend(prices, btc_symbol=btc_symbol, assets=assets_list)

    def _build_body(plotly_js: str) -> str:
        return f"""
<div class="wrap">
{hero_html}
{kpi_html}
{legend_html}

<section class="section">
  {
            _section_head(
                "Q1",
                "Which asset outperformed Bitcoin across each time window?",
                "Each line is <strong>% change from 1 year ago</strong>; every symbol starts at 0%. The grouped bars read the same story, window-by-window.",
            )
        }
  <div class="panel">
    <div class="chart">{_fig_html(q1_main, plotly_js=plotly_js)}</div>
    <div class="chart">{_fig_html(q1_winners, plotly_js=False)}</div>
    <div class="story"><div class="label">Reading</div>{q1_story}</div>
  </div>
</section>

<section class="section">
  {
            _section_head(
                "Q2",
                "Current worth of $1,000 invested one year ago",
                "Growth of a $1,000 principal deployed on day 1, per asset. <code>USD</code> is held at 1.00 as the stablecoin analogue (<strong>USDT / USDC proxy</strong>).",
            )
        }
  <div class="panel">
    <div class="chart">{_fig_html(q2_growth, plotly_js=False)}</div>
    <div class="chart">{_fig_html(q2_fig, plotly_js=False)}</div>
    <div class="story"><div class="label">Outcome</div>{q2_story}</div>
  </div>
</section>

<section class="section">
  {
            _section_head(
                "Q3",
                f"DCA vs lump sum into {btc_symbol}",
                f"Both strategies buy <strong>{btc_symbol}</strong> at the spot close. Portfolio values evolve with BTC's daily price; the dotted line is DCA's running <strong>cost basis</strong>.",
            )
        }
  <div class="panel">
    <div class="chart">{_fig_html(q3_fig, plotly_js=False)}</div>
    <div class="story"><div class="label">Verdict</div>{q3_story}</div>
  </div>
</section>

<section class="section">
  {
            _section_head(
                "Q4",
                "Fiat vs Bitcoin — which was more volatile?",
                "Top: rolling 30-day annualised stdev of daily returns. Bottom: risk-return scatter — <strong>annualised volatility (Y)</strong> vs <strong>1Y return (X)</strong>.",
            )
        }
  <div class="panel">
    <div class="chart">{_fig_html(q4_fig, plotly_js=False)}</div>
    <div class="chart">{_fig_html(q4_scatter, plotly_js=False)}</div>
    <div class="story"><div class="label">Risk vs reward</div>{q4_story}</div>
  </div>
</section>

<section class="section">
  {
            _section_head(
                "Σ",
                "Correlation matrix",
                "Pearson correlation on inner-joined dates — how the assets <strong>co-move</strong>.",
            )
        }
  <div class="panel">
    <div class="chart">{_fig_html(corr_fig, plotly_js=False)}</div>
    <div class="story"><div class="label">Reading</div>{corr_story}</div>
  </div>
</section>

<footer>
  <div>Market review · Volume 01 · {as_of.year}</div>
  <div>./data/bronze · duckdb · plotly</div>
</footer>
</div>
"""

    def _wrap(body: str) -> str:
        return (
            "<!doctype html><html lang='en'><head><meta charset='utf-8'>"
            "<title>Traditional assets vs Bitcoin — Data Analysis</title>"
            f"{_FONTS}{_CSS}</head><body>{body}{_SORT_HOVER_JS}</body></html>"
        )

    cdn_doc = _wrap(_build_body("cdn"))
    static_doc = _wrap(_build_body("inline"))

    cdn_path = DATA_REPORT_DIR / "data_analysis.html"
    static_path = DATA_REPORT_DIR / "data_analysis_static.html"
    cdn_path.write_text(cdn_doc)
    static_path.write_text(static_doc)
    log.info("html_report.written", path=str(cdn_path), bytes=len(cdn_doc))
    log.info("html_report.written", path=str(static_path), bytes=len(static_doc))
    return cdn_path
