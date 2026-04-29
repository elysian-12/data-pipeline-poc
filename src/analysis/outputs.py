"""Orchestrates the analysis layer: reads gold, runs computations, writes outputs."""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import duckdb
import polars as pl

from analysis.correlation import correlation_matrix
from analysis.dca import compare_dca_vs_lump_sum, lump_sum
from analysis.html_report import pct, write_html_report
from analysis.returns import returns_by_window, volatility_summary
from pipeline.config import AnalysisConfig, Settings, YamlConfig
from pipeline.observability.logging import get_logger
from pipeline.observability.perf import timed

log = get_logger(__name__)

OUTPUTS_DIR = Path("outputs")
DATA_REPORT_DIR = Path("DATA_REPORTS")


def _coerce_date(value: object) -> date:
    """Narrow polars `.max()` / similar into a concrete date."""
    if isinstance(value, date):
        return value
    if isinstance(value, str):
        return date.fromisoformat(value)
    raise TypeError(f"Cannot coerce {value!r} (type {type(value).__name__}) to date")


def _load_prices(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Read gold.fact_daily_price + dim_asset to get (symbol, asset_type, date, close)."""
    arrow_tbl = conn.execute(
        """
        SELECT
          a.symbol,
          t.name AS asset_type,
          d.date,
          f.close
        FROM gold.fact_daily_price f
        JOIN gold.dim_asset       a ON a.asset_id      = f.asset_id
        JOIN gold.dim_asset_type  t ON t.asset_type_id = a.asset_type_id
        JOIN gold.dim_date        d ON d.date_id       = f.date_id
        ORDER BY a.symbol, d.date
        """
    ).arrow()
    return pl.from_arrow(arrow_tbl)  # type: ignore[return-value]


def _load_metrics(conn: duckdb.DuckDBPyConnection) -> pl.DataFrame:
    """Read gold.fact_daily_metrics joined to dims — the precomputed answer set
    for Q1 windowed returns, Q4 volatility, and the correlation matrix.

    Keeps the analysis layer a thin consumer of gold instead of recomputing
    daily / rolling / windowed returns from raw prices.
    """
    arrow_tbl = conn.execute(
        """
        SELECT
          a.symbol,
          t.name AS asset_type,
          d.date,
          m.daily_return,
          m.log_return,
          m.rolling_return_7d,
          m.rolling_return_30d,
          m.rolling_return_90d,
          m.rolling_return_180d,
          m.rolling_return_365d,
          m.rolling_vol_30d,
          m.rel_perf_vs_btc
        FROM gold.fact_daily_metrics m
        JOIN gold.dim_asset       a ON a.asset_id      = m.asset_id
        JOIN gold.dim_asset_type  t ON t.asset_type_id = a.asset_type_id
        JOIN gold.dim_date        d ON d.date_id       = m.date_id
        ORDER BY a.symbol, d.date
        """
    ).arrow()
    return pl.from_arrow(arrow_tbl)  # type: ignore[return-value]


def _write_csv_and_parquet(df: pl.DataFrame, name: str) -> None:
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    df.write_csv(OUTPUTS_DIR / f"{name}.csv")
    df.write_parquet(OUTPUTS_DIR / f"{name}.parquet")
    log.info("output.written", name=name, rows=df.height)


def run_analysis(
    settings: Settings, yaml_cfg: YamlConfig, *, as_of: date | None = None
) -> dict[str, int]:
    """Compute all outputs. Returns {output_name: row_count} for the run report."""
    import pipeline.storage.warehouse as warehouse

    analysis: AnalysisConfig = yaml_cfg.analysis
    row_counts: dict[str, int] = {}

    with timed("analyze:load_prices") as meta:
        with warehouse.connect(settings, read_only=True) as conn:
            prices = _load_prices(conn)
            metrics = _load_metrics(conn)
        meta["rows"] = prices.height
        meta["metric_rows"] = metrics.height

    if prices.is_empty():
        log.warning("analysis.empty_prices")
        return {}

    effective_as_of: date = as_of if as_of is not None else _coerce_date(prices["date"].max())

    # Q1 — returns by window. Pulls fixed-N rolling returns from gold; only the
    # YTD branch (variable-length window) still needs the raw close series.
    with timed("analyze:q1_returns") as meta:
        rbw = returns_by_window(
            metrics,
            prices,
            windows_days=analysis.windows_days,
            as_of=effective_as_of,
            btc_symbol=analysis.btc_symbol,
        )
        _write_csv_and_parquet(rbw, "returns_by_window")
        row_counts["returns_by_window"] = rbw.height
        meta["rows"] = rbw.height

    # Q2 — $1k lump-sum one year ago, per asset
    with timed("analyze:q2_lump_sum") as meta:
        rows_ls: list[dict[str, object]] = []
        for sym in prices["symbol"].unique().to_list():
            r = lump_sum(
                prices,
                symbol=sym,
                principal_usd=analysis.lump_sum.amount_usd,
                start=effective_as_of.replace(year=effective_as_of.year - 1),
                end=effective_as_of,
            )
            if r is not None:
                rows_ls.append(
                    {
                        "symbol": r.symbol,
                        "principal_usd": r.principal_usd,
                        "units": r.units,
                        "start_date": r.start_date,
                        "end_date": r.end_date,
                        "start_price": r.start_price,
                        "end_price": r.end_price,
                        "current_value_usd": r.current_value_usd,
                        "pnl_usd": r.pnl_usd,
                        "total_return": r.total_return,
                    }
                )
        ls_df = pl.DataFrame(rows_ls) if rows_ls else pl.DataFrame()
        _write_csv_and_parquet(ls_df, "lump_sum_1k")
        row_counts["lump_sum_1k"] = ls_df.height
        meta["rows"] = ls_df.height

    # Q3 — DCA vs lump sum into BTC
    with timed("analyze:q3_dca") as meta:
        dca_df = compare_dca_vs_lump_sum(
            prices,
            symbol=analysis.dca.btc_symbol,
            monthly_amount_usd=analysis.dca.monthly_amount_usd,
            months=analysis.dca.months,
            buy_day_of_month=analysis.dca.buy_day_of_month,
            as_of=effective_as_of,
        )
        _write_csv_and_parquet(dca_df, "dca_vs_lump")
        row_counts["dca_vs_lump"] = dca_df.height
        meta["rows"] = dca_df.height

    # Q4 — volatility summary, fiat vs BTC (aggregates gold's daily_return).
    with timed("analyze:q4_volatility") as meta:
        vol_df = volatility_summary(metrics, as_of=effective_as_of, lookback_days=365)
        _write_csv_and_parquet(vol_df, "volatility_summary")
        row_counts["volatility_summary"] = vol_df.height
        meta["rows"] = vol_df.height

    # Correlation matrix — pivots gold's daily_return; shape is presentation-layer.
    with timed("analyze:correlation") as meta:
        corr_df = correlation_matrix(metrics, as_of=effective_as_of)
        _write_csv_and_parquet(corr_df, "correlation_matrix")
        row_counts["correlation_matrix"] = corr_df.height
        meta["rows"] = corr_df.height

    with timed("analyze:write_md"):
        _write_data_analysis_md(
            settings=settings,
            analysis=analysis,
            as_of=effective_as_of,
            row_counts=row_counts,
            prices=prices,
            returns_df=rbw,
            lump_df=ls_df,
            dca_df=dca_df,
            vol_df=vol_df,
        )
    with timed("analyze:write_html"):
        write_html_report(
            analysis=analysis,
            as_of=effective_as_of,
            row_counts=row_counts,
            prices=prices,
            metrics=metrics,
            returns_df=rbw,
            lump_df=ls_df,
            dca_df=dca_df,
            vol_df=vol_df,
            corr_df=corr_df,
            assets=[(a.symbol, a.name) for a in yaml_cfg.assets],
        )
    return row_counts


def _write_data_analysis_md(
    *,
    settings: Settings,
    analysis: AnalysisConfig,
    as_of: date,
    row_counts: dict[str, int],
    prices: pl.DataFrame,
    returns_df: pl.DataFrame,
    lump_df: pl.DataFrame,
    dca_df: pl.DataFrame,
    vol_df: pl.DataFrame,
) -> None:
    DATA_REPORT_DIR.mkdir(parents=True, exist_ok=True)

    latest_by_sym = (
        prices.group_by("symbol").agg(pl.col("date").max().alias("latest_date")).sort("symbol")
    )
    latest_table = "\n".join(
        f"| {row['symbol']} | {row['latest_date']} |" for row in latest_by_sym.to_dicts()
    )

    # Q1 — winners per window. Iterate in canonical short→long order so the
    # narrative reads 7d → 1m → 3m → 6m → ytd → 1y instead of whatever
    # order polars happens to return from `.unique()`.
    window_order = ["7d", "1m", "3m", "6m", "ytd", "1y"]
    available = set(returns_df["window"].unique().to_list())
    ordered_windows = [w for w in window_order if w in available]
    # Tail: anything we didn't anticipate (e.g. a new YAML-configured window)
    # appears after the canonical list, in first-seen order.
    for w in returns_df["window"].to_list():
        if w not in window_order and w not in ordered_windows:
            ordered_windows.append(w)

    q1_lines = []
    for window in ordered_windows:
        slc = returns_df.filter(
            (pl.col("window") == window) & (pl.col("symbol") != analysis.btc_symbol)
        ).drop_nulls("return")
        if slc.is_empty():
            continue
        winner_row = slc.sort("return", descending=True).head(1).row(0, named=True)
        btc_ret = slc["btc_return"][0]
        q1_lines.append(
            f"- **{window}**: winner `{winner_row['symbol']}` at {pct(winner_row['return'])} "
            f"(BTC {pct(btc_ret)}) "
            f"{'beat BTC' if winner_row['beats_btc'] else 'did not beat BTC'}"
        )

    # Q2
    q2_lines = []
    for row in lump_df.sort("total_return", descending=True).to_dicts():
        q2_lines.append(
            f"- `{row['symbol']}`: ${row['principal_usd']:.0f} invested {row['start_date']} "
            f"is worth **${row['current_value_usd']:,.2f}** on {row['end_date']} "
            f"({pct(row['total_return'])})"
        )

    # Q3
    q3_lines = []
    for row in dca_df.to_dicts():
        q3_lines.append(
            f"- **{row['strategy']}** (`{row['symbol']}`): "
            f"invested ${row['principal_usd']:,.2f} → worth **${row['current_value_usd']:,.2f}** "
            f"({pct(row['total_return'])})"
        )

    # Q4 — volatility ranking. Raw daily stdev (0.02256) tells the reader
    # nothing on its own; annualise it (daily stdev * sqrt(252), as a
    # percent), rank it against peers, and show a multiplier vs SPY so the
    # comparison is legible at a glance.
    annualisation = 252**0.5  # trading-days-per-year factor
    vol_ranked = vol_df.sort("daily_return_stdev", descending=True).to_dicts()
    spy_row = next((r for r in vol_ranked if r["symbol"] == "SPY"), None)
    spy_stdev = spy_row["daily_return_stdev"] if spy_row else None
    total = len(vol_ranked)

    q4_lines = []
    if vol_ranked:
        btc_row = next((r for r in vol_ranked if r["symbol"] == analysis.btc_symbol), None)
        if btc_row is not None:
            btc_rank = next(
                i for i, r in enumerate(vol_ranked, 1) if r["symbol"] == analysis.btc_symbol
            )
            btc_mult = (
                f", {btc_row['daily_return_stdev'] / spy_stdev:.1f}× SPY"
                if spy_stdev and spy_stdev > 0
                else ""
            )
            q4_lines.append(
                f"**{analysis.btc_symbol} ranks #{btc_rank} of {total}** by daily-return stdev — "
                f"annualised vol {btc_row['daily_return_stdev'] * annualisation * 100:.1f}%"
                f"{btc_mult}. Ranked most → least volatile below."
            )
            q4_lines.append("")

    for i, row in enumerate(vol_ranked, 1):
        stdev = row["daily_return_stdev"]
        ann = stdev * annualisation * 100  # annualised vol as a percent
        mult = (
            f" · {stdev / spy_stdev:.1f}× SPY"
            if spy_stdev and spy_stdev > 0 and row["symbol"] != "SPY"
            else ""
        )
        q4_lines.append(
            f"- #{i} `{row['symbol']}` ({row['asset_type']}): "
            f"daily σ {stdev * 100:.2f}% · annualised {ann:.1f}%{mult} "
            f"({row['n_obs']} obs)"
        )

    report = f"""# Data Analysis — Traditional assets vs Bitcoin

- **Run completed**: {as_of.isoformat()} (reference date)
- **Bronze URI**: `{settings.bronze_uri}`
- **DuckDB**: `{settings.duckdb_path}`

## Output row counts

```json
{json.dumps(row_counts, indent=2)}
```

## Latest date per symbol

| symbol | latest_date |
|---|---|
{latest_table}

## Analysis answers

### Q1 — Which asset outperformed Bitcoin across each time window?

{chr(10).join(q1_lines) or "_no data_"}

### Q2 — Current worth of $1,000 invested one year ago

{chr(10).join(q2_lines) or "_no data_"}

### Q3 — DCA ($100/mo × 12) vs lump sum into Bitcoin

{chr(10).join(q3_lines) or "_no data_"}

### Q4 — Which was more volatile: fiat or Bitcoin?

{chr(10).join(q4_lines) or "_no data_"}

Output files: `outputs/returns_by_window.{{csv,parquet}}`, `outputs/lump_sum_1k.{{csv,parquet}}`, `outputs/dca_vs_lump.{{csv,parquet}}`, `outputs/volatility_summary.{{csv,parquet}}`, `outputs/correlation_matrix.{{csv,parquet}}`.
"""
    (DATA_REPORT_DIR / "data_analysis.md").write_text(report)
    log.info("data_analysis.written", path=str(DATA_REPORT_DIR / "data_analysis.md"))
