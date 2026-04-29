"""DCA (dollar-cost averaging) simulation vs lump-sum. Answers Q2 & Q3."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import polars as pl
from dateutil.relativedelta import relativedelta


@dataclass
class LumpSumResult:
    symbol: str
    principal_usd: float
    units: float
    start_date: date
    end_date: date
    start_price: float
    end_price: float
    current_value_usd: float
    pnl_usd: float
    total_return: float


@dataclass
class DCAResult:
    symbol: str
    monthly_amount_usd: float
    months: int
    total_invested_usd: float
    total_units: float
    final_price_usd: float
    current_value_usd: float
    pnl_usd: float
    total_return: float
    buys: list[tuple[date, float, float]]  # (date_executed, price, units)


def _price_on_or_after(
    prices: pl.DataFrame, *, symbol: str, target: date
) -> tuple[date, float] | None:
    """First available (date, close) for symbol on or after `target`. Respects market closures."""
    slc = (
        prices.filter((pl.col("symbol") == symbol) & (pl.col("date") >= target))
        .sort("date")
        .head(1)
    )
    if slc.is_empty():
        return None
    r = slc.row(0, named=True)
    return (r["date"], float(r["close"]))


def _latest_price(prices: pl.DataFrame, *, symbol: str, as_of: date) -> tuple[date, float] | None:
    slc = (
        prices.filter((pl.col("symbol") == symbol) & (pl.col("date") <= as_of)).sort("date").tail(1)
    )
    if slc.is_empty():
        return None
    r = slc.row(0, named=True)
    return (r["date"], float(r["close"]))


def lump_sum(
    prices: pl.DataFrame,
    *,
    symbol: str,
    principal_usd: float,
    start: date,
    end: date,
) -> LumpSumResult | None:
    """Buy `principal_usd` worth at the first available close on/after `start`, value at `end`."""
    entry = _price_on_or_after(prices, symbol=symbol, target=start)
    exit_ = _latest_price(prices, symbol=symbol, as_of=end)
    if entry is None or exit_ is None:
        return None
    entry_date, entry_price = entry
    exit_date, exit_price = exit_
    units = principal_usd / entry_price
    current_value = units * exit_price
    return LumpSumResult(
        symbol=symbol,
        principal_usd=principal_usd,
        units=units,
        start_date=entry_date,
        end_date=exit_date,
        start_price=entry_price,
        end_price=exit_price,
        current_value_usd=current_value,
        pnl_usd=current_value - principal_usd,
        total_return=(current_value - principal_usd) / principal_usd,
    )


def dca(
    prices: pl.DataFrame,
    *,
    symbol: str,
    monthly_amount_usd: float,
    months: int,
    start: date,
    as_of: date,
    buy_day_of_month: int = 1,
) -> DCAResult | None:
    """Buy `monthly_amount_usd` on the `buy_day_of_month` of each month for N months.

    Falls forward to the next available close if the target day has no data.
    """
    buys: list[tuple[date, float, float]] = []
    for i in range(months):
        target = (start + relativedelta(months=i)).replace(day=buy_day_of_month)
        fill = _price_on_or_after(prices, symbol=symbol, target=target)
        if fill is None:
            # ran past available data — stop
            break
        executed_date, price = fill
        units = monthly_amount_usd / price
        buys.append((executed_date, price, units))

    if not buys:
        return None

    last = _latest_price(prices, symbol=symbol, as_of=as_of)
    if last is None:
        return None
    _, final_price = last

    total_invested = monthly_amount_usd * len(buys)
    total_units = sum(u for _, _, u in buys)
    current_value = total_units * final_price
    return DCAResult(
        symbol=symbol,
        monthly_amount_usd=monthly_amount_usd,
        months=len(buys),
        total_invested_usd=total_invested,
        total_units=total_units,
        final_price_usd=final_price,
        current_value_usd=current_value,
        pnl_usd=current_value - total_invested,
        total_return=(current_value - total_invested) / total_invested,
        buys=buys,
    )


def compare_dca_vs_lump_sum(
    prices: pl.DataFrame,
    *,
    symbol: str,
    monthly_amount_usd: float,
    months: int,
    buy_day_of_month: int,
    as_of: date,
) -> pl.DataFrame:
    """Side-by-side DataFrame with totals for DCA vs lump sum of the same principal."""
    lump_principal = monthly_amount_usd * months
    start = as_of - timedelta(days=365)  # the task's "12-month" horizon
    dca_result = dca(
        prices,
        symbol=symbol,
        monthly_amount_usd=monthly_amount_usd,
        months=months,
        start=start,
        as_of=as_of,
        buy_day_of_month=buy_day_of_month,
    )
    ls_result = lump_sum(
        prices, symbol=symbol, principal_usd=lump_principal, start=start, end=as_of
    )

    rows = []
    if dca_result is not None:
        rows.append(
            {
                "strategy": "dca",
                "symbol": dca_result.symbol,
                "principal_usd": dca_result.total_invested_usd,
                "n_buys": dca_result.months,
                "units": dca_result.total_units,
                "current_value_usd": dca_result.current_value_usd,
                "pnl_usd": dca_result.pnl_usd,
                "total_return": dca_result.total_return,
            }
        )
    if ls_result is not None:
        rows.append(
            {
                "strategy": "lump_sum",
                "symbol": ls_result.symbol,
                "principal_usd": ls_result.principal_usd,
                "n_buys": 1,
                "units": ls_result.units,
                "current_value_usd": ls_result.current_value_usd,
                "pnl_usd": ls_result.pnl_usd,
                "total_return": ls_result.total_return,
            }
        )
    return pl.DataFrame(rows)
