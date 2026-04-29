#!/usr/bin/env python
"""Populate meta.nyse_calendar with NYSE trading days. Idempotent.

dbt's dim_date LEFT JOINs this table. If this seed is missing, dim_date falls
back to weekday-based is_trading_day (less accurate — misses holidays).
"""

from __future__ import annotations

from datetime import date, timedelta

import pandas as pd
import pandas_market_calendars as mcal

from pipeline.config import get_settings, get_yaml_config
from pipeline.observability.logging import configure_logging, get_logger
from pipeline.storage import warehouse


def main() -> None:
    configure_logging(level="INFO")
    log = get_logger(__name__)
    settings = get_settings()
    yaml_cfg = get_yaml_config()

    # Cover 2 years back and 1 year forward (bounded by the NYSE calendar library).
    today = date.today()
    start = date(today.year - 2, 1, 1)
    end = today + timedelta(days=365)

    nyse = mcal.get_calendar("NYSE")
    trading = nyse.valid_days(start_date=start.isoformat(), end_date=end.isoformat())
    trading_dates = {d.date() for d in pd.to_datetime(trading).to_pydatetime()}

    rows: list[tuple[str, bool]] = []
    cur = start
    while cur <= end:
        rows.append((cur.isoformat(), cur in trading_dates))
        cur += timedelta(days=1)

    with warehouse.connect(settings) as conn:
        warehouse.bootstrap(conn)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS meta.nyse_calendar (
                date DATE PRIMARY KEY,
                is_trading_day BOOLEAN NOT NULL
            )
            """
        )
        conn.execute("DELETE FROM meta.nyse_calendar")
        conn.executemany(
            "INSERT INTO meta.nyse_calendar (date, is_trading_day) VALUES (?, ?)",
            rows,
        )
    log.info("nyse_calendar.seeded", start=str(start), end=str(end), rows=len(rows))


if __name__ == "__main__":
    main()
