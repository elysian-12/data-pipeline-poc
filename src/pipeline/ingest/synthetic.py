"""Synthetic source: USD base-currency seed (close=1.0 per day).

The analysis layer treats USD as the numéraire; every FX/crypto price is
quoted per-USD. Rather than `UNION ALL SELECT 1.0` inside gold SQL, we
land USD as a real `source='synthetic'` row in silver so the star schema
joins remain uniform. See [README.md](../../../README.md) §"USD is the
numéraire".
"""

from __future__ import annotations

from datetime import date, datetime, timedelta

from pipeline.models import BronzeRow


def fetch_usd(
    *,
    start: date,
    end: date,
    run_id: str,
    ingested_at: datetime,
) -> list[BronzeRow]:
    """Emit one BronzeRow per day over [start, end] with close=1.0."""
    rows: list[BronzeRow] = []
    cur = start
    while cur <= end:
        rows.append(
            BronzeRow(
                source="synthetic",
                asset_type="fx",
                symbol="USD",
                date=cur,
                open=1.0,
                high=1.0,
                low=1.0,
                close=1.0,
                volume=None,
                vwap=None,
                trade_count=None,
                ingested_at=ingested_at,
                run_id=run_id,
            )
        )
        cur += timedelta(days=1)
    return rows
