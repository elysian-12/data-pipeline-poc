"""UTC boundary tests: ms→date normalization and DST-neutrality."""

from __future__ import annotations

from datetime import UTC, date, datetime

from pipeline.models import MassiveAggBar


def test_ms_to_date_utc_boundary() -> None:
    # 2025-03-09 07:00:00 UTC — inside "2025-03-09" regardless of US DST transition (which lags UTC).
    ts_ms = int(datetime(2025, 3, 9, 7, 0, 0, tzinfo=UTC).timestamp() * 1000)
    bar = MassiveAggBar(o=1.0, h=2.0, l=0.5, c=1.5, v=1.0, t=ts_ms)
    assert bar.trade_date == date(2025, 3, 9)


def test_ms_to_date_midnight_utc() -> None:
    ts_ms = int(datetime(2025, 1, 1, 0, 0, 0, tzinfo=UTC).timestamp() * 1000)
    bar = MassiveAggBar(o=1, h=1, l=1, c=1, v=1, t=ts_ms)
    assert bar.trade_date == date(2025, 1, 1)


def test_alias_low_from_l() -> None:
    bar = MassiveAggBar(o=1, h=2, l=0.5, c=1.5, v=1, t=0)
    assert bar.low == 0.5
