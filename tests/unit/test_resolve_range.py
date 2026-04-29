"""resolve_range: explicit args > incremental > lookback."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta

from pipeline.ingest.orchestrator import resolve_range


def test_explicit_range_wins() -> None:
    start, end = resolve_range(
        {"AAPL": date(2024, 12, 31)},
        start=date(2020, 1, 1),
        end=date(2020, 6, 1),
        default_lookback_days=30,
    )
    assert start == date(2020, 1, 1)
    assert end == date(2020, 6, 1)


def test_incremental_uses_latest_plus_one() -> None:
    latest = datetime.now(UTC).date() - timedelta(days=10)
    start, end = resolve_range(
        {"AAPL": latest, "BTC": latest - timedelta(days=1)},
        start=None,
        end=None,
        default_lookback_days=30,
    )
    assert start == latest + timedelta(days=1)
    assert end == datetime.now(UTC).date() - timedelta(days=1)


def test_cold_start_uses_lookback() -> None:
    today = datetime.now(UTC).date()
    start, end = resolve_range({}, start=None, end=None, default_lookback_days=365)
    assert start == today - timedelta(days=365)
    assert end == today - timedelta(days=1)
