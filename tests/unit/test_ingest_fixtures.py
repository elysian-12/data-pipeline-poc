"""Replay recorded API fixtures through the fetchers.

Catches payload-shape regressions: if Massive or CoinGecko changes a field
name or type, pydantic validation in `MassiveAggResponse` /
`CoinGeckoMarketChart` will fail here before it fails in production.

Refresh fixtures with `uv run python scripts/record_fixtures.py` (needs a
real `MASSIVE_API_KEY`).
"""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
import respx
from httpx import Response

from pipeline.config import AssetConfig
from pipeline.ingest.client import provider
from pipeline.ingest.coingecko import fetch_bitcoin
from pipeline.ingest.massive import fetch_symbol

FIXTURES = Path(__file__).resolve().parents[1] / "fixtures" / "api_responses"


def _load(name: str) -> dict[str, Any]:
    return json.loads((FIXTURES / name).read_text())


@pytest.mark.asyncio
async def test_massive_fetch_symbol_replays_aapl_fixture() -> None:
    payload = _load("massive_AAPL.json")
    asset = AssetConfig(
        symbol="AAPL",
        name="Apple Inc.",
        asset_type="stock",
        source="massive",
        price_completeness="ohlcv",
    )
    ingested_at = datetime(2026, 1, 1, tzinfo=UTC)

    with respx.mock(base_url="https://api.massive.com", assert_all_called=True) as mock:
        mock.get(url__regex=r"/v2/aggs/ticker/AAPL/range/.*").mock(
            return_value=Response(200, json=payload)
        )
        async with provider(
            base_url="https://api.massive.com",
            concurrency=1,
            max_retries=0,
            timeout_seconds=5,
        ) as client:
            rows = await fetch_symbol(
                client,
                asset,
                start=date(2026, 4, 15),
                end=date(2026, 4, 29),
                api_key="test-key",
                run_id="test-run",
                ingested_at=ingested_at,
            )

    assert len(rows) == len(payload["results"])
    bar = payload["results"][0]
    first = rows[0]
    assert first.source == "massive"
    assert first.asset_type == "stock"
    assert first.symbol == "AAPL"
    assert first.open == bar["o"]
    assert first.high == bar["h"]
    assert first.low == bar["l"]
    assert first.close == bar["c"]
    assert first.volume == bar["v"]
    assert first.vwap == bar.get("vw")
    assert first.trade_count == bar.get("n")
    assert first.run_id == "test-run"
    assert first.ingested_at == ingested_at


@pytest.mark.asyncio
async def test_massive_fetch_symbol_replays_fx_fixture() -> None:
    payload = _load("massive_C_EURUSD.json")
    asset = AssetConfig(
        symbol="C:EURUSD",
        name="Euro / US Dollar",
        asset_type="fx",
        source="massive",
        price_completeness="ohlcv",
    )
    with respx.mock(base_url="https://api.massive.com", assert_all_called=True) as mock:
        mock.get(url__regex=r"/v2/aggs/ticker/C:EURUSD/range/.*").mock(
            return_value=Response(200, json=payload)
        )
        async with provider(
            base_url="https://api.massive.com",
            concurrency=1,
            max_retries=0,
            timeout_seconds=5,
        ) as client:
            rows = await fetch_symbol(
                client,
                asset,
                start=date(2026, 4, 15),
                end=date(2026, 4, 29),
                api_key="test-key",
                run_id="test-run",
            )

    assert len(rows) == len(payload["results"])
    assert all(r.symbol == "C:EURUSD" for r in rows)
    assert all(r.asset_type == "fx" for r in rows)
    # FX still has full OHLCV on Massive.
    assert all(r.open is not None and r.high is not None and r.low is not None for r in rows)


@pytest.mark.asyncio
async def test_coingecko_fetch_bitcoin_replays_fixture() -> None:
    payload = _load("coingecko_bitcoin.json")
    asset = AssetConfig(
        symbol="BTC",
        name="Bitcoin",
        asset_type="crypto",
        source="coingecko",
        price_completeness="close_volume_only",
    )
    # Window must contain every timestamp in the fixture (recorder grabs ~14 days
    # ending yesterday, but make this generous so refreshes don't break the test).
    start = date(2025, 1, 1)
    end = date(2027, 1, 1)
    ingested_at = datetime(2026, 1, 1, tzinfo=UTC)

    with respx.mock(base_url="https://api.coingecko.com/api/v3", assert_all_called=True) as mock:
        mock.get(url__regex=r"/coins/bitcoin/market_chart/range").mock(
            return_value=Response(200, json=payload)
        )
        async with provider(
            base_url="https://api.coingecko.com/api/v3",
            concurrency=1,
            max_retries=0,
            timeout_seconds=5,
        ) as client:
            rows = await fetch_bitcoin(
                client,
                asset,
                start=start,
                end=end,
                run_id="test-run",
                ingested_at=ingested_at,
            )

    assert len(rows) > 0
    assert all(r.symbol == "BTC" for r in rows)
    assert all(r.source == "coingecko" for r in rows)
    assert all(r.asset_type == "crypto" for r in rows)
    # CoinGecko free tier: close + volume only.
    assert all(r.open is None and r.high is None and r.low is None for r in rows)
    assert all(r.close > 0 for r in rows)
    # Sorted ascending by date.
    dates = [r.date for r in rows]
    assert dates == sorted(dates)
