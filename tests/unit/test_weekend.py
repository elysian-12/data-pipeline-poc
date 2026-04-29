"""Massive returns empty results[] for weekends/holidays — treat as success."""

from __future__ import annotations

import json
from datetime import UTC, date, datetime
from unittest.mock import AsyncMock

import pytest

from pipeline.config import AssetConfig
from pipeline.ingest import massive
from pipeline.ingest.client import ProviderClient


@pytest.mark.asyncio
async def test_empty_results_returns_no_rows() -> None:
    client = ProviderClient(base_url="https://x", concurrency=1, max_retries=0, timeout_seconds=1)
    client.get_json = AsyncMock(return_value={"status": "OK", "results": [], "resultsCount": 0})  # type: ignore[method-assign]
    asset = AssetConfig(
        symbol="AAPL",
        name="Apple",
        asset_type="stock",
        source="massive",
        price_completeness="ohlcv",
        base_ccy="USD",
    )
    # Mark the client as "entered" so fetch_symbol can call get_json
    client._client = object()  # type: ignore[assignment]
    import asyncio

    client._sem = asyncio.Semaphore(1)

    rows = await massive.fetch_symbol(
        client,
        asset,
        start=date(2025, 1, 4),  # Saturday
        end=date(2025, 1, 5),  # Sunday
        api_key="test",
        run_id="r1",
        ingested_at=datetime.now(UTC),
    )
    assert rows == []


@pytest.mark.asyncio
async def test_payload_parses_as_typed_model() -> None:
    """Golden: a fake trading-day payload produces one BronzeRow."""
    client = ProviderClient(base_url="https://x", concurrency=1, max_retries=0, timeout_seconds=1)
    client.get_json = AsyncMock(  # type: ignore[method-assign]
        return_value=json.loads(
            json.dumps(
                {
                    "status": "OK",
                    "results": [
                        {
                            "t": int(datetime(2025, 1, 6, 5, tzinfo=UTC).timestamp() * 1000),
                            "o": 100.0,
                            "h": 105.0,
                            "l": 99.0,
                            "c": 104.0,
                            "v": 1_000_000,
                            "vw": 102.5,
                            "n": 42,
                        }
                    ],
                }
            )
        )
    )
    import asyncio

    client._client = object()  # type: ignore[assignment]
    client._sem = asyncio.Semaphore(1)
    asset = AssetConfig(
        symbol="AAPL",
        name="Apple",
        asset_type="stock",
        source="massive",
        price_completeness="ohlcv",
        base_ccy="USD",
    )
    rows = await massive.fetch_symbol(
        client,
        asset,
        start=date(2025, 1, 6),
        end=date(2025, 1, 6),
        api_key="test",
        run_id="r1",
        ingested_at=datetime.now(UTC),
    )
    assert len(rows) == 1
    assert rows[0].close == 104.0
    assert rows[0].date == date(2025, 1, 6)
