#!/usr/bin/env python
"""Capture live API responses into tests/fixtures/api_responses/ for snapshot testing."""

from __future__ import annotations

import asyncio
import json
from datetime import date, timedelta
from pathlib import Path

from pipeline.config import get_settings, get_yaml_config
from pipeline.ingest.client import provider
from pipeline.observability.logging import configure_logging, get_logger

FIXTURES = Path("tests/fixtures/api_responses")
log = get_logger(__name__)


async def main() -> None:
    configure_logging(level="INFO")
    FIXTURES.mkdir(parents=True, exist_ok=True)
    settings = get_settings()
    yaml_cfg = get_yaml_config()

    today = date.today()
    end = today - timedelta(days=1)
    start = end - timedelta(days=14)  # two weeks is plenty for fixtures

    massive_cfg = yaml_cfg.sources["massive"]
    async with provider(
        base_url=str(massive_cfg.base_url),
        concurrency=massive_cfg.concurrency,
        max_retries=massive_cfg.max_retries,
        timeout_seconds=massive_cfg.timeout_seconds,
    ) as m_client:
        for symbol in ["AAPL", "C:EURUSD"]:
            url = f"/v2/aggs/ticker/{symbol}/range/1/day/{start.isoformat()}/{end.isoformat()}"
            payload = await m_client.get_json(
                url,
                params={"adjusted": "true", "sort": "asc", "limit": 50000},
                headers={"Authorization": f"Bearer {settings.massive_api_key.get_secret_value()}"},
            )
            dest = FIXTURES / f"massive_{symbol.replace(':', '_')}.json"
            dest.write_text(json.dumps(payload, indent=2))
            log.info("fixture.saved", dest=str(dest))

    cg_cfg = yaml_cfg.sources["coingecko"]
    async with provider(
        base_url=str(cg_cfg.base_url),
        concurrency=cg_cfg.concurrency,
        max_retries=cg_cfg.max_retries,
        timeout_seconds=cg_cfg.timeout_seconds,
    ) as cg_client:
        from datetime import datetime, timezone

        payload = await cg_client.get_json(
            "/coins/bitcoin/market_chart/range",
            params={
                "vs_currency": "usd",
                "from": int(datetime(start.year, start.month, start.day, tzinfo=timezone.utc).timestamp()),
                "to":   int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=timezone.utc).timestamp()),
            },
        )
        dest = FIXTURES / "coingecko_bitcoin.json"
        dest.write_text(json.dumps(payload, indent=2))
        log.info("fixture.saved", dest=str(dest))


if __name__ == "__main__":
    asyncio.run(main())
