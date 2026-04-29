"""Massive API ingest: stocks, FX, index symbols."""

from __future__ import annotations

from datetime import UTC, date, datetime
from urllib.parse import parse_qs, urlparse

from pipeline.config import AssetConfig, SourceConfig
from pipeline.ingest.client import ProviderClient
from pipeline.models import BronzeRow, MassiveAggResponse
from pipeline.observability.logging import get_logger

log = get_logger(__name__)


async def fetch_symbol(
    client: ProviderClient,
    asset: AssetConfig,
    *,
    start: date,
    end: date,
    api_key: str,
    run_id: str,
    ingested_at: datetime | None = None,
) -> list[BronzeRow]:
    """Fetch daily aggregates for one symbol over [start, end]. Handles pagination.

    Empty results[] (weekends/holidays) is treated as success, not failure.
    """
    ingested_at = ingested_at or datetime.now(UTC)
    url = f"/v2/aggs/ticker/{asset.symbol}/range/1/day/{start.isoformat()}/{end.isoformat()}"
    params: dict[str, object] = {"adjusted": "true", "sort": "asc", "limit": 50000}
    headers = {"Authorization": f"Bearer {api_key}"}

    rows: list[BronzeRow] = []
    next_url: str | None = None

    while True:
        if next_url is None:
            payload = await client.get_json(url, params=params, headers=headers)
        else:
            # Re-append the api key — Massive strips it from next_url.
            parsed = urlparse(next_url)
            next_params = {k: v[0] for k, v in parse_qs(parsed.query).items()}
            payload = await client.get_json(parsed.path, params=next_params, headers=headers)

        response = MassiveAggResponse.model_validate(payload)
        for bar in response.results:
            rows.append(
                BronzeRow(
                    source="massive",
                    asset_type=asset.asset_type,
                    symbol=asset.symbol,
                    date=bar.trade_date,
                    open=bar.o,
                    high=bar.h,
                    low=bar.low,
                    close=bar.c,
                    volume=bar.v,
                    vwap=bar.vw,
                    trade_count=bar.n,
                    ingested_at=ingested_at,
                    run_id=run_id,
                )
            )
        next_url = response.next_url
        if not next_url:
            break

    log.info(
        "massive.fetch.ok",
        symbol=asset.symbol,
        start=start.isoformat(),
        end=end.isoformat(),
        rows=len(rows),
    )
    return rows


def build_client_kwargs(source_cfg: SourceConfig) -> dict[str, object]:
    return {
        "base_url": str(source_cfg.base_url),
        "concurrency": source_cfg.concurrency,
        "max_retries": source_cfg.max_retries,
        "timeout_seconds": source_cfg.timeout_seconds,
    }
