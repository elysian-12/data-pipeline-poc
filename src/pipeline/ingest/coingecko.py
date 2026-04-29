"""CoinGecko /market_chart/range ingest: BTC daily close + volume.

Free tier has no OHL on /market_chart/range (close + volume only). That's why
dim_asset marks BTC as price_completeness='close_volume_only'.
"""

from __future__ import annotations

from datetime import UTC, date, datetime

from pipeline.config import AssetConfig, SourceConfig
from pipeline.ingest.client import ProviderClient
from pipeline.models import BronzeRow, CoinGeckoMarketChart
from pipeline.observability.logging import get_logger

log = get_logger(__name__)


async def fetch_bitcoin(
    client: ProviderClient,
    asset: AssetConfig,
    *,
    start: date,
    end: date,
    run_id: str,
    ingested_at: datetime | None = None,
) -> list[BronzeRow]:
    """One request → the full range. Response is {prices, market_caps, total_volumes}."""
    ingested_at = ingested_at or datetime.now(UTC)
    start_ts = int(datetime(start.year, start.month, start.day, tzinfo=UTC).timestamp())
    # end is inclusive — add a day so the final bucket lands inside the window.
    end_ts = int(datetime(end.year, end.month, end.day, 23, 59, 59, tzinfo=UTC).timestamp())

    payload = await client.get_json(
        "/coins/bitcoin/market_chart/range",
        params={"vs_currency": "usd", "from": start_ts, "to": end_ts},
    )
    chart = CoinGeckoMarketChart.model_validate(payload)

    # Zip prices and volumes by timestamp — CoinGecko aligns them, but be defensive.
    by_ts: dict[int, dict[str, float]] = {}
    for ts_ms, price in chart.prices:
        by_ts.setdefault(int(ts_ms), {})["close"] = price
    for ts_ms, vol in chart.total_volumes:
        by_ts.setdefault(int(ts_ms), {})["volume"] = vol

    rows: list[BronzeRow] = []
    for ts_ms in sorted(by_ts):
        bucket = by_ts[ts_ms]
        d = datetime.fromtimestamp(ts_ms / 1000, tz=UTC).date()
        if d < start or d > end:
            continue
        close = bucket.get("close")
        if close is None:
            continue
        rows.append(
            BronzeRow(
                source="coingecko",
                asset_type=asset.asset_type,
                symbol=asset.symbol,
                date=d,
                open=None,
                high=None,
                low=None,
                close=close,
                volume=bucket.get("volume"),
                vwap=None,
                trade_count=None,
                ingested_at=ingested_at,
                run_id=run_id,
            )
        )

    log.info(
        "coingecko.fetch.ok",
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
