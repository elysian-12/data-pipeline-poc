"""End-to-end ingest: fetch from APIs → bronze Parquet → silver MERGE."""

from __future__ import annotations

import asyncio
from collections.abc import Iterable
from datetime import UTC, date, datetime, timedelta

import pyarrow as pa

from pipeline.config import AssetConfig, Settings, YamlConfig
from pipeline.ingest import coingecko, massive, synthetic
from pipeline.ingest.client import provider
from pipeline.models import BronzeRow
from pipeline.observability.logging import get_logger
from pipeline.observability.perf import timed
from pipeline.observability.run_tracker import RunContext
from pipeline.storage import bronze as bronze_io
from pipeline.storage import warehouse

log = get_logger(__name__)


def resolve_range(
    conn_ranges: dict[str, date],
    *,
    start: date | None,
    end: date | None,
    default_lookback_days: int,
) -> tuple[date, date]:
    """Pick effective [start, end] for ingest.

    Priority: explicit args > incremental (yesterday after max(silver.date)) > lookback.
    """
    today = datetime.now(UTC).date()
    if start is None:
        if conn_ranges:
            latest = max(conn_ranges.values())
            # latest is a `date`; tolerate DuckDB returning datetime sometimes
            if isinstance(latest, datetime):
                latest = latest.date()
            start = latest + timedelta(days=1)
        else:
            start = today - timedelta(days=default_lookback_days)
    if end is None:
        end = today - timedelta(days=1)  # yesterday UTC
    # If start > end, leave as-is. Caller short-circuits on that signal rather
    # than doing a useless same-day fetch.
    return start, end


async def _fetch_all(
    assets: Iterable[AssetConfig],
    *,
    settings: Settings,
    yaml_cfg: YamlConfig,
    start: date,
    end: date,
    run_id: str,
) -> dict[str, list[BronzeRow]]:
    """Group assets by source and run each source's fetch concurrently under its own client."""
    ingested_at = datetime.now(UTC)
    massive_assets = [a for a in assets if a.source == "massive"]
    cg_assets = [a for a in assets if a.source == "coingecko"]
    synth_assets = [a for a in assets if a.source == "synthetic"]

    async def _massive_all() -> list[BronzeRow]:
        if not massive_assets:
            return []
        cfg = yaml_cfg.sources["massive"]
        with timed("ingest:fetch:massive", assets=len(massive_assets)) as meta:
            async with provider(
                **massive.build_client_kwargs(cfg),  # type: ignore[arg-type]
            ) as client:
                tasks = [
                    massive.fetch_symbol(
                        client,
                        a,
                        start=start,
                        end=end,
                        api_key=settings.massive_api_key.get_secret_value(),
                        run_id=run_id,
                        ingested_at=ingested_at,
                    )
                    for a in massive_assets
                ]
                batches = await asyncio.gather(*tasks)
                out = [row for batch in batches for row in batch]
            meta["rows"] = len(out)
            return out

    async def _coingecko_all() -> list[BronzeRow]:
        if not cg_assets:
            return []
        cfg = yaml_cfg.sources["coingecko"]
        with timed("ingest:fetch:coingecko", assets=len(cg_assets)) as meta:
            async with provider(
                **coingecko.build_client_kwargs(cfg),  # type: ignore[arg-type]
            ) as client:
                tasks = [
                    coingecko.fetch_bitcoin(
                        client, a, start=start, end=end, run_id=run_id, ingested_at=ingested_at
                    )
                    for a in cg_assets
                ]
                batches = await asyncio.gather(*tasks)
                out = [row for batch in batches for row in batch]
            meta["rows"] = len(out)
            return out

    massive_rows, cg_rows = await asyncio.gather(_massive_all(), _coingecko_all())
    synth_rows: list[BronzeRow] = []
    for a in synth_assets:
        if a.symbol == "USD":
            synth_rows.extend(
                synthetic.fetch_usd(start=start, end=end, run_id=run_id, ingested_at=ingested_at)
            )

    return {
        "massive": massive_rows,
        "coingecko": cg_rows,
        "synthetic": synth_rows,
    }


def _partition_key(row: BronzeRow) -> tuple[str, str]:
    return (row.source, row.asset_type)


def _write_bronze_partitions(
    rows: Iterable[BronzeRow], *, settings: Settings, ingested_date: date
) -> dict[tuple[str, str], str]:
    """Group rows by (source, asset_type) and write one file per partition."""
    buckets: dict[tuple[str, str], list[BronzeRow]] = {}
    for row in rows:
        buckets.setdefault(_partition_key(row), []).append(row)

    paths: dict[tuple[str, str], str] = {}
    for (source, asset_type), batch in buckets.items():
        path = bronze_io.bronze_path(
            settings, source=source, asset_type=asset_type, ingested_date=ingested_date
        )
        bronze_io.write_bronze(batch, settings, path)
        paths[(source, asset_type)] = path
    return paths


def run_ingest(
    *,
    settings: Settings,
    yaml_cfg: YamlConfig,
    start: date | None,
    end: date | None,
    run_ctx: RunContext,
) -> tuple[date, date, int]:
    """Full ingest pipeline: API → bronze → silver MERGE. Returns (start, end, silver_rows)."""
    with warehouse.connect(settings) as conn:
        warehouse.bootstrap(conn)
        silver_maxes = warehouse.latest_date_per_symbol(conn)

    effective_start, effective_end = resolve_range(
        silver_maxes,
        start=start,
        end=end,
        default_lookback_days=yaml_cfg.run.default_lookback_days,
    )

    # Up-to-date: silver already has yesterday's data and no explicit range was
    # requested. Skip the API round-trip entirely — a cron missing a day still
    # self-heals next run because `resolve_range` walks from max(silver.date)+1.
    if effective_start > effective_end and start is None and end is None:
        log.info("ingest.up_to_date", start=str(effective_start), end=str(effective_end))
        return effective_start, effective_end, 0

    log.info(
        "ingest.range",
        start=effective_start.isoformat(),
        end=effective_end.isoformat(),
        run_id=run_ctx.run_id,
    )

    fetched = asyncio.run(
        _fetch_all(
            yaml_cfg.assets,
            settings=settings,
            yaml_cfg=yaml_cfg,
            start=effective_start,
            end=effective_end,
            run_id=run_ctx.run_id,
        )
    )
    all_rows: list[BronzeRow] = []
    for source, rows in fetched.items():
        run_ctx.add_rows(source, len(rows))
        all_rows.extend(rows)

    ingested_date = datetime.now(UTC).date()
    with timed("ingest:write_bronze", rows=len(all_rows)):
        _write_bronze_partitions(all_rows, settings=settings, ingested_date=ingested_date)

    # Silver MERGE
    if not all_rows:
        return effective_start, effective_end, 0

    table = pa.Table.from_pylist([r.model_dump() for r in all_rows])
    with (
        timed("ingest:merge_silver", rows=len(all_rows)),
        warehouse.connect(settings) as conn,
    ):
        inserted = warehouse.merge_into_silver(conn, table)
    return effective_start, effective_end, inserted
