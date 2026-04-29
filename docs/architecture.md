# Architecture

C4 context + container views of the Traditional Assets vs Bitcoin pipeline.
Module-level detail (C4 Code) lives inline with the source in
[src/pipeline/](../src/pipeline/).

---

## C4 Level 1 — System context

```mermaid
C4Context
    title System context — Traditional Assets vs Bitcoin pipeline

    Person(analyst, "Analyst", "Reads DATA_REPORTS/data_analysis.html/md and the five CSV/Parquet outputs")
    Person(ops, "Operator", "Runs make doctor; investigates DQ failures")

    System(pipeline, "Data Platform PoC", "Ingests, transforms, and analyzes daily prices — compares traditional assets vs Bitcoin")

    System_Ext(massive, "Massive API", "FX, equities, SPY — daily aggregates")
    System_Ext(coingecko, "CoinGecko API", "BTC daily close + volume")

    Rel(pipeline, massive, "Fetches daily OHLCV", "HTTPS / Bearer token")
    Rel(pipeline, coingecko, "Fetches daily close + volume", "HTTPS / public")
    Rel(analyst, pipeline, "Reads DATA_REPORTS/ + outputs/")
    Rel(ops, pipeline, "Runs make doctor / make backfill")
```

---

## C4 Level 2 — Containers

```mermaid
C4Container
    title Container view

    Person(analyst, "Analyst")
    Person(ops, "Operator")

    System_Ext(massive, "Massive API")
    System_Ext(coingecko, "CoinGecko API")

    System_Boundary(pipeline, "Data Platform PoC") {
        Container(cli, "Typer CLI (pipeline)", "Python 3.12", "ingest / transform / analyze / run / backfill-gaps / doctor")
        Container(ingest, "Ingest module", "httpx + tenacity + pydantic v2", "Async fetch; per-provider semaphore; 429 backoff")
        ContainerDb(bronze, "Bronze (Parquet)", "fsspec — local FS or s3://", "Hive-partitioned by source / asset_type / ingested_date; idempotent overwrite per day")
        ContainerDb(warehouse, "DuckDB warehouse", "Embedded OLAP", "silver.stg_prices (MERGE); gold.* (dbt); meta.* (run + DQ history)")
        Container(dbt, "dbt-core + dbt-duckdb", "SQL transformations", "silver views + gold star schema; tests; lineage docs")
        Container(analysis, "Analysis module", "polars", "Reads gold.fact_daily_price + gold.fact_daily_metrics — windows, aggregates, simulations (DCA / lump-sum)")
        ContainerDb(outputs, "Outputs", "Filesystem", "outputs/*.csv + *.parquet; DATA_REPORTS/data_analysis.md + data_analysis.html")
        ContainerDb(logs, "Logs", "Filesystem", "structlog JSON lines")
    }

    Rel(ops, cli, "Invokes via make / cron")
    Rel(cli, ingest, "Orchestrates")
    Rel(ingest, massive, "GET /v2/aggs/ticker/{sym}/range/1/day/{from}/{to}", "HTTPS")
    Rel(ingest, coingecko, "GET /coins/bitcoin/market_chart/range", "HTTPS")
    Rel(ingest, bronze, "Writes partitioned Parquet")
    Rel(ingest, warehouse, "MERGE into silver.stg_prices")
    Rel(cli, dbt, "dbt seed (if stale) + dbt run + dbt test")
    Rel(dbt, warehouse, "Reads silver; writes gold.*")
    Rel(cli, analysis, "Runs after transforms")
    Rel(analysis, warehouse, "Reads gold.fact_daily_price + gold.fact_daily_metrics")
    Rel(analysis, outputs, "Writes CSV / Parquet / report")
    Rel(cli, logs, "JSON lines")
    Rel(analyst, outputs, "Reads")
```

---

## C4 Level 3 — Component view (ingest container)

Solid arrow = calls / data flow. Dashed arrow = depends on / uses.

```mermaid
flowchart TB
    subgraph cli[pipeline/cli.py]
        ingestcmd["ingest / backfill-gaps"]
    end

    subgraph orch[pipeline/ingest/orchestrator.py]
        runingest["run_ingest()"]
        resolve["resolve_range() — explicit &gt; incremental &gt; lookback"]
        fetchall["_fetch_all() — group by source, gather concurrently"]
        writebronze["_write_bronze_partitions() — bucket by source, asset_type"]
    end

    subgraph fetchers[pipeline/ingest/ — per-source fetchers]
        massivefn["massive.fetch_symbol() — paginate next_url; UTC ms→date"]
        cgfn["coingecko.fetch_bitcoin() — /coins/bitcoin/market_chart/range"]
        synthfn["synthetic.fetch_usd() — emit close=1.0 per day"]
    end

    subgraph httpclient[pipeline/ingest/client.py]
        provider["provider() — async ctx mgr yielding ProviderClient"]
        clientcls["ProviderClient — httpx.AsyncClient + asyncio.Semaphore + tenacity"]
    end

    subgraph storage[pipeline/storage/]
        bronzeio["bronze.write_bronze() — pyarrow + fsspec (local FS or s3://)"]
        warehousefn["warehouse.merge_into_silver() — INSERT … ON CONFLICT DO UPDATE"]
    end

    subgraph obs[pipeline/observability/]
        runtracker["RunContext — writes meta.pipeline_runs"]
        logging["get_logger() — structlog JSON"]
    end

    ingestcmd --> runingest
    runingest -->|1. window| resolve
    runingest -->|2. fetch| fetchall
    runingest -->|3. write| writebronze
    runingest -->|4. merge| warehousefn
    runingest -. tracks .-> runtracker
    runingest -. logs .-> logging

    fetchall --> massivefn
    fetchall --> cgfn
    fetchall --> synthfn
    fetchall -. opens .-> provider
    provider --> clientcls
    massivefn -. uses .-> clientcls
    cgfn -. uses .-> clientcls

    writebronze --> bronzeio
```

---

## Data flow — single daily run

```mermaid
sequenceDiagram
    participant Cron
    participant CLI as pipeline CLI
    participant Ingest
    participant Massive
    participant CoinGecko
    participant Bronze as Bronze (Parquet)
    participant Silver as silver.stg_prices
    participant dbt
    participant Gold as gold.* (star schema)
    participant Analysis
    participant Outputs

    Cron->>CLI: make run (02:00 UTC)
    CLI->>Ingest: run_ingest()
    Note over Ingest: resolve_range() picks window:<br/>explicit > incremental > lookback
    par Massive fetch (asyncio.gather over symbols)
        Ingest->>Massive: GET /v2/aggs/ticker/{sym}/range/1/day/{from}/{to}
        Massive-->>Ingest: OHLCV aggregates (paginated next_url)
    and CoinGecko fetch
        Ingest->>CoinGecko: GET /coins/bitcoin/market_chart/range
        CoinGecko-->>Ingest: {prices, total_volumes}
    end
    Ingest->>Bronze: write_bronze (overwrite by ingested_date)
    Ingest->>Silver: merge_into_silver — INSERT … ON CONFLICT DO UPDATE (last-write-wins)
    CLI->>dbt: dbt seed (if stale) + dbt run + dbt test
    dbt->>Silver: SELECT * (staging view)
    dbt->>Gold: dim_asset_type, dim_asset, dim_date, fact_daily_price, fact_daily_metrics
    CLI->>Analysis: run_analysis()
    Analysis->>Gold: SELECT fact_daily_price (close series) + fact_daily_metrics (precomputed returns / vol)
    Note over Analysis: No metric recomputation —<br/>analysis filters / aggregates gold,<br/>then runs DCA + lump-sum simulations.
    Analysis->>Outputs: outputs/*.csv + *.parquet; DATA_REPORTS/data_analysis.md + data_analysis.html
    CLI-->>Cron: exit 0 (or non-zero on failure)
```

---

## Medallion layering

| Layer      | Storage                                                        | Owned by                                                     | Idempotency                                                                                                                           |
| ---------- | -------------------------------------------------------------- | ------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------- |
| **Bronze** | Parquet — `source=X/asset_type=Y/ingested_date=Z/data.parquet` | `pipeline/storage/bronze.py`                                 | Deterministic path — re-runs **overwrite** the same file. `ingested_at` + `run_id` as in-row columns preserve audit.                  |
| **Silver** | `silver.stg_prices` (DuckDB)                                   | `pipeline/storage/warehouse.py` via `INSERT ... ON CONFLICT` | Last-write-wins on `ingested_at`; identical batch replays are no-ops.                                                                 |
| **Gold**   | `gold.dim_*`, `gold.fact_*` (dbt models)                       | dbt                                                          | `fact_daily_price` is incremental on `(asset_id, date_id)`; `fact_daily_metrics` is full-rebuild because window functions require it. |
| **Meta**   | `meta.pipeline_runs`, `meta.fact_data_quality_runs`            | `pipeline/observability/run_tracker.py`                      | Append-only.                                                                                                                          |

---

## Deployment topologies

### Demo (what you run locally)

- Everything in-process. DuckDB is an embedded library (no daemon).
- Bronze is the local filesystem (`./data/bronze/`).
- `make run` serializes `ingest → dbt run → dbt test → analyze`.

### Production (documented, not deployed here)

- Bronze → S3 (flip `BRONZE_URI=s3://…`). fsspec + pyarrow handle the switch.
- Warehouse → ClickHouse (`dbt-clickhouse` profile; MergeTree engine with
  `ORDER BY (asset_id, date_id)`).
- Orchestrator → Airflow. Module entrypoints map 1:1 to tasks.
- Secrets → AWS Secrets Manager / Vault; not `.env`.
- Monitoring → ship `structlog` JSON to Loki / Elasticsearch; alert on DQ
  failures via PagerDuty.

See the ADRs in [decisions/](decisions/) for the rationale behind each choice.
