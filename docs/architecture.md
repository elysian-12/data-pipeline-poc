# Architecture

C4 context + container views of the Traditional Assets vs Bitcoin pipeline.
Module-level detail (C4 Code) lives inline with the source in
[src/pipeline/](../src/pipeline/).

---

## C4 Level 1 — System context

```mermaid
flowchart LR
    Analyst([Analyst])
    Operator([Operator])
    Pipeline[<b>Data Platform PoC</b><br/>ingest · transform · analyze]
    Massive[(Massive API<br/>FX · equities · SPY)]
    CoinGecko[(CoinGecko API<br/>BTC close + volume)]

    Analyst -->|reads reports| Pipeline
    Operator -->|make doctor / backfill| Pipeline
    Pipeline -->|HTTPS| Massive
    Pipeline -->|HTTPS| CoinGecko

    classDef person fill:#1f3a5f,stroke:#4a7ab8,color:#fff
    classDef system fill:#2d4a6e,stroke:#5a8bc4,color:#fff
    classDef ext fill:#3a3a3a,stroke:#777,color:#ddd
    class Analyst,Operator person
    class Pipeline system
    class Massive,CoinGecko ext
```

---

## C4 Level 2 — Containers

Linear data flow left-to-right; the Typer CLI orchestrates each stage (dotted
arrows show "triggers", solid arrows show data movement).

```mermaid
flowchart LR
    Operator([Operator])
    CLI[Typer CLI]
    Ingest[Ingest<br/>httpx · pydantic]
    DBT[dbt-core]
    Analysis[Analysis<br/>polars]
    Massive[(Massive)]
    CoinGecko[(CoinGecko)]
    Bronze[(Bronze<br/>Parquet · fsspec)]
    DuckDB[(DuckDB<br/>silver · gold · meta)]
    Outputs[(outputs/<br/>DATA_REPORTS/)]
    Analyst([Analyst])

    Operator -->|make / cron| CLI
    CLI -.triggers.-> Ingest
    CLI -.triggers.-> DBT
    CLI -.triggers.-> Analysis

    Massive -->|HTTPS| Ingest
    CoinGecko -->|HTTPS| Ingest
    Ingest -->|write| Bronze
    Bronze -->|read + MERGE| DuckDB
    DBT --> DuckDB
    DuckDB --> Analysis
    Analysis --> Outputs
    Outputs --> Analyst

    classDef person fill:#1f3a5f,stroke:#4a7ab8,color:#fff
    classDef sys fill:#2d4a6e,stroke:#5a8bc4,color:#fff
    classDef store fill:#3a5a3a,stroke:#6aa86a,color:#fff
    classDef extcls fill:#3a3a3a,stroke:#777,color:#ddd
    class Operator,Analyst person
    class CLI,Ingest,DBT,Analysis sys
    class Bronze,DuckDB,Outputs store
    class Massive,CoinGecko extcls
```

| Container | Tech | Role |
| --- | --- | --- |
| **Typer CLI** | Python 3.12 | Orchestrates ingest → transform → analyze; `make doctor`; backfills |
| **Ingest** | httpx + tenacity + pydantic v2 | Async fetch, per-provider semaphore, 429 backoff |
| **Bronze** | Parquet via fsspec | Hive-partitioned `source/asset_type/ingested_date`; idempotent overwrite |
| **DuckDB** | Embedded OLAP | `silver.stg_prices` (MERGE), `gold.*` (dbt), `meta.*` (run + DQ history) |
| **dbt-core** | dbt-duckdb adapter | Silver views, gold star schema, tests, lineage |
| **Analysis** | polars | Reads gold facts; window aggregates; DCA / lump-sum simulations |
| **Outputs** | Filesystem | `outputs/*.csv + *.parquet`; `DATA_REPORTS/*.md + *.html` |

---

## C4 Level 3 — Component view (ingest container)

```mermaid
flowchart LR
    CLI[cli.py<br/>ingest / backfill-gaps]
    Orch[orchestrator.run_ingest]
    HttpFetch[massive · coingecko<br/>HTTP fetchers]
    Synth[synthetic<br/>USD row generator]
    Client[client.ProviderClient<br/>httpx + tenacity + semaphore]
    Bronze[storage.bronze.write_bronze]
    ReadBronze[storage.bronze.read_bronze]
    Silver[storage.warehouse.merge_into_silver]

    CLI --> Orch
    Orch -.calls.-> HttpFetch
    Orch -.calls.-> Synth
    HttpFetch --> Client
    HttpFetch -->|rows| Orch
    Synth -->|rows| Orch
    Orch -->|rows| Bronze
    Bronze -->|parquet on disk| ReadBronze
    ReadBronze -->|pyarrow table| Silver

    classDef sys fill:#2d4a6e,stroke:#5a8bc4,color:#fff
    classDef io fill:#3a5a3a,stroke:#6aa86a,color:#fff
    class CLI,Orch,HttpFetch,Synth,Client sys
    class Bronze,ReadBronze,Silver io
```

`run_ingest` runs four steps in order: **resolve window** (explicit > incremental > lookback) → **fetch** (HTTP fetchers `gather()` concurrently per provider; `synthetic` short-circuits to hardcoded USD=1.0 rows without any HTTP call) → **write bronze** (partitioned Parquet) → **merge silver** (read each just-written parquet back via `read_bronze`, concat with `promote_options="default"` to reconcile null-typed columns across partitions, then `INSERT … ON CONFLICT DO UPDATE`). Bronze on disk is the source of truth — a partition that fails to write never reaches silver. Run state persists to `meta.pipeline_runs` via `RunContext`; structured events log to `logs/pipeline-*.log` via `get_logger()`.

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
    Ingest->>Bronze: read_bronze (re-read each partition just written)
    Bronze-->>Ingest: pyarrow tables (concat with promote_options="default")
    Ingest->>Silver: merge_into_silver — INSERT ... ON CONFLICT DO UPDATE (last-write-wins)
    CLI->>dbt: dbt seed (if stale), then dbt run, then dbt test
    dbt->>Silver: SELECT * (staging view)
    dbt->>Gold: dim_asset_type, dim_asset, dim_date, fact_daily_price, fact_daily_metrics
    CLI->>Analysis: run_analysis()
    Analysis->>Gold: SELECT fact_daily_price (close) and fact_daily_metrics (returns, vol)
    Note over Analysis: No metric recomputation —<br/>analysis filters / aggregates gold,<br/>then runs DCA / lump-sum simulations.
    Analysis->>Outputs: outputs/*.csv, outputs/*.parquet, DATA_REPORTS/data_analysis.{md,html}
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
