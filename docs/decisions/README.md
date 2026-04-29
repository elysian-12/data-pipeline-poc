# Architecture Decision Records (ADRs)

Short, dated, immutable records of the non-obvious choices behind the pipeline.
Each answers *what was chosen, what was rejected, why, and what it costs to
change*.

| # | Title | Status |
|---|---|---|
| [0001](0001-duckdb-default-clickhouse-scale.md) | DuckDB by default; ClickHouse at scale | Accepted |
| [0002](0002-bronze-parquet-fsspec-s3-ready.md) | Bronze on Parquet via fsspec; S3-ready by URI | Accepted |
| [0003](0003-dbt-for-transforms.md) | dbt-core for all silver→gold transformations | Accepted |
| [0004](0004-cron-over-airflow.md) | Cron + Typer CLI over Airflow for this scale | Accepted |
| [0005](0005-single-fact-nullable-ohlc.md) | Single `fact_daily_price` with nullable OHL | Accepted |

## Format

Each record is short — most fit on one screen. Sections: **Context**,
**Decision**, **Alternatives considered**, **Consequences**, **Cost to reverse**.
