-- Pre-dbt DDL for silver and meta tables.
-- Applied once by scripts/bootstrap_warehouse.py. Idempotent: CREATE SCHEMA IF NOT EXISTS.
-- dbt owns the `gold` schema (dim_*, fact_*) and the `staging` schema (stg_prices view).

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS meta;

-- Silver: conformed, deduped daily prices. Populated by Python MERGE from bronze Parquet.
CREATE TABLE IF NOT EXISTS silver.stg_prices (
    source              VARCHAR NOT NULL,
    asset_type          VARCHAR NOT NULL,
    symbol              VARCHAR NOT NULL,
    date                DATE    NOT NULL,
    open                DOUBLE,
    high                DOUBLE,
    low                 DOUBLE,
    close               DOUBLE  NOT NULL,
    volume              DOUBLE,
    vwap                DOUBLE,
    trade_count         INTEGER,
    ingested_at         TIMESTAMP WITH TIME ZONE NOT NULL,
    run_id              VARCHAR NOT NULL,
    PRIMARY KEY (symbol, date)
);

CREATE INDEX IF NOT EXISTS idx_silver_stg_prices_date ON silver.stg_prices(date);

-- Meta: pipeline run registry.
CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
    run_id          VARCHAR PRIMARY KEY,
    started_at      TIMESTAMP WITH TIME ZONE NOT NULL,
    ended_at        TIMESTAMP WITH TIME ZONE,
    status          VARCHAR NOT NULL,   -- running | success | failed
    rows_by_source  VARCHAR,            -- JSON string
    error_payload   VARCHAR,
    git_sha         VARCHAR
);

-- Meta: data quality test history (pandera + singular SQL assertions).
CREATE SEQUENCE IF NOT EXISTS meta.seq_dq_id START 1;
CREATE TABLE IF NOT EXISTS meta.fact_data_quality_runs (
    dq_id           BIGINT PRIMARY KEY DEFAULT nextval('meta.seq_dq_id'),
    run_id          VARCHAR,
    test_name       VARCHAR NOT NULL,
    run_ts          TIMESTAMP WITH TIME ZONE NOT NULL,
    passed          BOOLEAN NOT NULL,
    row_count       INTEGER NOT NULL,
    severity        VARCHAR NOT NULL,   -- error | warning | info
    details         VARCHAR             -- JSON string
);

CREATE INDEX IF NOT EXISTS idx_dq_run_ts ON meta.fact_data_quality_runs(run_ts);

-- Meta: dbt seed content hashes. Lets `transform`/`run` skip `dbt seed`
-- when dbt/seeds/*.csv is unchanged since the last bootstrap. Bootstrap
-- always refreshes this table; later stages compare hashes and only re-seed
-- when a CSV has been edited since bootstrap (covers the edit-seed-then-
-- transform sharp edge without re-seeding on every run).
CREATE TABLE IF NOT EXISTS meta.seed_fingerprints (
    seed_name       VARCHAR PRIMARY KEY,
    sha256          VARCHAR NOT NULL,
    updated_at      TIMESTAMP WITH TIME ZONE NOT NULL
);
