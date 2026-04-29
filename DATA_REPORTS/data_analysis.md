# Data Analysis — Traditional assets vs Bitcoin

- **Run completed**: 2026-04-24 (reference date)
- **Bronze URI**: `./data/bronze`
- **DuckDB**: `data/warehouse.duckdb`

## Output row counts

```json
{
  "returns_by_window": 48,
  "lump_sum_1k": 8,
  "dca_vs_lump": 2,
  "volatility_summary": 8,
  "correlation_matrix": 7
}
```

## Latest date per symbol

| symbol | latest_date |
|---|---|
| AAPL | 2026-04-24 |
| BTC | 2026-04-24 |
| C:EURUSD | 2026-04-24 |
| C:GBPUSD | 2026-04-24 |
| GOOGL | 2026-04-24 |
| MSFT | 2026-04-24 |
| SPY | 2026-04-24 |
| USD | 2026-04-24 |

## Analysis answers

### Q1 — Which asset outperformed Bitcoin across each time window?

- **7d**: winner `GOOGL` at 0.80% (BTC 4.14%) did not beat BTC
- **1m**: winner `GOOGL` at 18.38% (BTC 10.97%) beat BTC
- **3m**: winner `AAPL` at 6.13% (BTC -12.47%) beat BTC
- **6m**: winner `GOOGL` at 27.90% (BTC -29.89%) beat BTC
- **ytd**: winner `GOOGL` at 9.28% (BTC -10.58%) beat BTC
- **1y**: winner `GOOGL` at 112.65% (BTC -16.63%) beat BTC

### Q2 — Current worth of $1,000 invested one year ago

- `GOOGL`: $1000 invested 2025-04-25 is worth **$2,126.45** on 2026-04-24 (112.65%)
- `SPY`: $1000 invested 2025-04-25 is worth **$1,296.56** on 2026-04-24 (29.66%)
- `AAPL`: $1000 invested 2025-04-25 is worth **$1,295.20** on 2026-04-24 (29.52%)
- `MSFT`: $1000 invested 2025-04-25 is worth **$1,083.63** on 2026-04-24 (8.36%)
- `C:EURUSD`: $1000 invested 2025-04-25 is worth **$1,031.42** on 2026-04-24 (3.14%)
- `C:GBPUSD`: $1000 invested 2025-04-25 is worth **$1,016.83** on 2026-04-24 (1.68%)
- `USD`: $1000 invested 2025-04-25 is worth **$1,000.00** on 2026-04-24 (0.00%)
- `BTC`: $1000 invested 2025-04-25 is worth **$833.69** on 2026-04-24 (-16.63%)

### Q3 — DCA ($100/mo × 12) vs lump sum into Bitcoin

- **dca** (`BTC`): invested $1,200.00 → worth **$986.40** (-17.80%)
- **lump_sum** (`BTC`): invested $1,200.00 → worth **$1,000.43** (-16.63%)

### Q4 — Which was more volatile: fiat or Bitcoin?

**BTC ranks #1 of 8** by daily-return stdev — annualised vol 35.4%, 2.8× SPY. Ranked most → least volatile below.

- #1 `BTC` (crypto): daily σ 2.23% · annualised 35.4% · 2.8× SPY (364 obs)
- #2 `GOOGL` (stock): daily σ 1.79% · annualised 28.4% · 2.3× SPY (250 obs)
- #3 `MSFT` (stock): daily σ 1.55% · annualised 24.5% · 2.0× SPY (250 obs)
- #4 `AAPL` (stock): daily σ 1.47% · annualised 23.4% · 1.9× SPY (250 obs)
- #5 `SPY` (index): daily σ 0.79% · annualised 12.5% (250 obs)
- #6 `C:GBPUSD` (fx): daily σ 0.40% · annualised 6.4% · 0.5× SPY (312 obs)
- #7 `C:EURUSD` (fx): daily σ 0.40% · annualised 6.3% · 0.5× SPY (312 obs)
- #8 `USD` (fx): daily σ 0.00% · annualised 0.0% · 0.0× SPY (364 obs)

Output files: `outputs/returns_by_window.{csv,parquet}`, `outputs/lump_sum_1k.{csv,parquet}`, `outputs/dca_vs_lump.{csv,parquet}`, `outputs/volatility_summary.{csv,parquet}`, `outputs/correlation_matrix.{csv,parquet}`.
