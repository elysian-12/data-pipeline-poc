# Data Analysis — Traditional assets vs Bitcoin

- **Run completed**: 2026-04-28 (reference date)
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
| AAPL | 2026-04-28 |
| BTC | 2026-04-28 |
| C:EURUSD | 2026-04-28 |
| C:GBPUSD | 2026-04-28 |
| GOOGL | 2026-04-28 |
| MSFT | 2026-04-28 |
| SPY | 2026-04-28 |
| USD | 2026-04-28 |

## Analysis answers

### Q1 — Which asset outperformed Bitcoin across each time window?

- **7d**: winner `GOOGL` at 5.26% (BTC 1.96%) beat BTC
- **1m**: winner `GOOGL` at 27.89% (BTC 16.65%) beat BTC
- **3m**: winner `AAPL` at 5.56% (BTC -13.28%) beat BTC
- **6m**: winner `GOOGL` at 24.26% (BTC -29.70%) beat BTC
- **ytd**: winner `GOOGL` at 10.99% (BTC -11.61%) beat BTC
- **1y**: winner `GOOGL` at 117.78% (BTC -17.53%) beat BTC

### Q2 — Current worth of $1,000 invested one year ago

- `GOOGL`: $1000 invested 2025-04-28 is worth **$2,177.82** on 2026-04-28 (117.78%)
- `SPY`: $1000 invested 2025-04-28 is worth **$1,291.99** on 2026-04-28 (29.20%)
- `AAPL`: $1000 invested 2025-04-28 is worth **$1,288.24** on 2026-04-28 (28.82%)
- `MSFT`: $1000 invested 2025-04-28 is worth **$1,097.38** on 2026-04-28 (9.74%)
- `C:EURUSD`: $1000 invested 2025-04-28 is worth **$1,027.21** on 2026-04-28 (2.72%)
- `C:GBPUSD`: $1000 invested 2025-04-28 is worth **$1,006.65** on 2026-04-28 (0.66%)
- `USD`: $1000 invested 2025-04-28 is worth **$1,000.00** on 2026-04-28 (0.00%)
- `BTC`: $1000 invested 2025-04-28 is worth **$824.67** on 2026-04-28 (-17.53%)

### Q3 — DCA ($100/mo × 12) vs lump sum into Bitcoin

- **dca** (`BTC`): invested $1,200.00 → worth **$975.07** (-18.74%)
- **lump_sum** (`BTC`): invested $1,200.00 → worth **$989.60** (-17.53%)

### Q4 — Which was more volatile: fiat or Bitcoin?

**BTC ranks #1 of 8** by daily-return stdev — annualised vol 35.4%, 2.8× SPY. Ranked most → least volatile below.

- #1 `BTC` (crypto): daily σ 2.23% · annualised 35.4% · 2.8× SPY (365 obs)
- #2 `GOOGL` (stock): daily σ 1.79% · annualised 28.4% · 2.3× SPY (251 obs)
- #3 `MSFT` (stock): daily σ 1.54% · annualised 24.5% · 2.0× SPY (251 obs)
- #4 `AAPL` (stock): daily σ 1.48% · annualised 23.4% · 1.9× SPY (251 obs)
- #5 `SPY` (index): daily σ 0.79% · annualised 12.5% (251 obs)
- #6 `C:GBPUSD` (fx): daily σ 0.40% · annualised 6.3% · 0.5× SPY (313 obs)
- #7 `C:EURUSD` (fx): daily σ 0.39% · annualised 6.2% · 0.5× SPY (313 obs)
- #8 `USD` (fx): daily σ 0.00% · annualised 0.0% · 0.0× SPY (365 obs)

Output files: `outputs/returns_by_window.{csv,parquet}`, `outputs/lump_sum_1k.{csv,parquet}`, `outputs/dca_vs_lump.{csv,parquet}`, `outputs/volatility_summary.{csv,parquet}`, `outputs/correlation_matrix.{csv,parquet}`.
