"""Pydantic v2 schemas: API response boundary + the bronze row shape."""

from __future__ import annotations

from datetime import UTC, date, datetime

from pydantic import BaseModel, Field, field_validator

# ---------- Massive aggregates ----------


class MassiveAggBar(BaseModel):
    """One daily bar from /v2/aggs/ticker/.../range/1/day/..."""

    o: float = Field(description="open")
    h: float = Field(description="high")
    low: float = Field(alias="l", description="low")
    c: float = Field(description="close")
    v: float = Field(description="volume")
    vw: float | None = Field(default=None, description="volume-weighted avg price")
    t: int = Field(description="start of aggregate window, UTC ms")
    n: int | None = Field(default=None, description="trade count")

    @property
    def trade_date(self) -> date:
        return datetime.fromtimestamp(self.t / 1000, tz=UTC).date()


class MassiveAggResponse(BaseModel):
    ticker: str | None = None
    status: str
    results: list[MassiveAggBar] = Field(default_factory=list)
    results_count: int | None = Field(default=None, alias="resultsCount")
    next_url: str | None = None


# ---------- CoinGecko market_chart/range ----------


class CoinGeckoMarketChart(BaseModel):
    """Three parallel arrays, each [ts_ms, value]."""

    prices: list[list[float]] = Field(default_factory=list)
    market_caps: list[list[float]] = Field(default_factory=list)
    total_volumes: list[list[float]] = Field(default_factory=list)

    @field_validator("prices", "market_caps", "total_volumes")
    @classmethod
    def _validate_pairs(cls, v: list[list[float]]) -> list[list[float]]:
        for row in v:
            if len(row) != 2:
                raise ValueError(f"expected [ts_ms, value] pair, got {row!r}")
        return v


# ---------- Bronze row — what lands in Parquet ----------


class BronzeRow(BaseModel):
    """Common row shape for all sources. OHL are nullable for close-only sources (BTC)."""

    source: str
    asset_type: str
    symbol: str
    date: date
    open: float | None = None
    high: float | None = None
    low: float | None = None
    close: float
    volume: float | None = None
    vwap: float | None = None
    trade_count: int | None = None
    ingested_at: datetime
    run_id: str
