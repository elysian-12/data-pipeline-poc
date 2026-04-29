"""Pandera schemas applied at DataFrame stage boundaries."""

from __future__ import annotations

import pandera.pandas as pa
from pandera.typing import DataFrame, Series


class BronzeFrame(pa.DataFrameModel):
    """Schema for the bronze DataFrame before writing Parquet."""

    source: Series[str] = pa.Field(isin=["massive", "coingecko", "synthetic"])
    asset_type: Series[str] = pa.Field(isin=["stock", "fx", "index", "crypto"])
    symbol: Series[str]
    date: Series[pa.DateTime] = pa.Field(nullable=False)
    open: Series[float] = pa.Field(nullable=True, ge=0)
    high: Series[float] = pa.Field(nullable=True, ge=0)
    low: Series[float] = pa.Field(nullable=True, ge=0)
    close: Series[float] = pa.Field(ge=0)
    volume: Series[float] = pa.Field(nullable=True, ge=0)
    vwap: Series[float] = pa.Field(nullable=True, ge=0)
    trade_count: Series[pa.Int64] = pa.Field(nullable=True, ge=0)
    ingested_at: Series[pa.DateTime]
    run_id: Series[str]

    class Config:
        strict = True
        coerce = True


class AnalysisInputFrame(pa.DataFrameModel):
    """Schema for the frame going into analysis (returns, DCA, correlation)."""

    symbol: Series[str]
    asset_type: Series[str]
    date: Series[pa.DateTime] = pa.Field(nullable=False)
    close: Series[float] = pa.Field(ge=0)

    class Config:
        strict = False  # extra columns (open/high/low/volume) are allowed
        coerce = True


__all__ = ["AnalysisInputFrame", "BronzeFrame", "DataFrame"]
