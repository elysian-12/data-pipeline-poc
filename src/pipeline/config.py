"""Configuration loading: env vars (.env via pydantic-settings) + YAML for static content."""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, Field, HttpUrl, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_SETTINGS_YAML = PROJECT_ROOT / "config" / "settings.yaml"


AssetType = Literal["stock", "fx", "index", "crypto"]
AssetSource = Literal["massive", "coingecko", "synthetic"]
PriceCompleteness = Literal["ohlcv", "close_volume_only"]


class AssetConfig(BaseModel):
    symbol: str
    name: str
    asset_type: AssetType
    source: AssetSource
    price_completeness: PriceCompleteness
    base_ccy: str = "USD"


class SourceConfig(BaseModel):
    base_url: HttpUrl
    rate_limit_per_minute: int = Field(gt=0)
    concurrency: int = Field(gt=0)
    timeout_seconds: float = Field(gt=0)
    max_retries: int = Field(ge=0)


class RunConfig(BaseModel):
    default_lookback_days: int = Field(gt=0)
    reference_timezone: str = "UTC"


class DCAConfig(BaseModel):
    btc_symbol: str
    monthly_amount_usd: float = Field(gt=0)
    months: int = Field(gt=0)
    buy_day_of_month: int = Field(ge=1, le=28)


class LumpSumConfig(BaseModel):
    amount_usd: float = Field(gt=0)


class AnalysisConfig(BaseModel):
    btc_symbol: str
    windows_days: dict[str, int]
    rolling_windows_days: list[int]
    rolling_vol_days: int = Field(gt=0)
    dca: DCAConfig
    lump_sum: LumpSumConfig


class QualityConfig(BaseModel):
    freshness_max_lag_days: int = Field(ge=0)
    stock_min_trading_days_per_year: int = Field(ge=0)
    crypto_min_days_per_year: int = Field(ge=0)
    close_lower_bound: float
    close_upper_bound: float


class YamlConfig(BaseModel):
    """Typed wrapper around settings.yaml."""

    run: RunConfig
    sources: dict[str, SourceConfig]
    assets: list[AssetConfig]
    analysis: AnalysisConfig
    quality: QualityConfig


class Settings(BaseSettings):
    """Environment-driven settings. Secrets and deployment knobs only."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    massive_api_key: SecretStr = SecretStr("")
    bronze_uri: str = "./data/bronze"
    duckdb_path: Path = Path("./data/warehouse.duckdb")

    s3_endpoint: str | None = None
    aws_access_key_id: SecretStr | None = None
    aws_secret_access_key: SecretStr | None = None
    aws_region: str = "us-east-1"

    healthcheck_url: str | None = None
    log_level: str = "INFO"
    git_sha: str | None = None


def load_yaml(path: Path = DEFAULT_SETTINGS_YAML) -> YamlConfig:
    raw = yaml.safe_load(path.read_text())
    return YamlConfig.model_validate(raw)


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


@lru_cache(maxsize=1)
def get_yaml_config() -> YamlConfig:
    return load_yaml()
