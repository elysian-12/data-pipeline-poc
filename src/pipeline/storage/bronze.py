"""Bronze layer: idempotent Parquet writes via fsspec (local FS or S3)."""

from __future__ import annotations

import os
import uuid
from collections.abc import Iterable
from datetime import date as date_type

import fsspec
import pyarrow as pa
import pyarrow.parquet as pq

from pipeline.config import Settings
from pipeline.models import BronzeRow
from pipeline.observability.logging import get_logger
from pipeline.quality.schemas import BronzeFrame

log = get_logger(__name__)


def _storage_options(settings: Settings) -> dict[str, object]:
    """Build fsspec kwargs for S3. Returns {} for local FS."""
    if not settings.bronze_uri.startswith("s3://"):
        return {}
    opts: dict[str, object] = {}
    if settings.s3_endpoint:
        opts["endpoint_url"] = settings.s3_endpoint
    if settings.aws_access_key_id is not None:
        opts["key"] = settings.aws_access_key_id.get_secret_value()
    if settings.aws_secret_access_key is not None:
        opts["secret"] = settings.aws_secret_access_key.get_secret_value()
    if settings.aws_region:
        opts["client_kwargs"] = {"region_name": settings.aws_region}
    return opts


def bronze_path(
    settings: Settings, *, source: str, asset_type: str, ingested_date: date_type
) -> str:
    """Deterministic Hive-partitioned path: one file per (source, asset_type, ingested_date).

    Re-running the same day overwrites the file rather than appending.
    """
    base = settings.bronze_uri.rstrip("/")
    return (
        f"{base}/source={source}/asset_type={asset_type}"
        f"/ingested_date={ingested_date.isoformat()}/data.parquet"
    )


def write_bronze(rows: Iterable[BronzeRow], settings: Settings, path: str) -> int:
    """Write rows to a single Parquet file, overwriting if present. Returns row count.

    Atomic: write to `<path>.tmp.<uuid>.<pid>` then `fs.mv(tmp, final)`. On POSIX
    a same-directory rename is atomic; on S3 fsspec's `mv` copies + deletes,
    which is atomic from a reader's perspective (either the old object or the
    new one is visible, never a partial stream). A crash mid-write leaves only
    the tmp file; we clean it up on the error path so bronze never holds a
    corrupt `data.parquet`.

    Validates the batch against `BronzeFrame` before writing — pandera enforces
    type, nullability, isin, and non-negative ranges at the DataFrame boundary,
    complementing the per-row pydantic checks on API responses.
    """
    records = [r.model_dump() for r in rows]
    if not records:
        log.info("bronze.write.skipped", path=path, reason="no_rows")
        return 0

    table = pa.Table.from_pylist(records)
    BronzeFrame.validate(table.to_pandas(), lazy=True)
    storage_options = _storage_options(settings)

    fs, resolved = fsspec.core.url_to_fs(path, **storage_options)
    fs.makedirs(resolved.rsplit("/", 1)[0], exist_ok=True)

    tmp_path = f"{resolved}.tmp.{uuid.uuid4().hex}.{os.getpid()}"
    try:
        with fs.open(tmp_path, "wb") as fh:
            pq.write_table(table, fh)
        fs.mv(tmp_path, resolved)
    except BaseException:
        # Best-effort cleanup; swallow cleanup errors so the original raises.
        try:
            if fs.exists(tmp_path):
                fs.rm(tmp_path)
        except Exception:
            log.warning("bronze.write.tmp_cleanup_failed", tmp_path=tmp_path)
        raise

    log.info("bronze.write.ok", path=path, rows=len(records))
    return len(records)


def read_bronze(path: str, settings: Settings) -> pa.Table:
    """Read one bronze file back (used by silver MERGE and tests)."""
    storage_options = _storage_options(settings)
    fs, resolved = fsspec.core.url_to_fs(path, **storage_options)
    with fs.open(resolved, "rb") as fh:
        return pq.read_table(fh)
