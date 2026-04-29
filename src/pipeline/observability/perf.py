"""Lightweight per-stage timing → outputs/performance.jsonl.

`make init` invokes four separate Python/dbt processes (bootstrap, ingest,
transform, analyze); `make run` the same minus bootstrap. Shared in-memory
state is off the table either way. Each `timed()` call appends a single JSON
line to `outputs/performance.jsonl`; the final `analyze` step reads the
accumulated file and renders an HTML performance report alongside the analysis
report.
"""

from __future__ import annotations

import json
import os
import threading
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import psutil

PERF_LOG = Path("outputs/performance.jsonl")

_SAMPLE_INTERVAL_S = 0.05


def _bytes_to_mb(b: int) -> float:
    return round(b / (1024 * 1024), 1)


def _tree_rss(parent: psutil.Process) -> int:
    """Sum RSS of `parent` plus all live descendants. Resilient to children
    exiting mid-walk — captures memory of subprocess shell-outs (e.g. dbt)
    that would otherwise be invisible to the parent's own RSS counter.
    """
    total = 0
    try:
        total += int(parent.memory_info().rss)
    except psutil.Error:
        return 0
    for child in parent.children(recursive=True):
        try:
            total += int(child.memory_info().rss)
        except psutil.Error:
            continue
    return total


def clear_perf_log() -> None:
    """Wipe the log at the start of a run. Idempotent."""
    PERF_LOG.parent.mkdir(parents=True, exist_ok=True)
    if PERF_LOG.exists():
        PERF_LOG.unlink()


def _append(record: dict[str, Any]) -> None:
    PERF_LOG.parent.mkdir(parents=True, exist_ok=True)
    with PERF_LOG.open("a") as fh:
        fh.write(json.dumps(record, default=str) + "\n")


@contextmanager
def timed(stage: str, **meta: Any) -> Iterator[dict[str, Any]]:
    """Time a block. Yields a mutable meta dict so the body can add fields
    (e.g. row counts known only after the work finishes).

    Writes one JSONL line on exit, even on exception (so a failed stage still
    shows up in the report). Meta values must be JSON-serialisable.

    Records pick up `PIPELINE_PERF_LABEL` from the env so multiple runs
    (e.g. the nightly run + a backfill smoke test) can co-exist in the
    same perf log and render as separate sections in the report. When the
    env var is unset (normal cron path), we default to `daily-YYYY-MM-DD`
    so each day's run gets its own section without the operator having
    to remember to set a label.
    """
    started = datetime.now(UTC)
    t0 = time.perf_counter()
    mutable: dict[str, Any] = dict(meta)
    status = "ok"

    proc = psutil.Process()
    rss_start = _tree_rss(proc)
    rss_peak = [rss_start]
    stop = threading.Event()

    def _sample() -> None:
        while not stop.wait(_SAMPLE_INTERVAL_S):
            rss_peak[0] = max(rss_peak[0], _tree_rss(proc))

    sampler = threading.Thread(target=_sample, daemon=True)
    sampler.start()

    try:
        yield mutable
    except BaseException:
        status = "error"
        raise
    finally:
        stop.set()
        sampler.join(timeout=0.2)
        rss_end = _tree_rss(proc)
        peak = max(rss_peak[0], rss_end)

        record: dict[str, Any] = {
            "stage": stage,
            "started_at": started.isoformat(),
            "duration_s": round(time.perf_counter() - t0, 6),
            "status": status,
            "pid": os.getpid(),
            "rss_mb_start": _bytes_to_mb(rss_start),
            "rss_mb_peak": _bytes_to_mb(peak),
            "rss_mb_end": _bytes_to_mb(rss_end),
            "meta": mutable,
        }
        record["run_label"] = (
            os.environ.get("PIPELINE_PERF_LABEL") or f"daily-{started.date().isoformat()}"
        )
        _append(record)


def read_perf_log() -> list[dict[str, Any]]:
    """Read accumulated records. Returns [] if the log is missing."""
    if not PERF_LOG.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in PERF_LOG.read_text().splitlines():
        line = line.strip()
        if line:
            records.append(json.loads(line))
    return records
