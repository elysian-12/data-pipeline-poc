"""Perf log + report rendering smoke tests."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from analysis import perf_report
from pipeline.observability import perf


def test_timed_appends_jsonl(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log_file = tmp_path / "performance.jsonl"
    monkeypatch.setattr(perf, "PERF_LOG", log_file)

    with perf.timed("demo:one", rows=5) as meta:
        meta["extra"] = "ok"

    lines = log_file.read_text().splitlines()
    assert len(lines) == 1
    rec = json.loads(lines[0])
    assert rec["stage"] == "demo:one"
    assert rec["status"] == "ok"
    assert rec["meta"] == {"rows": 5, "extra": "ok"}
    assert rec["duration_s"] >= 0
    # Memory tracking: every record carries process-tree RSS at entry/peak/exit.
    for key in ("rss_mb_start", "rss_mb_peak", "rss_mb_end"):
        assert key in rec
        assert isinstance(rec[key], int | float)
        assert rec[key] > 0  # process always uses some memory
    assert rec["rss_mb_peak"] >= rec["rss_mb_start"]
    assert rec["rss_mb_peak"] >= rec["rss_mb_end"]


def test_timed_records_errors(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log_file = tmp_path / "performance.jsonl"
    monkeypatch.setattr(perf, "PERF_LOG", log_file)

    with pytest.raises(RuntimeError), perf.timed("demo:boom"):
        raise RuntimeError("kaboom")

    rec = json.loads(log_file.read_text().strip())
    assert rec["stage"] == "demo:boom"
    assert rec["status"] == "error"


def test_clear_perf_log(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log_file = tmp_path / "performance.jsonl"
    log_file.write_text('{"stage":"old"}\n')
    monkeypatch.setattr(perf, "PERF_LOG", log_file)

    perf.clear_perf_log()
    assert not log_file.exists()


def test_write_perf_report_renders(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log_file = tmp_path / "performance.jsonl"
    records = [
        {
            "stage": "ingest:fetch:massive",
            "started_at": "2025-02-01T12:00:00+00:00",
            "duration_s": 1.25,
            "status": "ok",
            "pid": 123,
            "rss_mb_start": 120.5,
            "rss_mb_peak": 185.3,
            "rss_mb_end": 142.1,
            "meta": {"rows": 3500, "assets": 7},
        },
        {
            "stage": "ingest:merge_silver",
            "started_at": "2025-02-01T12:00:02+00:00",
            "duration_s": 0.42,
            "status": "ok",
            "pid": 123,
            "rss_mb_start": 142.1,
            "rss_mb_peak": 165.0,
            "rss_mb_end": 150.0,
            "meta": {"rows": 3500},
        },
        {
            "stage": "analyze:q1_returns",
            "started_at": "2025-02-01T12:00:05+00:00",
            "duration_s": 0.11,
            "status": "ok",
            "pid": 124,
            "rss_mb_start": 95.0,
            "rss_mb_peak": 102.5,
            "rss_mb_end": 96.0,
            "meta": {"rows": 40},
        },
    ]
    log_file.write_text("\n".join(json.dumps(r) for r in records))

    out_dir = tmp_path / "DATA_REPORTS"
    monkeypatch.setattr(perf, "PERF_LOG", log_file)
    monkeypatch.setattr(perf_report, "DATA_REPORT_DIR", out_dir)

    out_path = perf_report.write_perf_report()
    assert out_path.exists()
    html = out_path.read_text()
    assert "<!doctype html>" in html.lower()
    assert "Pipeline performance" in html
    assert "ingest:fetch:massive" in html
    assert "ingest:merge_silver" in html
    assert "analyze:q1_returns" in html
    # plotly from CDN, not inlined
    assert "plotly" in html.lower()
    assert len(html) < 500_000
    # New memory columns + KPI render with formatted values.
    assert "peak rss" in html.lower()
    assert "Δ rss" in html
    assert "Peak RSS" in html
    assert "185.3 MB" in html  # max peak from the seeded records
    assert "185 MB" in html  # KPI band rounds to whole MB


def test_write_perf_report_back_compat_no_memory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Old jsonl rows without rss_mb_* fields render with em-dashes, not crashes."""
    log_file = tmp_path / "performance.jsonl"
    records = [
        {
            "stage": "ingest:fetch:massive",
            "started_at": "2025-02-01T12:00:00+00:00",
            "duration_s": 1.25,
            "status": "ok",
            "pid": 123,
            "meta": {"rows": 3500, "assets": 7},
        },
    ]
    log_file.write_text("\n".join(json.dumps(r) for r in records))

    out_dir = tmp_path / "DATA_REPORTS"
    monkeypatch.setattr(perf, "PERF_LOG", log_file)
    monkeypatch.setattr(perf_report, "DATA_REPORT_DIR", out_dir)

    out_path = perf_report.write_perf_report()
    html = out_path.read_text()
    assert "ingest:fetch:massive" in html
    assert "peak rss" in html.lower()
    # No memory data → em-dash placeholder in both table and KPI cell.
    assert "—" in html


def test_write_perf_report_empty(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    log_file = tmp_path / "performance.jsonl"  # does not exist
    out_dir = tmp_path / "DATA_REPORTS"
    monkeypatch.setattr(perf, "PERF_LOG", log_file)
    monkeypatch.setattr(perf_report, "DATA_REPORT_DIR", out_dir)

    out_path = perf_report.write_perf_report()
    assert out_path.exists()
    html = out_path.read_text()
    assert "No performance records" in html
