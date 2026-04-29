#!/bin/bash
# Cron wrapper for the recurring data pipeline. Handles the things cron's
# minimal environment doesn't: PATH (so uv/dbt resolve), working directory
# (repo root), and log redirection (cron swallows stdout/stderr otherwise).
#
# Runs `make run` (ingest → transform → analyze). Bootstrap is a one-time
# setup step (`make init` on a fresh clone); scheduled invocations only do
# recurring data work.
#
# Installed by `make schedule`; removed by `make unschedule`.
set -eu

export PATH="/opt/homebrew/bin:/usr/local/bin:$HOME/.local/bin:/usr/bin:/bin:$PATH"

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$REPO_ROOT"

mkdir -p logs
LOG="logs/cron-$(date -u +%Y%m%d).log"

{
  echo "=== cron fire: $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
  make run
  echo "=== cron done: $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
} >> "$LOG" 2>&1
