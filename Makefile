.DEFAULT_GOAL := help
SHELL := /bin/bash
.ONESHELL:

UV       ?= uv
PY       ?= $(UV) run
PIPELINE ?= $(PY) pipeline
DBT      ?= $(PY) dbt
DBT_ARGS ?= --project-dir dbt --profiles-dir dbt

# Pass-through args for backfill: make backfill START=YYYY-MM-DD END=YYYY-MM-DD
START ?=
END   ?=

# Cron wiring for `make schedule`. Override either var at invocation time:
#   make schedule CRON_SCHEDULE='*/5 * * * *'        # every 5 min
#   make schedule CRON_SCRIPT=/tmp/other.sh          # different command
# Matching uses the absolute CRON_SCRIPT path, so overriding it also tells
# `make unschedule` which entry to remove.
CRON_SCHEDULE ?= 0 2 * * *
CRON_SCRIPT   ?= $(CURDIR)/scripts/cron-run.sh

.PHONY: help install bootstrap ingest transform analyze run init backfill backfill-gaps doctor test ci lint typecheck clean docs-dbt schedule unschedule

help: ## List available targets
	@awk 'BEGIN {FS = ":.*##"; printf "Targets:\n"} /^[a-zA-Z_-]+:.*?##/ {printf "  %-18s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

install: ## uv sync (installs runtime + dev deps)
	$(UV) sync --extra dev

bootstrap: ## Create the DuckDB file + silver/meta DDL + dim_date seed
	$(PY) python scripts/bootstrap_warehouse.py
	$(PY) python scripts/seed_dim_date.py

ingest: ## Incremental ingest (or pass START/END to force a range)
	@if [ -n "$(START)" ] && [ -n "$(END)" ]; then \
		$(PIPELINE) ingest --start $(START) --end $(END); \
	else \
		$(PIPELINE) ingest; \
	fi

transform: ## dbt seed + run + test (via pipeline CLI so timings land in perf log)
	$(PIPELINE) transform

analyze: ## Compute returns / DCA / correlation / volatility -> outputs/ tables + DATA_REPORTS/ md+html
	$(PIPELINE) analyze

run: ingest transform analyze ## Daily pipeline: ingest -> dbt -> analyze (what cron runs; requires `make init` first)

init: bootstrap run ## One-time setup on a fresh clone: bootstrap (DDL + seeds) + run. Use `make run` thereafter.

backfill: ## make backfill START=YYYY-MM-DD END=YYYY-MM-DD
	@if [ -z "$(START)" ] || [ -z "$(END)" ]; then \
		echo "usage: make backfill START=YYYY-MM-DD END=YYYY-MM-DD"; exit 2; \
	fi
	$(PIPELINE) ingest --start $(START) --end $(END)
	$(DBT) run  $(DBT_ARGS)
	$(DBT) test $(DBT_ARGS)

backfill-gaps: ## Auto-detect missing dates and fill them
	$(PIPELINE) backfill-gaps
	$(DBT) run  $(DBT_ARGS)
	$(DBT) test $(DBT_ARGS)

doctor: ## Print pipeline health, gaps, DQ failures, and suggested fixes
	$(PIPELINE) doctor

test: ## ruff + mypy + pytest + dbt parse
	$(PY) ruff check src tests
	$(PY) mypy src
	$(PY) pytest
	$(DBT) parse $(DBT_ARGS)

lint: ## ruff check + format check
	$(PY) ruff check src tests
	$(PY) ruff format --check src tests

typecheck: ## mypy --strict
	$(PY) mypy src

ci: install test ## What CI runs

docs-dbt: ## Generate & serve dbt lineage docs (http://localhost:8080)
	$(DBT) docs generate $(DBT_ARGS)
	$(DBT) docs serve    $(DBT_ARGS)

schedule: ## Install cron entry that runs `make run` on CRON_SCHEDULE (default 02:00 UTC daily)
	@chmod +x '$(CRON_SCRIPT)' 2>/dev/null || true
	@set -eu; \
	 { crontab -l 2>/dev/null | grep -Fv '$(CRON_SCRIPT)' || true; \
	   printf '%s %s\n' '$(CRON_SCHEDULE)' '$(CRON_SCRIPT)'; \
	 } | crontab -; \
	 echo "scheduled:"; \
	 crontab -l | grep -F '$(CRON_SCRIPT)' | sed 's/^/  /'

unschedule: ## Remove this repo's scheduled entry
	@set -eu; \
	 { crontab -l 2>/dev/null | grep -Fv '$(CRON_SCRIPT)' || true; } | crontab -; \
	 echo "unscheduled: $(CRON_SCRIPT)"

clean: ## Remove generated artifacts (keeps .env)
	rm -rf data outputs logs dbt/target dbt/logs .mypy_cache .pytest_cache .ruff_cache
