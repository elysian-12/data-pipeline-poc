# ADR 0004 — Cron + Typer CLI over Airflow for this scale

**Status**: Accepted
**Date**: 2026-04-19

## Context

The pipeline runs once per day, issues ~8 API calls, takes ~2 minutes, and
has no branching dependencies. We need scheduling, retries, failure alerts,
and a history trail.

Candidates: cron + Typer CLI, Airflow, Prefect, Dagster, GitHub Actions
scheduled jobs.

## Decision

**`cron` + a Typer CLI**. The CLI is the orchestration surface; cron is the
scheduler. History lives in `meta.pipeline_runs`. Optional alerting via
healthchecks.io is documented.

```cron
0 2 * * * cd /path/to/data-platform-poc && make run >> logs/cron-$(date +\%Y\%m\%d).log 2>&1
```

## Alternatives considered

| Alternative | Why not |
|---|---|
| **Airflow** | Airflow's overhead (scheduler + webserver + metadb + a worker) is 100× the work the pipeline does. Running it locally needs Docker + an init hour. Documented as the migration path in the README (10-line DAG) — at scale we'd adopt it. |
| **Prefect / Dagster** | Same verdict as Airflow at this scale. Prefect 2 is cheaper to run locally than Airflow, but still more infra than cron. Real advantage emerges at 10+ DAGs with fan-out or cross-DAG dependencies. |
| **GitHub Actions scheduled workflows** | Viable for public / low-sensitivity pipelines, but couples reliability to GitHub's minute-level cron jitter (docs: "not guaranteed to run at exact time") and doesn't help for self-hosted / on-prem deployments. |
| **systemd timers** | Strictly better than cron (sub-second resolution, run history via `journalctl`), but cron is more universally recognized. If we ran on a modern Linux box in prod, we'd prefer systemd timers. |

## Consequences

**Positive**

- Zero infrastructure. `crontab -e` + one line.
- The CLI is also the developer-local workflow — same commands in dev, CI,
  and prod.
- `meta.pipeline_runs` gives structured history without a separate orchestrator DB.
- Healthchecks.io closes the "did cron miss the window?" gap with free-tier alerts.

**Negative**

- No UI for run history (grep + SQL instead — `make doctor` bundles both).
- No backfill fan-out (we parallelize within one invocation via async, not
  across invocations). Not needed at this scale.
- Cron silently swallows output unless `MAILTO` is set. Documented; we log
  JSON to file + healthchecks.io pings on success.

## Cost to reverse

**Low.** The 10-line Airflow DAG stub in the README shows module entrypoints
map 1:1 to tasks:

```python
BashOperator(task_id="ingest",    bash_command="pipeline ingest")
BashOperator(task_id="transform", bash_command="pipeline transform")
BashOperator(task_id="analyze",   bash_command="pipeline analyze")
```

No code changes required in `pipeline/` — the CLI is the contract.
