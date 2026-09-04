# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

AI Service Behavior Detector — analyzes hourly service metrics stored in ClickHouse and classifies anomalous patterns into a behavior memory table. Pattern types: `daily`, `weekly`, `drift_up`, `drift_down`, `volume_driven`, `sudden_drop`, `sudden_spike`.

## Setup

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# Configure ClickHouse credentials
cp .env.example .env           # then fill in real host/username/password

# One-time DB setup
python create_tables.py        # Creates ai_service_behavior_memory + ai_detector_staging1
python create_run_log.py       # Creates ai_pattern_run_log table
```

## Running the Pipeline

Scripts run standalone in this order:

```bash
# 1. Refresh baseline views
python baseline_view.py        # 14-day rolling baseline → ai_baseline_view_2
python baseline_stats_30d.py   # 30-day delta stats → ai_baseline_stats_30d

# 2. Generate staging candidates
python stagging.py             # Populates ai_detector_staging1

# 3. Detect and promote patterns
python daily.py
python weekly.py
python drift.py
python volume1.py              # Reads ai_service_features_hourly, same as the other detectors
                                # (was originally meant to read ai_metrics_5m for finer granularity -
                                # never fully wired up; see fetch_5m_data() note below)
python sudden.py

# 4. (Optional) Risk scores
python ai_Probability.py       # CAUTION: drops and recreates ai_probability EMPTY on every run — no scoring logic implemented yet
```

To debug data fetching: `python fetch_data.py` — `main()` exercises all fetch functions and prints row counts.

## Architecture

### Data Flow

```
ai_service_features_hourly (raw hourly metrics)
    ↓
ai_baseline_view_2 (14-day SQL VIEW) + ai_baseline_stats_30d (30-day delta SQL VIEW)
    ↓
ai_detector_staging1 (daily/weekly candidates, breach_ratio ≥ 0.4)
    ↓
daily.py | weekly.py | drift.py | volume1.py | sudden.py
    ↓
ai_service_behavior_memory (promoted patterns with TTL)
    ↓
ai_pattern_run_log (execution log, written by every pattern script)
```

### Key Design Patterns

- **Anchor timestamps**: Each script fixes an `anchor` once (rather than calling `utcnow()` repeatedly) to prevent execution-delay skew:
  - `daily.py`, `weekly.py`, `volume1.py`: today at midnight UTC
  - `drift.py`, `sudden.py`: the latest `ts_hour` actually present in the fetched hourly data (`hourly_df['ts_hour'].max()`), falling back to wall-clock hour boundary only if `hourly_df` is empty. This avoids a real bug: anchoring to wall-clock time meant that if `ai_service_features_hourly` ingestion lags behind real time, the anchor hour's row wouldn't exist yet — `sudden.py` does an exact `ts_hour == anchor` match, so a lagged anchor silently produced 0 patterns for that hour and it was never rechecked (next run's anchor moves forward). `drift.py` is less exposed since it uses a 24h range and only needs 12 hours present, but anchors the same way for consistency.

- **Volume gating**: All detectors skip services where hourly volume < 30% of their baseline median (`VOLUME_THRESHOLD = 0.3`).

- **Baseline classification**: Services are classified from `ai_baseline_view_2` before pattern logic runs:
  - `breach_ratio >= 0.6` → `CHRONIC`
  - `breach_ratio >= 0.3` → `AT_RISK`
  - below → `HEALTHY` (skipped by daily/weekly detectors)

- **TTL-based expiration**: Patterns auto-expire per `TTL_DAYS` in `config.py`. `chronic` patterns never expire.

- **day_of_week normalization**: ClickHouse uses ISO format (1=Mon, 7=Sun); pandas uses 0-indexed (0=Mon, 6=Sun). All scripts convert on ingest: `staging_df['day_of_week'] = (staging_df['day_of_week'] - 1) % 7`.

### Shared Helpers (duplicated across pattern scripts)

Every pattern detection script (`daily.py`, `weekly.py`, `drift.py`, `sudden.py`, `volume1.py`) defines these local helpers:

```python
get_baseline(baseline_df, proj, app, svc, metric)      # → row or None
get_baseline_30d(baseline_30d_df, proj, app, svc, metric)  # → row or None
volume_ok(window_volume, median_volume)                 # → bool
classify_baseline(breach_ratio)                         # → "CHRONIC"|"AT_RISK"|"HEALTHY"
```

These are intentionally inlined (not shared via import) — modify consistently across all scripts when changing threshold logic.

### Configuration (`config.py`)

Single `CONFIG` dict imported by every script. Key thresholds:

| Key | Value | Used By |
|-----|-------|---------|
| `BASELINE_CHRONIC_THRESHOLD` | 0.6 | All scripts |
| `BASELINE_AT_RISK_THRESHOLD` | 0.3 | All scripts |
| `VOLUME_THRESHOLD` | 0.3 | All scripts (volume gate) |
| `BAD_RATIO_THRESHOLD` | 0.4 | staging + daily/weekly |
| `DAILY_LONG_TERM_CONFIDENCE_THRESHOLD` | 0.4 | daily.py |
| `DAILY_LONG_TERM_DAYS` / `DAILY_RECENCY_DAYS` | 30 / 7 | daily.py |
| `WEEKLY_REPEAT_THRESHOLD` | 0.6 | weekly.py |
| `DRIFT_DIRECTION_CONSISTENCY` | 0.7 | drift.py |
| `DRIFT_HOURS` / `DRIFT_MIN_HOURS` | 24 / 12 | drift.py |
| `SUDDEN_SUCCESS_DROP` | 5.0% | sudden.py |
| `SUDDEN_LATENCY_SPIKE` | 1.0s | sudden.py |
| `VOLUME_SUCCESS_CORRELATION_THRESHOLD` | -0.6 | volume1.py |
| `VOLUME_LATENCY_CORRELATION_THRESHOLD` | 0.6 | volume1.py |
| `DELTA_MULTIPLIER` | 1.5 | drift.py (chronic noise filter) |
| `MIN_SUPPORT` | 3 | daily.py, weekly.py, drift.py |
| `CONFIDENCE_DECAY` | 0.9 | daily.py, weekly.py |
| `TTL_DAYS` | per-type dict (see below) | All pattern scripts |

TTL values by pattern type:

| Pattern type | TTL (days) |
|---|---|
| `daily_seasonal` | 45 |
| `weekly_seasonal` | 90 |
| `drift_up` / `drift_down` | 14 |
| `sudden_drop` / `sudden_spike` | 3 |
| `volume_pattern` | 30 |
| `chronic` | never expires (`None`) |

### Pattern Window Formats

Each pattern type writes a specific `pattern_window` string to `ai_service_behavior_memory`:

| Pattern | Example `pattern_window` |
|---------|--------------------------|
| daily | `"Daily 14-15"` |
| weekly | `"Mon 9-10"` |
| drift_up / drift_down | `"Last 24h"` |
| sudden_drop / sudden_spike | `"2025-03-01 14:00"` |
| volume_driven | `"30 Days"` |

### Data Fetching (`fetch_data.py`)

All detector scripts import from `fetch_data.py`. Exported functions (all return pandas DataFrames):

| Function | Source table/view |
|---|---|
| `fetch_data_to_dataframe()` | `ai_detector_staging1` |
| `fetch_baseline_data()` | `ai_baseline_view_2` |
| `fetch_baseline_30d_data()` | `ai_baseline_stats_30d` |
| `fetch_hourly_data()` | `ai_service_features_hourly` |
| `fetch_5m_data()` | `ai_metrics_5m` — **fetched by every detector script via `fetch_data.main()` but not consumed by any of them.** Never finished: it doesn't split rows into a `metric` column the way callers expect (its own docstring notes this is needed), so `volume1.py` runs on `fetch_hourly_data()`'s output instead, same as the other detectors. |

### ClickHouse Connection

`CLICKHOUSE_CONFIG` is built once in `db_config.py` from environment variables (loaded via `python-dotenv` from a local `.env` file) and imported by every script that needs it — no script hardcodes credentials. Required vars: `CLICKHOUSE_HOST`, `CLICKHOUSE_USERNAME`, `CLICKHOUSE_PASSWORD` (script raises `KeyError` if missing); optional with defaults: `CLICKHOUSE_PORT` (443), `CLICKHOUSE_DATABASE` (metrics), `CLICKHOUSE_SECURE` (true), `CLICKHOUSE_VERIFY` (false). See `.env.example` for the template — copy it to `.env` and fill in real values; `.env` is gitignored. `baseline_view.py` and `baseline_stats_30d.py` create SQL `VIEW`s (not materialized tables).

### Table Names (`db_config.py`)

`TABLES` is a dict in `db_config.py`, alongside `CLICKHOUSE_CONFIG`, mapping logical names to actual table/view names — every script imports it instead of hardcoding table names in SQL. Each entry is `.env`-overridable (`TABLE_<NAME>`, e.g. `TABLE_HOURLY`) and defaults to the current schema name if unset, so a stock checkout needs no `.env` changes for this to work:

| `TABLES` key | Default value | `.env` override |
|---|---|---|
| `behavior_memory` | `ai_service_behavior_memory` | `TABLE_BEHAVIOR_MEMORY` |
| `staging` | `ai_detector_staging1` | `TABLE_STAGING` |
| `baseline_view` | `ai_baseline_view_2` | `TABLE_BASELINE_VIEW` |
| `baseline_stats_30d` | `ai_baseline_stats_30d` | `TABLE_BASELINE_STATS_30D` |
| `hourly` | `ai_service_features_hourly` | `TABLE_HOURLY` |
| `metrics_5m` | `ai_metrics_5m` | `TABLE_METRICS_5M` |
| `run_log` | `ai_pattern_run_log` | `TABLE_RUN_LOG` |
| `probability` | `ai_probability` | `TABLE_PROBABILITY` |

When adding a new table, add it to `TABLES` (and `.env.example`) rather than hardcoding the name in a script.

### Testing

There is no test suite in this repository — no pytest/unittest configuration or test files exist.

### Run Log

Every pattern script calls `run_log.log_run()` at the end of `__main__`, writing to `ai_pattern_run_log`. Fields: `script_name`, `anchor`, `started_at`, `completed_at`, `patterns_written`, `status`, `error_message`.

### Scheduler (`scheduler.py`)

Runs as a long-lived process (`python scheduler.py`). Orchestrates the pipeline:

| Trigger | Scripts |
|---------|---------|
| Every hour | `sudden.py`, `drift.py` |
| Daily 00:35 UTC | `baseline_view.py` → `baseline_stats_30d.py` → `stagging.py` → `daily.py` |
| Daily 23:35 UTC | `volume1.py` |
| Weekly Sunday 00:35 UTC | `stagging.py` → `weekly.py` |

Daily/eod/weekly jobs run 35 minutes past the boundary hour rather than exactly on it: `ai_service_features_hourly` ingestion lags ~30 min behind the wall clock, so a job firing exactly at `00:00`/`23:00` would query before the boundary hour's row has landed (e.g. `daily_job` at `00:00` would miss the previous day's `23:00` `ts_hour` row, delaying `MIN_SUPPORT`-based pattern promotion by a full day). The 35-min offset gives a 5-minute buffer past that lag.

### GitHub Actions (`.github/workflows/`)

| Workflow | Cron | Jobs |
|----------|------|------|
| `hourly.yml` | `0 * * * *` | `drift.py` + `sudden.py` (parallel) |
| `daily.yml` | `55 23 * * *` | `stagging.py` → `daily.py` + `volume1.py` (parallel) |
| `weekly.yml` | `55 23 * * 0` | `stagging.py` → `weekly.py` |

Note: GitHub Actions daily runs at 23:55 UTC (end-of-day), while `scheduler.py` runs daily jobs at 00:35 UTC — these are intentionally different schedules.

### Utilities

- `create_tables.py` — one-time setup; `CREATE TABLE IF NOT EXISTS` for `ai_service_behavior_memory` and `ai_detector_staging1` (does not drop/reset existing tables)
- `ai_Probability.py` — unlike every other script here, this unconditionally runs `DROP TABLE IF EXISTS ai_probability` then recreates it empty; it does not compute or insert any risk scores. It is intentionally **not** wired into `scheduler.py` (removed from the daily job) — run it standalone only, and do not re-add it to any scheduled job without first removing the drop and implementing real scoring logic.
