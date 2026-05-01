# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

AI Service Behavior Detector — analyzes hourly service metrics stored in ClickHouse and classifies anomalous patterns into a behavior memory table. Pattern types: `daily`, `weekly`, `drift_up`, `drift_down`, `volume_driven`, `sudden_drop`, `sudden_spike`.

## Setup

```bash
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt

# One-time DB setup
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
python volume1.py              # Reads ai_metrics_5m_v2, not the hourly table
python sudden.py

# 4. (Optional) Risk scores
python ai_Probability.py       # ai_Probability.py is not yet fully implemented
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

- **Anchor timestamps**: Each script fixes an `anchor` at the start (rather than calling `utcnow()` repeatedly) to prevent execution-delay skew:
  - `daily.py`, `weekly.py`, `volume1.py`: today at midnight UTC
  - `drift.py`: current hour UTC
  - `sudden.py`: previous completed hour UTC (`current_hour - 1h`)

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
| `fetch_hourly_data()` | `ai_service_features_hourly` (with fallback table logic) |
| `fetch_5m_data()` | `ai_metrics_5m_v2` (used only by `volume1.py`) |

### ClickHouse Connection

The canonical credentials dict (`CLICKHOUSE_CONFIG`) lives in `fetch_data.py`. However, `baseline_view.py`, `baseline_stats_30d.py`, `run_log.py`, and `create_run_log.py` each hardcode their own connection — update all locations when credentials change. Host: port 8123, database `metrics`. `baseline_view.py` and `baseline_stats_30d.py` create SQL `VIEW`s (not materialized tables).

### Testing

There is no test suite in this repository — no pytest/unittest configuration or test files exist.

### Run Log

Every pattern script calls `run_log.log_run()` at the end of `__main__`, writing to `ai_pattern_run_log`. Fields: `script_name`, `anchor`, `started_at`, `completed_at`, `patterns_written`, `status`, `error_message`.

### Scheduler (`scheduler.py`)

Runs as a long-lived process (`python scheduler.py`). Orchestrates the pipeline:

| Trigger | Scripts |
|---------|---------|
| Every hour | `sudden.py`, `drift.py` |
| Daily 00:00 UTC | `baseline_view.py` → `baseline_stats_30d.py` → `stagging.py` → `daily.py` → `ai_Probability.py` |
| Daily 23:00 UTC | `volume1.py` |
| Weekly Sunday 00:00 UTC | `stagging.py` → `weekly.py` |

### GitHub Actions (`.github/workflows/`)

| Workflow | Cron | Jobs |
|----------|------|------|
| `hourly.yml` | `0 * * * *` | `drift.py` + `sudden.py` (parallel) |
| `daily.yml` | `55 23 * * *` | `stagging.py` → `daily.py` + `volume1.py` (parallel) |
| `weekly.yml` | `55 23 * * 0` | `stagging.py` → `weekly.py` |

Note: GitHub Actions daily runs at 23:55 UTC (end-of-day), while `scheduler.py` runs daily jobs at 00:00 UTC — these are intentionally different schedules.

### Utilities

- `recreate_table.py` — drops and recreates detection tables (use for schema resets only)
