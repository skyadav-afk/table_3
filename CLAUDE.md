# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Project Does

AI Service Behavior Detector — analyzes hourly service metrics stored in ClickHouse and classifies anomalous patterns into a behavior memory table. Pattern types: `daily_candidate`, `weekly_candidate`, `drift_up`, `drift_down`, `volume_driven`, `sudden_drop`, `sudden_spike`.

## Setup

```bash
source venv/bin/activate
pip install -r requirements.txt

# One-time DB setup (only needed on first deployment)
python create_run_log.py       # Creates ai_pattern_run_log table
```

Dependencies: `pandas`, `numpy`, `clickhouse-connect`, `requests==2.31.0`

## Running the Pipeline

Scripts are run standalone in this order:

```bash
# 1. Refresh baseline views (run periodically)
python baseline_view.py        # 14-day rolling baseline → ai_baseline_view_2
python baseline_stats_30d.py   # 30-day delta stats → ai_baseline_stats_30d

# 2. Generate staging candidates
python stagging.py             # Populates ai_detector_staging1

# 3. Detect and promote patterns to memory
python daily.py                # Daily seasonal patterns
python weekly.py               # Weekly seasonal patterns
python drift.py                # Gradual drift
python volume1.py              # Volume-correlated patterns (reads ai_metrics_5m_v2, not hourly table)
python sudden.py               # Sudden drops/spikes

# 4. (Optional) Compute risk scores
python ai_Probability.py       # Populates ai_probability table
```

To debug data fetching independently, run `python fetch_data.py` — its `main()` exercises all fetch functions and prints row counts.

## Architecture

### Data Flow

```
ai_service_features_hourly (raw hourly metrics, ClickHouse)
    ↓
ai_baseline_view_2 (14-day baseline) + ai_baseline_stats_30d (30-day delta stats)
    ↓
ai_detector_staging1 (daily/weekly candidates, breach_ratio ≥ 0.4)
    ↓
Pattern detection: daily.py | weekly.py | drift.py | volume1.py | sudden.py
    ↓
ai_service_behavior_memory (promoted patterns with TTL)
    ↓
ai_probability (service-level risk scores)
```

### Key Design Patterns

- **Volume gating**: All detectors filter out services with volume < 30% of median to avoid false positives on sparse data.
- **TTL-based expiration**: Patterns auto-expire per `TTL_DAYS` in `config.py`. `chronic` patterns never expire.
- **Confidence scoring**: Daily/weekly patterns use a blend of long-term confidence (30-day window) and recency (7-day window) to score quality.
- **Multi-tenant**: All tables and queries are scoped by `project_id`, `application_id`, `service_id`.

### Configuration (`config.py`)

All thresholds live in a single `CONFIG` dict. Key groups:
- `BASELINE_CHRONIC_THRESHOLD` / `BASELINE_AT_RISK_THRESHOLD` — classify services as `HEALTHY`, `AT_RISK`, or `CHRONIC`
- `DRIFT_DIRECTION_CONSISTENCY` (0.7) — min directional consistency for drift
- `VOLUME_SUCCESS_CORRELATION_THRESHOLD` (-0.6) / `VOLUME_LATENCY_CORRELATION_THRESHOLD` (0.6)
- `SUDDEN_SUCCESS_DROP` (5.0%) / `SUDDEN_LATENCY_SPIKE` (1.0s)
- `TTL_DAYS` — per-pattern-type expiry in days

### ClickHouse Connection

Credentials are hardcoded in each script (host, port 8123, database `metrics`). The `fetch_data.py` module centralizes the connection logic and baseline/staging queries — all detector scripts import from it.

### Run Log

Every pattern script calls `run_log.log_run()` on completion, writing to `ai_pattern_run_log`. Fields: `script_name`, `anchor` (boundary time), `started_at`, `completed_at`, `patterns_written`, `status`, `error_message`.

### Utilities

- `recreate_table.py` — drops and recreates detection tables (use for schema resets, not routine operation)

### Scheduler (`scheduler.py`)

Runs as a long-lived process (`python scheduler.py`). Orchestrates the pipeline on schedule:

| Trigger | Scripts |
|---------|---------|
| Every hour | `sudden.py`, `drift.py` |
| Daily 00:00 UTC | `baseline_view.py` → `baseline_stats_30d.py` → `stagging.py` → `daily.py` → `ai_Probability.py` |
| Daily 23:00 UTC | `volume1.py` |
| Weekly Sunday 00:00 | `stagging.py` → `weekly.py` |

GitHub Actions (`.github/workflows/`) mirrors this schedule: `hourly.yml` runs `drift.py` + `sudden.py` in parallel; `daily.yml` runs `stagging.py` then `daily.py` + `volume1.py` in parallel.

### SQL Setup

- `staging.sql` — creates `ai_detector_staging1`, `ai_baseline_view_2`, and materialized views for candidates
- `create_memory_table.sql` — creates `ai_service_behavior_memory1` with partitioning and TTL
