# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **pattern detection and promotion pipeline** for service behavior anomalies. It analyzes time-series metrics data from ClickHouse to identify and promote patterns (daily seasonality, weekly seasonality, and drift) in service success rates and latency.

The system identifies behavioral patterns in application services, helping detect when services consistently underperform at specific times or experience drift over time.

## Core Architecture

### Data Flow Pipeline

1. **Data Ingestion**: ClickHouse materialized views populate `ai_detector_staging1` with pattern candidates
2. **Data Fetching**: `fetch_data.py` retrieves staging, baseline, and hourly metrics from ClickHouse
3. **Pattern Promotion**: Promotion logic validates candidates and filters noise
4. **Memory Storage**: Promoted patterns are written to `ai_service_behavior_memory` table

### Key Modules

- **`config.py`**: Centralized configuration with thresholds for pattern detection (repeat threshold, volume threshold, drift thresholds, TTL days)
- **`fetch_data.py`**: ClickHouse data fetching layer with functions for staging, baseline, 30-day baseline, and hourly metrics
- **`daily_weekly.py`**: Core promotion logic for seasonal patterns (daily and weekly)
- **`drift.py`**: Drift pattern detection logic (drift_up and drift_down)
- **`run_promotion_pipeline.py`**: Main orchestration script that runs the complete pipeline
- **`recreate_table.py`**: Utility to drop and recreate the memory table

### Pattern Types

The system detects four pattern types:

1. **`daily_seasonal`**: Patterns that occur at the same hour every day (e.g., "Daily 14-15")
2. **`weekly_seasonal`**: Patterns that occur at specific day-hour combinations (e.g., "Mon 14-15")
3. **`drift_up`**: Recent 24-hour upward trend in metrics
4. **`drift_down`**: Recent 24-hour downward trend in metrics

### Baseline Classification

Services are classified into three baseline states based on `breach_ratio`:
- **HEALTHY**: breach_ratio < 0.3
- **AT_RISK**: 0.3 <= breach_ratio < 0.6
- **CHRONIC**: breach_ratio >= 0.6

### Data Sources (ClickHouse)

- **`ai_detector_staging1`**: Pattern candidates populated by materialized views
- **`ai_baseline_view_2`**: 14-day baseline statistics (median values, breach ratios)
- **`ai_baseline_stats_30d`**: 30-day baseline with pre-calculated delta values
- **`service_metrics_hourly_2`**: Hourly time-series metrics (success_rate_p50, p90_latency, total_requests)
- **`ai_service_behavior_memory`**: Target table for promoted patterns

## Development Commands

### Environment Setup

Activate virtual environment:
```bash
source venv/bin/activate
```

Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Pipeline

Run the complete promotion pipeline:
```bash
python run_promotion_pipeline.py
```

This will:
1. Fetch all required data from ClickHouse
2. Promote daily seasonal patterns
3. Promote weekly seasonal patterns
4. Promote drift patterns
5. Write results to `ai_service_behavior_memory`
6. Save a backup CSV with timestamp

### Utility Scripts

Test data fetching (without promotion):
```bash
python fetch_data.py
```

Recreate the memory table (drops and recreates with correct schema):
```bash
python recreate_table.py
```

Test daily/weekly promotion logic only:
```bash
python daily_weekly.py
```

Test drift detection logic only:
```bash
python drift.py
```

## Critical Implementation Details

### Daily vs Weekly Pattern Logic

**Daily patterns** (mode='daily_candidate'):
- Group by: `application_id, service, metric, hour` (NO day_of_week)
- Filtered by baseline state: ONLY promote AT_RISK and CHRONIC (HEALTHY filtered out)
- Confidence calculation: Uses both `long_term` (30-day) and `recency` (7-day) confidence scores
- Threshold: `long_term_confidence >= 0.4` and `support_days >= MIN_SUPPORT`
- Day_of_week normalization: Converts ISO format (1-7) to Pandas format (0-6)

**Weekly patterns** (mode='weekly_candidate'):
- Group by: `application_id, service, metric, day_of_week, hour`
- No baseline state filtering (promotes all states)
- Confidence calculation: Uses week-based repeat ratio across ALL historical data
- Threshold: `repeat_ratio >= REPEAT_THRESHOLD` and `bad_weeks >= MIN_SUPPORT`
- Uses `week_year` format to avoid year boundary issues

### Volume Gate

All patterns must pass a volume gate:
```python
window_volume >= VOLUME_THRESHOLD * median_volume
```
Where `window_volume` is the median of the last 30 days of data.

### Chronic Noise Filter

Pattern-specific delta must be significantly larger than chronic (baseline) delta:
```python
abs(window_delta) > DELTA_MULTIPLIER * abs(median_d)
```

This prevents promoting patterns that are just chronic baseline behavior.

### Drift Detection

Drift patterns require:
- At least 12 hours of data in the last 24 hours
- Significant deviation: `>= max(threshold, 2 * baseline_delta)`
- Direction consistency: 70% of values going in the same direction

## Configuration Tuning

Key thresholds in `config.py`:

- `REPEAT_THRESHOLD: 0.6` - Minimum repeat ratio for weekly patterns
- `MIN_SUPPORT: 3` - Minimum support days/weeks/hours
- `BAD_RATIO_THRESHOLD: 0.4` - Threshold for counting "bad" occurrences
- `VOLUME_THRESHOLD: 0.3` - Minimum volume as fraction of median
- `DELTA_MULTIPLIER: 1.5` - Multiplier for chronic noise filter
- `DRIFT_HOURS: 24` - Time window for drift detection

## Important Gotchas

1. **Day of Week Format**: The staging data uses ISO format (1=Monday, 7=Sunday), but hourly data uses Pandas format (0=Monday, 6=Sunday). The promotion logic normalizes this.

2. **Baseline 30D Delta**: Always prefer pre-calculated `delta_median_success` and `delta_median_latency` from `ai_baseline_stats_30d` over calculating deltas on-the-fly.

3. **Time Windows**:
   - Long-term confidence: Last 30 days from max date
   - Recency confidence: Last 7 days from max date
   - Volume gate: Last 30 days from max date
   - Drift detection: Last 24 hours

4. **ClickHouse Connection**: The connection configuration is duplicated in `fetch_data.py` and `run_promotion_pipeline.py` - keep them in sync.

## Database Schema

The memory table (`ai_service_behavior_memory`) stores these columns:
- Service identifiers: `application_id`, `service`, `metric`
- Baseline info: `baseline_state`, `baseline_value`
- Pattern info: `pattern_type`, `pattern_window`
- Deltas: `delta_success`, `delta_latency_p90`
- Confidence: `support_days`, `confidence`, `long_term`, `recency`
- Temporal: `first_seen`, `last_seen`, `detected_at`

Note: `long_term` and `recency` are NULL for weekly and drift patterns (only used for daily patterns).
