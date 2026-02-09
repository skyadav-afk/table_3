# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **pattern detection and promotion pipeline** for service behavior anomalies. It analyzes time-series metrics data from ClickHouse to identify and promote patterns (daily seasonality, weekly seasonality, drift, and volume-driven) in service success rates and latency.

The system identifies behavioral patterns in application services, helping detect when services consistently underperform at specific times, experience drift over time, or degrade under high volume.

## Core Architecture

### Data Flow Pipeline

```
Raw Metrics (service_metrics_hourly_2)
    ↓
Materialized Views → ai_detector_staging1 (pattern candidates)
    ↓
Baseline Calculation → ai_baseline_view_2 (14-day stats)
                    → ai_baseline_stats_30d (30-day deltas)
    ↓
fetch_data.py → Load all data into DataFrames
    ↓
Pattern Detection (in parallel):
├─ daily_weekly.py  → Daily & Weekly seasonal patterns
├─ drift.py         → Drift up/down patterns (24h trends)
├─ volume.py        → Volume-driven patterns (correlation)
└─ sudden.py        → Sudden drop/spike patterns (current hour)
    ↓
Combine & Validate → Volume gate + Chronic noise filter
    ↓
Write to ClickHouse → ai_service_behavior_memory
    ↓
Backup to CSV → promoted_patterns_YYYYMMDD_HHMMSS.csv
```

**Key Data Sources:**
1. **Data Ingestion**: ClickHouse materialized views populate `ai_detector_staging1` with pattern candidates
2. **Data Fetching**: `fetch_data.py` retrieves staging, baseline, and hourly metrics from ClickHouse
3. **Pattern Promotion**: Promotion logic validates candidates and filters noise
4. **Memory Storage**: Promoted patterns are written to `ai_service_behavior_memory` table

### Key Modules

- **`config.py`**: Centralized configuration with thresholds for pattern detection
- **`fetch_data.py`**: ClickHouse data fetching layer with functions for staging, baseline, 30-day baseline, and hourly metrics
- **`daily_weekly.py`**: Core promotion logic for seasonal patterns (daily and weekly)
- **`drift.py`**: Drift pattern detection logic (drift_up and drift_down) based on recent 24-hour trends
- **`volume.py`**: Volume-driven pattern detection using correlation analysis between request volume and performance
- **`sudden.py`**: Sudden drop/spike pattern detection logic for immediate anomalies
- **`run_promotion_pipeline.py`**: Main orchestration script that runs the complete pipeline
- **`recreate_table.py`**: Utility to drop and recreate the memory table

### Pattern Types

The system detects seven pattern types:

1. **`daily_seasonal`**: Patterns that occur at the same hour every day (e.g., "Daily 14-15")
2. **`weekly_seasonal`**: Patterns that occur at specific day-hour combinations (e.g., "Mon 14-15")
3. **`drift_up`**: Recent 24-hour upward trend in metrics
4. **`drift_down`**: Recent 24-hour downward trend in metrics
5. **`volume_driven`**: Performance degradation correlated with request volume (pattern_window: "30 Days")
6. **`sudden_drop`**: Immediate success rate drop >= 5% from baseline (single hour)
7. **`sudden_spike`**: Immediate latency spike >= 1 second from baseline (single hour)

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
5. Promote volume-driven patterns
6. Promote sudden drop/spike patterns
7. Combine and write results to `ai_service_behavior_memory`
8. Save a backup CSV with timestamp

### Utility Scripts

Test data fetching (without promotion):
```bash
python fetch_data.py
```

Recreate the memory table (drops and recreates with correct schema):
```bash
python recreate_table.py
```

Test individual pattern detection modules:
```bash
python daily_weekly.py  # Daily/weekly patterns only
python drift.py         # Drift patterns only
python volume.py        # Volume-driven patterns only
python sudden.py        # Sudden drop/spike patterns only
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

### Drift Detection Logic

**Critical Bug Fix**: Uses **time-based filtering**, not row-based filtering.

```python
# CORRECT - Time-based (last 24 hours)
max_date = hourly_subset['ts_hour'].max()
recent = hourly_subset[hourly_subset['ts_hour'] >= max_date - pd.Timedelta(hours=24)]

# WRONG - Row-based (would break with sparse data)
recent = hourly_subset.tail(24)  # DON'T DO THIS
```

**Requirements:**
- At least 12 hours of data in the last 24 hours
- Significant deviation compared to baseline chronic delta:
  - Success rate: `abs(median_delta) >= max(0.3%, 0.25 * baseline_chronic_delta)`
  - Latency: `abs(median_delta) >= max(0.000001s, 1.5 * baseline_chronic_delta)`
- Direction consistency: 60% of values going in the same direction

**support_days calculation**: Days between first_seen and last_seen (typically 1 day for 24h drift window)

### Volume-Driven Detection Logic

**Time window**: Last 30 days from each service's own max_date (not global max_date)

**Correlation thresholds:**
- Success rate: `correlation <= -0.5` (volume up → success down)
- Latency: `correlation >= 0.5` (volume up → latency up)

**Requirements:**
- At least 30 hours of data in the last 30 days
- Sufficient variance in both volume and metrics
- Must pass volume gate (median volume >= 30% of baseline)

**support_days calculation**: Days between first_seen and last_seen, capped at 30 days

### Sudden Drop/Spike Detection Logic

**Detection method**: Compares the most recent hour to baseline values for immediate anomalies.

**Thresholds:**
- Sudden drop: Success rate drop >= 5% from baseline
- Sudden spike: Latency increase >= 1 second from baseline

**Requirements:**
- Only examines the most recent hour (max timestamp)
- Confidence is set to 1.0 (high confidence for clear threshold breaches)
- Must pass volume gate (median volume >= 30% of baseline)

**support_days calculation**: Always 1 day (single-hour events)

**Pattern window format**: Exact timestamp of the sudden event (e.g., "2024-01-15 14:00")

### Per-Service Max Date

**CRITICAL**: All pattern detection uses **per-service max_date**, not global max_date.

```python
# Get each service's latest data point
max_date = hourly_subset['ts_hour'].max()

# Then calculate time window from that
recent = hourly_subset[hourly_subset['ts_hour'] >= max_date - pd.Timedelta(days=30)]
```

This ensures services with delayed data are analyzed correctly using their own time windows.

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

### Delta Precision

**CRITICAL**: Use 4 decimal places for latency deltas to capture millisecond-scale changes:

```python
"delta_success": round(delta_success, 2),        # 2 decimals for percentage
"delta_latency_p90": round(delta_latency, 4),   # 4 decimals for milliseconds
```

Without 4 decimals, small latency drifts (e.g., 0.0016s) would round to 0.00 and appear as no change.

## Configuration Tuning

Key thresholds in `config.py`:

- `REPEAT_THRESHOLD: 0.6` - Minimum repeat ratio for weekly patterns
- `MIN_SUPPORT: 3` - Minimum support days/weeks/hours
- `BAD_RATIO_THRESHOLD: 0.4` - Threshold for counting "bad" occurrences
- `VOLUME_THRESHOLD: 0.3` - Minimum volume as fraction of median
- `DELTA_MULTIPLIER: 1.5` - Multiplier for chronic noise filter
- `DRIFT_HOURS: 24` - Time window for drift detection

Drift thresholds (tuned for real-world data):
- Success rate: 0.3% minimum, 0.25x baseline chronic delta
- Latency: ~0 minimum (0.000001s), 1.5x baseline chronic delta
- Direction consistency: 60% of values in same direction

Volume-driven thresholds:
- Success rate correlation: <= -0.5
- Latency correlation: >= 0.5
- Minimum data points: 30 hours in 30 days

Sudden drop/spike thresholds:
- Success rate drop: >= 5% from baseline
- Latency spike: >= 1 second from baseline

## Important Gotchas

1. **Day of Week Format**: The staging data uses ISO format (1=Monday, 7=Sunday), but hourly data uses Pandas format (0=Monday, 6=Sunday). The promotion logic normalizes this.

2. **Baseline 30D Delta**: Always prefer pre-calculated `delta_median_success` and `delta_median_latency` from `ai_baseline_stats_30d` over calculating deltas on-the-fly.

3. **Time Windows**:
   - Long-term confidence (daily patterns): Last 30 days from service's max_date
   - Recency confidence (daily patterns): Last 7 days from service's max_date
   - Volume gate (all patterns): Last 30 days from service's max_date
   - Drift detection: Last 24 hours from service's max_date
   - Volume-driven: Last 30 days from service's max_date
   - Sudden drop/spike: Most recent hour only (single timestamp)

4. **ClickHouse Connection**: The connection configuration is duplicated in `fetch_data.py` and `run_promotion_pipeline.py` - keep them in sync.

5. **Diagnostic Queries**: When writing SQL queries to verify pattern detection, always use **per-service max_date** in WHERE clauses, not `now()` or global max_date. Example:

```sql
-- CORRECT - matches Python logic
SELECT ...
FROM service_metrics_hourly_2 AS t1
WHERE ts_hour >= (
  SELECT MAX(ts_hour) - INTERVAL 30 DAY
  FROM service_metrics_hourly_2 AS t2
  WHERE t2.application_id = t1.application_id
    AND t2.service = t1.service
    AND t2.metric = t1.metric
)
GROUP BY application_id, service, metric

-- WRONG - uses global time reference
SELECT ...
WHERE ts_hour >= now() - INTERVAL 30 DAY
```

## Database Schema

The memory table (`ai_service_behavior_memory`) stores these columns:
- Service identifiers: `application_id`, `service`, `metric`
- Baseline info: `baseline_state`, `baseline_value`
- Pattern info: `pattern_type`, `pattern_window`
- Deltas: `delta_success`, `delta_latency_p90` (4 decimals for latency)
- Confidence: `support_days`, `confidence`, `long_term`, `recency`
- Temporal: `first_seen`, `last_seen`, `detected_at`

**Field semantics:**
- `support_days`: Days between first_seen and last_seen (not hours!)
  - Daily patterns: varies based on data
  - Weekly patterns: varies based on data
  - Drift patterns: typically 1 (24-hour window)
  - Volume-driven: 1-30 days (capped at 30)
  - Sudden patterns: always 1 (single-hour event)
- `long_term`, `recency`: Only populated for daily patterns, NULL for others
- `pattern_window`: Human-readable time window (e.g., "Daily 14-15", "Mon 14-15", "Last 24h", "30 Days", "2024-01-15 14:00")

## Debugging and Validation

### Quick ClickHouse Queries

Use curl to query ClickHouse for debugging (connection details in `config.py`):

**Check current hour data vs baseline:**
```bash
curl -s 'http://ec2-47-129-241-41.ap-southeast-1.compute.amazonaws.com:8123/?user=wm_test&password=Watermelon@123&database=metrics' --data-binary "
SELECT h.service, h.metric, h.success_rate_p50, b.baseline_value,
       (b.baseline_value - h.success_rate_p50) as drop
FROM service_metrics_hourly_2 h
JOIN ai_baseline_view_2 b USING (application_id, service, metric)
WHERE h.ts_hour = (SELECT MAX(ts_hour) FROM service_metrics_hourly_2 h2
                   WHERE h2.application_id = h.application_id
                   AND h2.service = h.service AND h2.metric = h.metric)
LIMIT 10 FORMAT PrettyCompact
"
```

**Check promoted patterns:**
```bash
curl -s 'http://ec2-47-129-241-41.ap-southeast-1.compute.amazonaws.com:8123/?user=wm_test&password=Watermelon@123&database=metrics' --data-binary "
SELECT pattern_type, COUNT(*) as count
FROM ai_service_behavior_memory
GROUP BY pattern_type
FORMAT PrettyCompact
"
```

**Find sudden pattern candidates:**
```bash
curl -s 'http://ec2-47-129-241-41.ap-southeast-1.compute.amazonaws.com:8123/?user=wm_test&password=Watermelon@123&database=metrics' --data-binary "
SELECT h.service, h.metric,
       CASE WHEN h.metric = 'success_rate'
            THEN round(b.baseline_value - h.success_rate_p50, 2)
            ELSE round(h.p90_latency - b.baseline_value_p90, 4) END as delta
FROM service_metrics_hourly_2 h
JOIN ai_baseline_view_2 b USING (application_id, service, metric)
WHERE h.ts_hour = (SELECT MAX(ts_hour) FROM service_metrics_hourly_2 h2
                   WHERE h2.application_id = h.application_id
                   AND h2.service = h.service AND h2.metric = h.metric)
  AND ((h.metric = 'success_rate' AND (b.baseline_value - h.success_rate_p50) >= 5.0)
    OR (h.metric = 'latency' AND (h.p90_latency - b.baseline_value_p90) >= 1.0))
FORMAT PrettyCompact
"
```

### Verifying Pattern Detection

After running the pipeline, verify results match expectations:

1. **Check pattern counts**: Compare detected patterns against SQL queries
2. **Verify deltas**: Ensure delta values match between Python output and ClickHouse
3. **Volume gate validation**: Confirm patterns passed the 30% volume threshold
4. **Timestamp alignment**: For sudden patterns, verify pattern_window matches actual event time

### Common Issues and Solutions

**No patterns detected:**
- Check if services have sufficient data (MIN_SUPPORT days/weeks)
- Verify volume gate is being met (30% of baseline median)
- Review thresholds in `config.py` - may be too strict
- Check if baseline data exists for services in question

**Patterns detected in SQL but not in Python:**
- Verify Python logic uses per-service max_date (not global max_date)
- Check if chronic noise filter is filtering out the pattern
- Ensure volume gate calculation uses same 30-day window

**Delta values don't match:**
- Latency: Must use 4 decimal precision (round to 0.0001s)
- Success: Use 2 decimal precision (round to 0.01%)
- Verify baseline source (14-day vs 30-day baseline tables)

**Sudden patterns not triggering:**
- Verify thresholds: 5% for success drop, 1s for latency spike
- Check that comparing against correct baseline table (ai_baseline_view_2)
- Ensure examining most recent hour only (per-service max timestamp)
