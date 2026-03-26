"""
Daily Pattern Promotion Logic
Contains functions for promoting daily seasonal patterns based on staging data
"""

import pandas as pd
import numpy as np
from datetime import datetime
from config import CONFIG


## Baseline Classifier


def classify_baseline(breach_ratio):
    if breach_ratio >= CONFIG["BASELINE_CHRONIC_THRESHOLD"]:
        return "CHRONIC"
    elif breach_ratio >= CONFIG["BASELINE_AT_RISK_THRESHOLD"]:
        return "AT_RISK"
    return "HEALTHY"




## Common Helpers


def get_baseline(baseline_df, proj, app, svc, metric):
    row = baseline_df[
        (baseline_df.project_id == proj) &
        (baseline_df.application_id == app) &
        (baseline_df.service == svc) &
        (baseline_df.metric == metric)
    ]
    return None if row.empty else row.iloc[0]

def get_baseline_30d(baseline_30d_df, proj, app, svc, metric):
    """
    Get pre-calculated delta values from 30-day baseline stats
    Returns delta_median_success and delta_median_latency
    """
    row = baseline_30d_df[
        (baseline_30d_df.project_id == proj) &
        (baseline_30d_df.application_id == app) &
        (baseline_30d_df.service == svc) &
        (baseline_30d_df.metric == metric)
    ]
    return None if row.empty else row.iloc[0]

def median_delta(hourly_df, proj, app, svc, metric, baseline_value):
    subset = hourly_df[
        (hourly_df.project_id == proj) &
        (hourly_df.application_id == app) &
        (hourly_df.service == svc) &
        (hourly_df.metric == metric)
    ]

    # Keep negative values - they convey important information
    return np.median(subset.success_rate_p50 - baseline_value)

def volume_ok(window_volume, median_volume):
    return window_volume >= CONFIG["VOLUME_THRESHOLD"] * median_volume


## Seasonality Promoter (Daily only)

def promote_seasonality(staging_df, baseline_df, baseline_30d_df, hourly_df, mode, anchor):
    """
    mode = 'daily_candidate' or 'weekly_candidate'

    Args:
        staging_df: Staging data with pattern candidates
        baseline_df: Baseline stats (ai_baseline_view_2)
        baseline_30d_df: 30-day baseline stats with pre-calculated deltas (ai_baseline_stats_30d)
        hourly_df: Hourly metrics data
        mode: 'daily_candidate' or 'weekly_candidate'
    """
    promoted = []

    # Normalize day_of_week to 0-6 format (Monday=0, Sunday=6) in BOTH staging_df and hourly_df
    staging_df = staging_df.copy()
    if staging_df['day_of_week'].max() == 7:
        # ISO format detected (1-7), convert to Pandas format (0-6)
        staging_df['day_of_week'] = (staging_df['day_of_week'] - 1) % 7

    hourly_df = hourly_df.copy()
    if 'day_of_week' in hourly_df.columns and hourly_df['day_of_week'].max() == 7:
        # ISO format detected (1-7), convert to Pandas format (0-6)
        hourly_df['day_of_week'] = (hourly_df['day_of_week'] - 1) % 7

    # For daily patterns, group only by hour (not day_of_week) to get one row per pattern
    # For weekly patterns, keep the original grouping
    if mode == 'daily_candidate':
        grouped = staging_df[
            staging_df.pattern_type == mode
        ].groupby(
            ["project_id", "application_id", "service", "metric", "hour"]
        )
    else:
        grouped = staging_df[
            staging_df.pattern_type == mode
        ].groupby(
            ["project_id", "application_id", "service", "metric", "day_of_week", "hour"]
        )

    for group_key, group in grouped:
        # Unpack group_key based on mode
        if mode == 'daily_candidate':
            proj, app, svc, metric, hour = group_key
            dow = None  # Not used for daily patterns
        else:
            proj, app, svc, metric, dow, hour = group_key

        # Extract service_id from the group (should be consistent across all rows)
        service_id = group['service_id'].iloc[0] if 'service_id' in group.columns else None

        base = get_baseline(baseline_df, proj, app, svc, metric)
        if base is None:
            continue

        baseline_value = float(base.baseline_value)
        median_volume = float(base.median_hour_volumne)
        breach_ratio = float(base.breach_ratio)

        # --- FILTER OUT HEALTHY BASELINE ---
        baseline_state = classify_baseline(breach_ratio)
        if baseline_state == "HEALTHY":
            continue

        # --- CALCULATE LONG_TERM AND RECENCY CONFIDENCE (DAILY PATTERNS ONLY) ---
        long_term_confidence = None
        recency_confidence = None
        support_days = None
        repeat_ratio = None

        if mode == 'daily_candidate':
            # Filter hourly_df for this specific app, service, metric, and hour
            hourly_subset = hourly_df[
                (hourly_df.project_id == proj) &
                (hourly_df.application_id == app) &
                (hourly_df.service == svc) &
                (hourly_df.metric == metric) &
                (hourly_df.hour == hour)
            ].copy()

            # Anchor to scheduled day boundary — prevents delay skew
            max_date = anchor

            # LONG TERM CONFIDENCE: last N days from max date
            long_term_start = max_date - pd.Timedelta(days=CONFIG["DAILY_LONG_TERM_DAYS"] - 1)

            # Count total days from hourly_df where total_windows > 0 at this hour in past 30 days
            long_term_hourly = hourly_subset[
                (hourly_subset.ts_hour >= long_term_start) &
                (hourly_subset.ts_hour <= max_date) &
                (hourly_subset.total_windows > 0)
            ].copy()
            long_term_hourly['date'] = long_term_hourly.ts_hour.dt.date
            long_term_total_days = long_term_hourly['date'].nunique()

            # Count bad days from staging data in past 30 days
            long_term_group = group[
                (group.ts_hour >= long_term_start) &
                (group.ts_hour <= max_date)
            ].copy()
            long_term_group['date'] = long_term_group.ts_hour.dt.date
            long_term_bad_days = long_term_group[long_term_group.bad_ratio >= CONFIG["BAD_RATIO_THRESHOLD"]]['date'].nunique()

            if long_term_total_days > 0:
                long_term_confidence = long_term_bad_days / long_term_total_days
            else:
                long_term_confidence = None

            # RECENCY CONFIDENCE: last N days from max date
            recency_start = max_date - pd.Timedelta(days=CONFIG["DAILY_RECENCY_DAYS"] - 1)

            # Count total days from hourly_df where total_windows > 0 at this hour in past 7 days
            recency_hourly = hourly_subset[
                (hourly_subset.ts_hour >= recency_start) &
                (hourly_subset.ts_hour <= max_date) &
                (hourly_subset.total_windows > 0)
            ].copy()
            recency_hourly['date'] = recency_hourly.ts_hour.dt.date
            recency_total_days = recency_hourly['date'].nunique()

            # Count bad days from staging data in past 7 days
            recency_group = group[
                (group.ts_hour >= recency_start) &
                (group.ts_hour <= max_date)
            ].copy()
            recency_group['date'] = recency_group.ts_hour.dt.date
            recency_bad_days = recency_group[recency_group.bad_ratio >= CONFIG["BAD_RATIO_THRESHOLD"]]['date'].nunique()

            if recency_total_days > 0:
                recency_confidence = recency_bad_days / recency_total_days
            else:
                recency_confidence = None

            # Support days and repeat ratio (use long_term values)
            support_days = long_term_bad_days
            repeat_ratio = long_term_bad_days / max(1, long_term_total_days) if long_term_total_days > 0 else 0

            # Check thresholds for daily patterns - use long_term_confidence for filtering (more lenient)
            if long_term_confidence is None or long_term_confidence < CONFIG["DAILY_LONG_TERM_CONFIDENCE_THRESHOLD"]:
                continue

            if long_term_bad_days < CONFIG["MIN_SUPPORT"]:
                continue
        else:
            # WEEKLY PATTERNS: use week-based logic with ALL historical data
            # Filter hourly_df for this specific app, service, metric, day_of_week, and hour
            hourly_subset = hourly_df[
                (hourly_df.project_id == proj) &
                (hourly_df.application_id == app) &
                (hourly_df.service == svc) &
                (hourly_df.metric == metric) &
                (hourly_df.day_of_week == dow) &
                (hourly_df.hour == hour)
            ].copy()

            # Anchor to scheduled day boundary — prevents delay skew
            max_date = anchor

            # Count total weeks from hourly_df where total_windows > 0 at this day_of_week and hour
            # Use ISO week format to properly handle year boundary issues (%G = ISO year, %V = ISO week)
            hourly_subset['week_year'] = hourly_subset.ts_hour.dt.strftime('%G-W%V')
            valid_weeks = set(hourly_subset[hourly_subset.total_windows > 0]['week_year'])
            total_weeks = len(valid_weeks)

            # Count bad weeks from staging data (only count if week exists in valid_weeks)
            group_copy = group.copy()
            group_copy['week_year'] = group_copy.ts_hour.dt.strftime('%G-W%V')
            bad_weeks_set = set(group_copy[group_copy.bad_ratio >= CONFIG["BAD_RATIO_THRESHOLD"]]['week_year'])
            bad_weeks = len(bad_weeks_set & valid_weeks)  # Intersection ensures bad_weeks <= total_weeks

            support_days = bad_weeks
            repeat_ratio = bad_weeks / max(1, total_weeks) if total_weeks > 0 else 0

            # Check thresholds for weekly patterns
            if repeat_ratio < CONFIG["WEEKLY_REPEAT_THRESHOLD"] or bad_weeks < CONFIG["MIN_SUPPORT"]:
                continue

        # --- VOLUME GATE ---
        # Filter to past N days for volume calculation
        max_date_for_group = max_date  # Use the already-calculated max_date
        gate_days = CONFIG["DAILY_VOLUME_GATE_DAYS"] if mode == 'daily_candidate' else CONFIG["WEEKLY_VOLUME_GATE_DAYS"]
        volume_window_start = max_date_for_group - pd.Timedelta(days=gate_days - 1)

        # For DAILY patterns: use staging data (group) for volume calculation
        # For WEEKLY patterns: use hourly data (hourly_subset) for accurate volume
        if mode == 'daily_candidate':
            group_30d_for_volume = group[
                (group.ts_hour >= volume_window_start) &
                (group.ts_hour <= max_date_for_group)
            ]
            window_volume = group_30d_for_volume.total_requests.median()
        else:
            # Weekly patterns: use all hourly data for this day_of_week + hour combination
            hourly_30d = hourly_subset[
                (hourly_subset.ts_hour >= volume_window_start) &
                (hourly_subset.ts_hour <= max_date_for_group)
            ]
            window_volume = hourly_30d.total_requests.median()
            # Also create group_30d_for_volume for delta calculation below
            group_30d_for_volume = group[
                (group.ts_hour >= volume_window_start) &
                (group.ts_hour <= max_date_for_group)
            ]

        if not volume_ok(window_volume, median_volume):
            continue

        # If staging data falls outside the 30d window, fall back to full group
        if len(group_30d_for_volume) == 0:
            group_30d_for_volume = group

        # --- GET PRE-CALCULATED DELTA VALUES FROM 30D BASELINE ---
        baseline_30d = get_baseline_30d(baseline_30d_df, proj, app, svc, metric)

        if baseline_30d is not None and pd.notna(baseline_30d.delta_median_success):
            # Use pre-calculated chronic delta
            median_d = float(baseline_30d.delta_median_success)
            chronic_delta_latency = float(baseline_30d.delta_median_latency) if pd.notna(baseline_30d.delta_median_latency) else 0.0
        else:
            # Fallback to calculated chronic delta
            median_d = median_delta(hourly_df, proj, app, svc, metric, baseline_value)
            chronic_delta_latency = 0.0

        # Pattern-specific deltas (from past 30 days data)
        window_delta = group_30d_for_volume.delta_success.median()

        # For WEEKLY patterns: always use pattern-specific latency delta from staging data
        # For DAILY patterns: use chronic baseline delta (existing behavior)
        if mode == 'weekly_candidate':
            delta_latency = group_30d_for_volume.delta_latency_p90.median()
        else:
            delta_latency = chronic_delta_latency

        # --- CHRONIC NOISE FILTER ---
        if abs(window_delta) <= CONFIG["DELTA_MULTIPLIER"] * abs(median_d):
            continue

        # Determine pattern type and format pattern_window accordingly
        pattern_type = mode.replace("_candidate", "")

        if pattern_type == "daily":
            # Daily seasonality: "Daily {hour}-{hour+1}"
            pattern_window = f"Daily {hour}-{(hour + 1) % 24}"
        else:  # weekly
            # Weekly seasonality: "{day_name} {hour}-{hour+1}" (e.g., "Mon 1-2")
            day_names = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
            day_name = day_names[dow % 7]  # Use modulo to ensure 0-6 range
            pattern_window = f"{day_name} {hour}-{(hour + 1) % 24}"

        # Calculate first_seen and last_seen from past 30 days data only
        first_seen = group_30d_for_volume.ts_hour.min()
        last_seen = group_30d_for_volume.ts_hour.max()

        promoted.append({
            "project_id": proj,
            "application_id": app,
            "service_id": service_id,
            "service": svc,
            "metric": metric,

            "baseline_state": baseline_state,
            "baseline_value": round(baseline_value, 2),

            "pattern_type": pattern_type,
            "pattern_window": pattern_window,

            "delta_success": round(window_delta, 2),
            "delta_latency_p90": round(delta_latency, 4),  # 4 decimals for millisecond precision

            "support_days": int(support_days),
            "confidence": round(repeat_ratio, 2),

            # New confidence columns (null for weekly patterns)
            "long_term": round(long_term_confidence, 2) if long_term_confidence is not None else None,
            "recency": round(recency_confidence, 2) if recency_confidence is not None else None,

            "first_seen": first_seen,
            "last_seen": last_seen,
            "detected_at": datetime.utcnow()
        })

    return pd.DataFrame(promoted)


if __name__ == "__main__":
    """
    Standalone mode - fetches data, runs daily pattern detection, and writes to ClickHouse
    """
    import logging
    import clickhouse_connect
    from fetch_data import main as fetch_all_data

    # ClickHouse connection configuration
    CLICKHOUSE_CONFIG = {
        'host': 'ec2-47-129-241-41.ap-southeast-1.compute.amazonaws.com',
        'port': 8123,
        'database': 'metrics',
        'username': 'wm_test',
        'password': 'Watermelon@123'
    }
    TARGET_TABLE = 'ai_service_behavior_memory'

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    logger.info("=" * 80)
    logger.info("DAILY PATTERN DETECTION - STANDALONE MODE")
    logger.info("=" * 80)

    # Fetch all required data using fetch_data.py main function
    logger.info("\nFetching all data from ClickHouse...")
    staging_df, baseline_df, baseline_30d_df, hourly_df, metrics_5m_df = fetch_all_data()

    logger.info("\n✓ All data loaded successfully")
    logger.info(f"  - Staging: {staging_df.shape[0]} rows")
    logger.info(f"  - Baseline: {baseline_df.shape[0]} rows")
    logger.info(f"  - 30-day baseline: {baseline_30d_df.shape[0]} rows")
    logger.info(f"  - Hourly: {hourly_df.shape[0]} rows")

    # Run daily pattern detection
    logger.info("\n" + "=" * 80)
    logger.info("Running DAILY pattern detection...")
    logger.info("=" * 80)

    # Anchor to today midnight — any GitHub Actions delay is ignored
    anchor = datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    logger.info(f"\nAnchor (day boundary): {anchor}")

    daily_df = promote_seasonality(staging_df, baseline_df, baseline_30d_df, hourly_df, mode="daily_candidate", anchor=anchor)

    logger.info("\n" + "=" * 80)
    logger.info("RESULTS")
    logger.info("=" * 80)

    print(f"\n✓ Daily patterns detected: {len(daily_df)}")

    if len(daily_df) > 0:
        print(f"\nPattern breakdown:")
        print(daily_df['pattern_type'].value_counts())

        print(f"\n\nFirst 10 patterns:")
        print(daily_df.head(10).to_string())

        # Save to CSV backup
        output_file = f"daily_patterns_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        daily_df.to_csv(output_file, index=False)
        print(f"\n💾 Backup CSV saved to: {output_file}")

        # Write to ClickHouse
        logger.info("\n" + "=" * 80)
        logger.info("Writing to ClickHouse")
        logger.info("=" * 80)

        try:
            logger.info(f"\nConnecting to ClickHouse at {CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}...")
            client = clickhouse_connect.get_client(
                host=CLICKHOUSE_CONFIG['host'],
                port=CLICKHOUSE_CONFIG['port'],
                database=CLICKHOUSE_CONFIG['database'],
                username=CLICKHOUSE_CONFIG['username'],
                password=CLICKHOUSE_CONFIG['password']
            )

            version = client.command('SELECT version()')
            logger.info(f"✓ Connected to ClickHouse version: {version}")

            logger.info(f"\nDeleting existing daily patterns from {TARGET_TABLE}...")
            client.command(f"ALTER TABLE {TARGET_TABLE} DELETE WHERE pattern_type = 'daily'")
            logger.info("✓ Existing daily patterns deleted")

            logger.info(f"\nInserting {len(daily_df)} rows into {TARGET_TABLE}...")
            client.insert_df(TARGET_TABLE, daily_df)

            logger.info(f"✓ Successfully wrote {len(daily_df)} patterns to {TARGET_TABLE}")

            # Verify
            count_query = f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE pattern_type = 'daily'"
            total_count = client.command(count_query)
            logger.info(f"✓ Verified: Total daily patterns in table: {total_count}")

            client.close()
            logger.info("✓ Connection closed")

            print(f"\n✅ SUCCESS: {len(daily_df)} daily patterns written to {TARGET_TABLE}")

        except Exception as e:
            logger.error(f"\n❌ Failed to write to ClickHouse: {str(e)}")
            print(f"\n⚠️  Patterns saved to CSV but failed to write to ClickHouse")
            raise

    else:
        print("\n⚠️  No daily patterns detected with current thresholds")
