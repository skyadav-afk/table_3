"""
Drift Pattern Detection Logic
Detects drift_up and drift_down patterns based on recent hourly data
"""

import pandas as pd
import numpy as np
from datetime import datetime
from config import CONFIG


## Common Helpers (reused from daily_weekly.py)

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


def volume_ok(window_volume, median_volume):
    return window_volume >= CONFIG["VOLUME_THRESHOLD"] * median_volume


def classify_baseline(breach_ratio):
    if breach_ratio >= CONFIG["BASELINE_CHRONIC_THRESHOLD"]:
        return "CHRONIC"
    elif breach_ratio >= CONFIG["BASELINE_AT_RISK_THRESHOLD"]:
        return "AT_RISK"
    return "HEALTHY"


## Drift Detection

def detect_drift_pattern(hourly_subset, baseline_row, baseline_30d, anchor):
    """
    Detect drift pattern from recent hourly data

    Args:
        hourly_subset: Hourly data filtered for specific app/service/metric
        baseline_row: Baseline stats from ai_baseline_view_2
        baseline_30d: 30-day baseline stats with pre-calculated deltas
        anchor: Fixed hour boundary (datetime truncated to hour) — prevents delay skew

    Returns:
        dict with drift pattern info or None
    """
    import logging
    logger = logging.getLogger(__name__)

    is_grid = baseline_row["metric"] == "success_rate" and "grid" in baseline_row["service"].lower()

    # Get last N hours of data anchored to scheduled hour boundary, not actual runtime
    max_date = anchor
    recent = hourly_subset[hourly_subset['ts_hour'] >= max_date - pd.Timedelta(hours=CONFIG["DRIFT_HOURS"])]

    if is_grid:
        logger.info(f"DEBUG detect_drift: recent hours = {len(recent)}, max_date = {max_date}")

    if len(recent) < CONFIG["DRIFT_MIN_HOURS"]:  # Need at least N hours of data
        if is_grid:
            logger.info(f"DEBUG detect_drift: FAIL - not enough hours ({len(recent)} < {CONFIG['DRIFT_MIN_HOURS']})")
        return None

    # Get baseline values
    if baseline_row["metric"] == "success_rate":
        baseline_value = float(baseline_row.baseline_value)
        baseline_delta = float(baseline_30d.delta_median_success) if baseline_30d is not None and pd.notna(baseline_30d.delta_median_success) else 0.0

        # Calculate delta for success_rate
        delta_values = recent["success_rate_p50"] - baseline_value
        median_delta = delta_values.median()

        # Check if drift is significant
        is_significant = abs(median_delta) >= max(
            CONFIG["DRIFT_SUCCESS_MIN_THRESHOLD"],
            CONFIG["DRIFT_SUCCESS_BASELINE_MULTIPLIER"] * abs(baseline_delta)
        )

    else:  # latency
        baseline_value = float(baseline_row.baseline_value_p90) if pd.notna(baseline_row.baseline_value_p90) else 0.0
        baseline_delta = float(baseline_30d.delta_median_latency) if baseline_30d is not None and pd.notna(baseline_30d.delta_median_latency) else 0.0

        # Calculate delta for latency
        delta_values = recent["p90_latency"] - baseline_value
        median_delta = delta_values.median()

        # Check if drift is significant
        is_significant = abs(median_delta) >= max(
            CONFIG["DRIFT_LATENCY_MIN_THRESHOLD"],
            CONFIG["DRIFT_LATENCY_BASELINE_MULTIPLIER"] * abs(baseline_delta)
        )

    # Check direction consistency
    direction_consistency = (
        (delta_values > 0).mean() > CONFIG["DRIFT_DIRECTION_CONSISTENCY"] or
        (delta_values < 0).mean() > CONFIG["DRIFT_DIRECTION_CONSISTENCY"]
    )

    if is_grid:
        threshold = max(CONFIG["DRIFT_SUCCESS_MIN_THRESHOLD"], CONFIG["DRIFT_SUCCESS_BASELINE_MULTIPLIER"] * abs(baseline_delta)) if baseline_row['metric'] == 'success_rate' else max(CONFIG["DRIFT_LATENCY_MIN_THRESHOLD"], CONFIG["DRIFT_LATENCY_BASELINE_MULTIPLIER"] * abs(baseline_delta))
        logger.info(f"DEBUG detect_drift: median_delta = {median_delta}, threshold = {threshold}")
        logger.info(f"DEBUG detect_drift: is_significant = {is_significant}, direction_consistency = {direction_consistency}")
        logger.info(f"DEBUG detect_drift: positive_ratio = {(delta_values > 0).mean()}, negative_ratio = {(delta_values < 0).mean()}")

    if not (is_significant and direction_consistency):
        if is_grid:
            logger.info(f"DEBUG detect_drift: FAIL - significance or direction check failed")
        return None

    # Determine drift direction
    if median_delta > 0:
        if baseline_row["metric"] == "success_rate":
            pattern_type = "drift_up"  # Success rate increasing (good)
        else:
            pattern_type = "drift_up"  # Latency increasing (bad)
    else:
        if baseline_row["metric"] == "success_rate":
            pattern_type = "drift_down"  # Success rate decreasing (bad)
        else:
            pattern_type = "drift_down"  # Latency decreasing (good)

    # Calculate confidence based on data availability and consistency
    data_completeness = len(recent) / CONFIG["DRIFT_HOURS"]  # How complete is the drift window
    confidence = round(min(1.0, data_completeness * (max((delta_values > 0).mean(), (delta_values < 0).mean()))), 2)

    return {
        "pattern_type": pattern_type,
        "median_delta": float(median_delta),
        "confidence": confidence,
        "data_points": len(recent),
        "first_seen": recent["ts_hour"].min(),
        "last_seen": recent["ts_hour"].max()
    }


def promote_drift(baseline_df, baseline_30d_df, hourly_df, anchor):
    """
    Detect and promote drift patterns (drift_up and drift_down)

    Args:
        baseline_df: Baseline stats (ai_baseline_view_2)
        baseline_30d_df: 30-day baseline stats with pre-calculated deltas (ai_baseline_stats_30d)
        hourly_df: Hourly metrics data

    Returns:
        DataFrame with promoted drift patterns
    """
    import logging
    logger = logging.getLogger(__name__)

    promoted = []

    # Group by project_id, application_id, service, metric (no hour/day_of_week needed for drift)
    grouped = hourly_df.groupby(["project_id", "application_id", "service", "metric"])

    for (proj, app, svc, metric), group in grouped:
        # Extract service_id from the group (should be consistent across all rows)
        service_id = group['service_id'].iloc[0] if 'service_id' in group.columns else None

        # Debug logging for success_rate
        if metric == "success_rate" and "grid" in svc.lower():
            logger.info(f"DEBUG: Processing {app}, {svc[:50]}, {metric}")
        base = get_baseline(baseline_df, proj, app, svc, metric)
        if base is None:
            if metric == "success_rate" and "grid" in svc.lower():
                logger.info(f"DEBUG: No baseline found for {svc[:50]}")
            continue

        baseline_value = float(base.baseline_value)
        median_volume = float(base.median_hour_volumne)
        breach_ratio = float(base.breach_ratio)

        baseline_state = classify_baseline(breach_ratio)

        # Get baseline 30d stats
        baseline_30d = get_baseline_30d(baseline_30d_df, proj, app, svc, metric)

        # Detect drift pattern
        drift_result = detect_drift_pattern(group, base, baseline_30d, anchor)

        if drift_result is None:
            if metric == "success_rate" and "grid" in svc.lower():
                logger.info(f"DEBUG: drift_result is None for {svc[:50]}")
            continue

        if metric == "success_rate" and "grid" in svc.lower():
            logger.info(f"DEBUG: drift_result = {drift_result}")

        # --- VOLUME GATE ---
        # Use last N days anchored to scheduled hour boundary
        max_date = anchor
        volume_window_start = max_date - pd.Timedelta(days=CONFIG["DRIFT_VOLUME_GATE_DAYS"] - 1)

        recent_30d = group[
            (group.ts_hour >= volume_window_start) &
            (group.ts_hour <= max_date)
        ]

        window_volume = recent_30d.total_requests.median()
        if not volume_ok(window_volume, median_volume):
            if metric == "success_rate" and "grid" in svc.lower():
                logger.info(f"DEBUG: Volume gate failed: {window_volume} < {0.3 * median_volume}")
            continue

        if metric == "success_rate" and "grid" in svc.lower():
            logger.info(f"DEBUG: Passed all checks, will promote!")

        # --- GET DELTA VALUES ---
        if metric == "success_rate":
            delta_success = drift_result["median_delta"]
            delta_latency = 0.0
        else:  # latency
            delta_success = 0.0
            delta_latency = drift_result["median_delta"]

        # Pattern window is "Last 24h" for drift patterns
        pattern_window = "Last 24h"

        promoted.append({
            "project_id": proj,
            "application_id": app,
            "service_id": service_id,
            "service": svc,
            "metric": metric,

            "baseline_state": baseline_state,
            "baseline_value": round(baseline_value, 2),

            "pattern_type": drift_result["pattern_type"],
            "pattern_window": pattern_window,

            "delta_success": round(delta_success, 2),
            "delta_latency_p90": round(delta_latency, 4),  # 4 decimals for millisecond precision

            "support_days": (drift_result["last_seen"] - drift_result["first_seen"]).days + 1,  # Days between first and last seen
            "confidence": drift_result["confidence"],

            # For drift patterns, long_term and recency are not applicable
            "long_term": None,
            "recency": None,

            "first_seen": drift_result["first_seen"],
            "last_seen": drift_result["last_seen"],
            "detected_at": datetime.utcnow()
        })

    return pd.DataFrame(promoted)


if __name__ == "__main__":
    """
    Standalone mode - fetches data, runs drift pattern detection, and writes to ClickHouse
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
    logger.info("DRIFT PATTERN DETECTION - STANDALONE MODE")
    logger.info("=" * 80)

    # Fetch all required data using fetch_data.py main function
    logger.info("\nFetching all data from ClickHouse...")
    staging_df, baseline_df, baseline_30d_df, hourly_df, metrics_5m_df = fetch_all_data()

    logger.info("\n✓ All data loaded successfully")
    logger.info(f"  - Baseline: {baseline_df.shape[0]} rows")
    logger.info(f"  - 30-day baseline: {baseline_30d_df.shape[0]} rows")
    logger.info(f"  - Hourly: {hourly_df.shape[0]} rows")

    # Anchor to current hour boundary — any GitHub Actions delay is ignored
    anchor = datetime.utcnow().replace(minute=0, second=0, microsecond=0)
    logger.info(f"\nAnchor (hour boundary): {anchor}")

    # Run drift pattern detection
    logger.info("\n" + "=" * 80)
    logger.info("Running drift pattern detection...")
    logger.info("=" * 80)

    drift_df = promote_drift(baseline_df, baseline_30d_df, hourly_df, anchor)

    logger.info("\n" + "=" * 80)
    logger.info("RESULTS")
    logger.info("=" * 80)

    print(f"\n✓ Drift patterns detected: {len(drift_df)}")

    if len(drift_df) > 0:
        print(f"\nPattern breakdown:")
        print(drift_df['pattern_type'].value_counts())

        print(f"\n\nFirst 10 drift patterns:")
        print(drift_df.head(10).to_string())

        # Save to CSV backup
        output_file = f"drift_patterns_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        drift_df.to_csv(output_file, index=False)
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

            logger.info(f"\nInserting {len(drift_df)} rows into {TARGET_TABLE}...")
            client.insert_df(TARGET_TABLE, drift_df)

            logger.info(f"✓ Successfully wrote {len(drift_df)} drift patterns to {TARGET_TABLE}")

            # Verify
            count_query = f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE pattern_type IN ('drift_up', 'drift_down')"
            total_count = client.command(count_query)
            logger.info(f"✓ Verified: Total drift patterns in table: {total_count}")

            client.close()
            logger.info("✓ Connection closed")

            print(f"\n✅ SUCCESS: {len(drift_df)} drift patterns written to {TARGET_TABLE}")

        except Exception as e:
            logger.error(f"\n❌ Failed to write to ClickHouse: {str(e)}")
            print(f"\n⚠️  Patterns saved to CSV but failed to write to ClickHouse")
            raise

    else:
        print("\n⚠️  No drift patterns detected with current thresholds")
        print(f"   - Drift window: {CONFIG['DRIFT_HOURS']} hours")
        print(f"   - Minimum hours required: {CONFIG['DRIFT_MIN_HOURS']}")
