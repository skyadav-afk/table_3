"""
Drift Pattern Detection Logic
Detects drift_up and drift_down patterns based on recent hourly data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timezone
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

def detect_drift_pattern(hourly_subset, baseline_row, baseline_30d, max_date):
    """
    Detect drift pattern from recent hourly data

    Args:
        hourly_subset: Hourly data filtered for specific app/service/metric
        baseline_row: Baseline stats from ai_baseline_view_2
        baseline_30d: 30-day baseline stats with pre-calculated deltas
        max_date: this tenant+service's own latest ts_hour (customer-local) - never a
            UTC timestamp, and never shared across tenants/services

    Returns:
        dict with drift pattern info or None
    """
    # Get last N hours of data anchored to this tenant+service's own latest hour
    recent = hourly_subset[hourly_subset['ts_hour'] >= max_date - pd.Timedelta(hours=CONFIG["DRIFT_HOURS"])]

    if len(recent) < CONFIG["DRIFT_MIN_HOURS"]:  # Need at least N hours of data
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

    if not (is_significant and direction_consistency):
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


def promote_drift(baseline_df, baseline_30d_df, hourly_df):
    """
    Detect and promote drift patterns (drift_up and drift_down)

    Args:
        baseline_df: Baseline stats (ai_baseline_view_2)
        baseline_30d_df: 30-day baseline stats with pre-calculated deltas (ai_baseline_stats_30d)
        hourly_df: Hourly metrics data

    Returns:
        DataFrame with promoted drift patterns
    """
    promoted = []

    # Group by project_id, application_id, service, metric (no hour/day_of_week needed for drift)
    grouped = hourly_df.groupby(["project_id", "application_id", "service", "metric"])

    for (proj, app, svc, metric), group in grouped:
        # Extract service_id from the group (should be consistent across all rows)
        service_id = group['service_id'].iloc[0] if 'service_id' in group.columns else None

        # Anchor to THIS tenant+service's own latest local hour (ts_hour is
        # customer-local) - never a global/UTC value shared across tenants
        max_date = group['ts_hour'].max()

        base = get_baseline(baseline_df, proj, app, svc, metric)
        if base is None:
            continue

        baseline_value = float(base.baseline_value)
        median_volume = float(base.median_hour_volumne)
        breach_ratio = float(base.breach_ratio)

        baseline_state = classify_baseline(breach_ratio)

        # Get baseline 30d stats
        baseline_30d = get_baseline_30d(baseline_30d_df, proj, app, svc, metric)

        # Detect drift pattern
        drift_result = detect_drift_pattern(group, base, baseline_30d, max_date)

        if drift_result is None:
            continue

        # --- VOLUME GATE ---
        # Use last N days anchored to this tenant+service's own latest hour
        volume_window_start = max_date - pd.Timedelta(days=CONFIG["DRIFT_VOLUME_GATE_DAYS"] - 1)

        recent_30d = group[
            (group.ts_hour >= volume_window_start) &
            (group.ts_hour <= max_date)
        ]

        window_volume = recent_30d.total_requests.median()
        if not volume_ok(window_volume, median_volume):
            continue

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
            "detected_at_utc": datetime.now(timezone.utc)
        })

    return pd.DataFrame(promoted)


if __name__ == "__main__":
    """
    Standalone mode - fetches data, runs drift pattern detection, and writes to ClickHouse
    """
    import logging
    import clickhouse_connect
    from fetch_data import load_all_data as fetch_all_data
    from run_log import log_run
    from db_config import CLICKHOUSE_CONFIG, TABLES

    TARGET_TABLE = TABLES['behavior_memory']

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    logger.info("=" * 80)
    logger.info("DRIFT PATTERN DETECTION - STANDALONE MODE")
    logger.info("=" * 80)

    started_at = datetime.now(timezone.utc)

    # Fetch all required data using fetch_data.py main function
    logger.info("\nFetching all data from ClickHouse...")
    staging_df, baseline_df, baseline_30d_df, hourly_df, metrics_5m_df = fetch_all_data()

    logger.info("\n[OK] All data loaded successfully")
    logger.info(f"  - Baseline: {baseline_df.shape[0]} rows")
    logger.info(f"  - 30-day baseline: {baseline_30d_df.shape[0]} rows")
    logger.info(f"  - Hourly: {hourly_df.shape[0]} rows")

    # Per-tenant+service anchors are computed inside promote_drift() itself (each
    # group uses its own ts_hour.max()) - this top-level value is only a
    # diagnostic/log-friendly stand-in for ai_pattern_run_log, never used for filtering
    if len(hourly_df) > 0 and hourly_df['ts_hour'].notna().any():
        run_log_anchor = hourly_df['ts_hour'].max().to_pydatetime()
    else:
        run_log_anchor = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
        logger.warning("No hourly data available - nothing to anchor to")
    logger.info(f"\nRun-log reference timestamp: {run_log_anchor}")

    # Run drift pattern detection
    logger.info("\n" + "=" * 80)
    logger.info("Running drift pattern detection...")
    logger.info("=" * 80)

    drift_df = promote_drift(baseline_df, baseline_30d_df, hourly_df)

    logger.info("\n" + "=" * 80)
    logger.info("RESULTS")
    logger.info("=" * 80)

    print(f"\n[OK] Drift patterns detected: {len(drift_df)}")

    if len(drift_df) > 0:
        print(f"\nPattern breakdown:")
        print(drift_df['pattern_type'].value_counts())

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
                password=CLICKHOUSE_CONFIG['password'],
                secure=CLICKHOUSE_CONFIG['secure'],
                verify=CLICKHOUSE_CONFIG['verify'],
            )

            version = client.command('SELECT version()')
            logger.info(f"[OK] Connected to ClickHouse version: {version}")

            logger.info(f"\nInserting {len(drift_df)} rows into {TARGET_TABLE}...")
            client.insert_df(TARGET_TABLE, drift_df)
            client.command(f'OPTIMIZE TABLE {TARGET_TABLE} FINAL')
            logger.info("[OK] Forced merge complete — duplicates collapsed")

            logger.info(f"[OK] Successfully wrote {len(drift_df)} drift patterns to {TARGET_TABLE}")

            # Verify
            count_query = f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE pattern_type IN ('drift_up', 'drift_down')"
            total_count = client.command(count_query)
            logger.info(f"[OK] Verified: Total drift patterns in table: {total_count}")

            client.close()
            logger.info("[OK] Connection closed")

            print(f"\n[OK] SUCCESS: {len(drift_df)} drift patterns written to {TARGET_TABLE}")
            log_run('drift', run_log_anchor, started_at, len(drift_df), 'success')

        except Exception as e:
            logger.error(f"\n[FAIL] Failed to write to ClickHouse: {str(e)}")
            log_run('drift', run_log_anchor, started_at, 0, 'failed', str(e))
            print(f"\n[WARN]  Patterns saved to CSV but failed to write to ClickHouse")
            raise

    else:
        print("\n[WARN]  No drift patterns detected with current thresholds")
        print(f"   - Drift window: {CONFIG['DRIFT_HOURS']} hours")
        print(f"   - Minimum hours required: {CONFIG['DRIFT_MIN_HOURS']}")
        log_run('drift', run_log_anchor, started_at, 0, 'success')
