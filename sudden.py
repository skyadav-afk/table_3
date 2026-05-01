"""
Sudden Drop/Spike Pattern Detection Logic
Detects sudden_drop and sudden_spike patterns based on immediate hour-to-baseline comparison
"""

import pandas as pd
import numpy as np
from datetime import datetime
from config import CONFIG


## Common Helpers (reused from drift.py and volume.py)

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


## Sudden Drop/Spike Detection

def detect_sudden_pattern(hourly_subset, baseline_row):
    """
    Detect sudden drop/spike pattern from recent hourly data

    Args:
        hourly_subset: Hourly data filtered for specific app/service/metric at global max hour
        baseline_row: Baseline stats from ai_baseline_view_2

    Returns:
        dict with sudden pattern info or None
    """
    import logging
    logger = logging.getLogger(__name__)

    # hourly_subset is already filtered to global max hour, just get the first row
    if hourly_subset.empty:
        return None

    latest = hourly_subset.iloc[0]

    # Get baseline values based on metric type
    if baseline_row["metric"] == "success_rate":
        baseline_success = float(baseline_row.baseline_value)
        baseline_latency = 0.0

        # Calculate drop in success rate
        drop = baseline_success - float(latest["success_rate_p50"])
        spike = 0.0

        # Check if drop is significant (>= 5%)
        if drop >= CONFIG["SUDDEN_SUCCESS_DROP"]:
            return {
                "pattern_type": "sudden_drop",
                "delta_success": -drop,  # Negative because it's a drop
                "delta_latency": 0.0,
                "confidence": 1.0,
                "first_seen": latest["ts_hour"] - pd.Timedelta(hours=1),
                "last_seen": latest["ts_hour"],
                "data_points": 1
            }
    else:  # latency
        baseline_latency = float(baseline_row.baseline_value_p90) if pd.notna(baseline_row.baseline_value_p90) else 0.0

        # Calculate spike in latency
        spike = float(latest["p90_latency"]) - baseline_latency
        drop = 0.0

        # Check if spike is significant (>= 1 second)
        if spike >= CONFIG["SUDDEN_LATENCY_SPIKE"]:
            return {
                "pattern_type": "sudden_spike",
                "delta_success": 0.0,
                "delta_latency": spike,
                "confidence": 1.0,
                "first_seen": latest["ts_hour"] - pd.Timedelta(hours=1),
                "last_seen": latest["ts_hour"],
                "data_points": 1
            }

    return None


def promote_sudden(baseline_df, baseline_30d_df, hourly_df, anchor):
    """
    Detect and promote sudden drop/spike patterns

    Args:
        baseline_df: Baseline stats (ai_baseline_view_2)
        baseline_30d_df: 30-day baseline stats with pre-calculated deltas (ai_baseline_stats_30d)
        hourly_df: Hourly metrics data

    Returns:
        DataFrame with promoted sudden patterns
    """
    import logging
    logger = logging.getLogger(__name__)

    promoted = []

    # Use anchor (previous completed hour) - prevents delay skew
    global_max_hour = anchor
    logger.info(f"Global maximum hour for sudden pattern detection: {global_max_hour}")

    # Filter hourly data to only the global maximum hour
    latest_hour_df = hourly_df[hourly_df['ts_hour'] == global_max_hour]
    logger.info(f"Services with data at global max hour: {len(latest_hour_df)}")

    # Group by project_id, application_id, service, metric
    grouped = latest_hour_df.groupby(["project_id", "application_id", "service", "metric"])

    for (proj, app, svc, metric), group in grouped:
        # Extract service_id from the group (should be consistent across all rows)
        service_id = group['service_id'].iloc[0] if 'service_id' in group.columns else None

        base = get_baseline(baseline_df, proj, app, svc, metric)
        if base is None:
            continue

        baseline_value = float(base.baseline_value) if metric == "success_rate" else float(base.baseline_value_p90) if pd.notna(base.baseline_value_p90) else 0.0
        median_volume = float(base.median_hour_volumne)
        breach_ratio = float(base.breach_ratio)

        baseline_state = classify_baseline(breach_ratio)

        # Detect sudden pattern (group is already filtered to global max hour)
        sudden_result = detect_sudden_pattern(group, base)

        if sudden_result is None:
            continue

        # --- VOLUME GATE ---
        # Check volume from the latest hour (group is already at global max hour)
        window_volume = float(group.iloc[0].total_requests)
        if not volume_ok(window_volume, median_volume):
            continue

        # --- GET DELTA VALUES ---
        delta_success = sudden_result["delta_success"]
        delta_latency = sudden_result["delta_latency"]

        # Pattern window is the specific hour when sudden event occurred
        pattern_hour = sudden_result["first_seen"]
        pattern_window = f"{pattern_hour.strftime('%Y-%m-%d %H:00')}"

        promoted.append({
            "project_id": proj,
            "application_id": app,
            "service_id": service_id,
            "service": svc,
            "metric": metric,

            "baseline_state": baseline_state,
            "baseline_value": round(baseline_value, 2),

            "pattern_type": sudden_result["pattern_type"],
            "pattern_window": pattern_window,

            "delta_success": round(delta_success, 2),
            "delta_latency_p90": round(delta_latency, 4),  # 4 decimals for millisecond precision

            "support_days": 1,  # Sudden events are single-hour occurrences
            "confidence": sudden_result["confidence"],

            # For sudden patterns, long_term and recency are not applicable
            "long_term": None,
            "recency": None,

            "first_seen": sudden_result["first_seen"],
            "last_seen": sudden_result["last_seen"],
            "detected_at": datetime.utcnow()
        })

    return pd.DataFrame(promoted)


if __name__ == "__main__":
    """
    Standalone mode - fetches data, runs sudden pattern detection, and writes to ClickHouse
    """
    import logging
    import clickhouse_connect
    from fetch_data import main as fetch_all_data
    from run_log import log_run

    # ClickHouse connection configuration
    CLICKHOUSE_CONFIG = {
        'host': 'wmsandbox5-clickhouse.watermelon.us',
        'port': 443,
        'database': 'metrics',
        'username': 'admin',
        'password': 'W@terlem0n@123#',
        'secure': True,
        'verify': False,
    }
    TARGET_TABLE = 'ai_service_behavior_memory'

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    logger.info("=" * 80)
    logger.info("SUDDEN DROP/SPIKE PATTERN DETECTION - STANDALONE MODE")
    logger.info("=" * 80)

    started_at = datetime.utcnow()

    # Fetch all required data using fetch_data.py main function
    logger.info("\nFetching all data from ClickHouse...")
    staging_df, baseline_df, baseline_30d_df, hourly_df, metrics_5m_df = fetch_all_data()

    logger.info("\n[OK] All data loaded successfully")
    logger.info(f"  - Baseline: {baseline_df.shape[0]} rows")
    logger.info(f"  - 30-day baseline: {baseline_30d_df.shape[0]} rows")
    logger.info(f"  - Hourly: {hourly_df.shape[0]} rows")

    # Anchor to previous completed hour - any GitHub Actions delay is ignored
    anchor = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - pd.Timedelta(hours=1)
    logger.info(f"\nAnchor (previous completed hour): {anchor}")

    # Run sudden pattern detection
    logger.info("\n" + "=" * 80)
    logger.info("Running sudden pattern detection...")
    logger.info("=" * 80)

    sudden_df = promote_sudden(baseline_df, baseline_30d_df, hourly_df, anchor)

    logger.info("\n" + "=" * 80)
    logger.info("RESULTS")
    logger.info("=" * 80)

    print(f"\n[OK] Sudden patterns detected: {len(sudden_df)}")

    if len(sudden_df) > 0:
        print(f"\nPattern breakdown:")
        print(sudden_df['pattern_type'].value_counts())

        print(f"\n\nFirst 10 sudden patterns:")
        print(sudden_df.head(10).to_string())

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

            logger.info(f"\nInserting {len(sudden_df)} rows into {TARGET_TABLE}...")
            client.insert_df(TARGET_TABLE, sudden_df)

            logger.info(f"[OK] Successfully wrote {len(sudden_df)} sudden patterns to {TARGET_TABLE}")

            # Verify
            count_query = f"SELECT COUNT(*) FROM {TARGET_TABLE} WHERE pattern_type IN ('sudden_drop', 'sudden_spike')"
            total_count = client.command(count_query)
            logger.info(f"[OK] Verified: Total sudden patterns in table: {total_count}")

            client.close()
            logger.info("[OK] Connection closed")

            print(f"\n[OK] SUCCESS: {len(sudden_df)} sudden patterns written to {TARGET_TABLE}")
            log_run('sudden', anchor, started_at, len(sudden_df), 'success')

        except Exception as e:
            logger.error(f"\n[FAIL] Failed to write to ClickHouse: {str(e)}")
            log_run('sudden', anchor, started_at, 0, 'failed', str(e))
            print(f"\n[WARN]  Patterns saved to CSV but failed to write to ClickHouse")
            raise

    else:
        print("\n[WARN]  No sudden patterns detected with current thresholds")
        print(f"   - Success drop threshold: >= {CONFIG['SUDDEN_SUCCESS_DROP']}%")
        print(f"   - Latency spike threshold: >= {CONFIG['SUDDEN_LATENCY_SPIKE']}s")
        log_run('sudden', anchor, started_at, 0, 'success')

