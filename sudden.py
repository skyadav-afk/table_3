"""
Sudden Drop/Spike Pattern Detection Logic
Detects sudden_drop and sudden_spike patterns based on immediate hour-to-baseline comparison
"""

import pandas as pd
import numpy as np
from datetime import datetime
from config import CONFIG


## Common Helpers (reused from drift.py and volume.py)

def get_baseline(baseline_df, app, svc, metric):
    row = baseline_df[
        (baseline_df.application_id == app) &
        (baseline_df.service == svc) &
        (baseline_df.metric == metric)
    ]
    return None if row.empty else row.iloc[0]


def get_baseline_30d(baseline_30d_df, app, svc, metric):
    """
    Get pre-calculated delta values from 30-day baseline stats
    Returns delta_median_success and delta_median_latency
    """
    row = baseline_30d_df[
        (baseline_30d_df.application_id == app) &
        (baseline_30d_df.service == svc) &
        (baseline_30d_df.metric == metric)
    ]
    return None if row.empty else row.iloc[0]


def volume_ok(window_volume, median_volume):
    return window_volume >= CONFIG["VOLUME_THRESHOLD"] * median_volume


def classify_baseline(breach_ratio):
    if breach_ratio >= 0.6:
        return "CHRONIC"
    elif breach_ratio >= 0.3:
        return "AT_RISK"
    return "HEALTHY"


## Sudden Drop/Spike Detection

def detect_sudden_pattern(hourly_subset, baseline_row):
    """
    Detect sudden drop/spike pattern from recent hourly data

    Args:
        hourly_subset: Hourly data filtered for specific app/service/metric
        baseline_row: Baseline stats from ai_baseline_view_2

    Returns:
        dict with sudden pattern info or None
    """
    import logging
    logger = logging.getLogger(__name__)

    # Get the most recent hour (use max timestamp to get the latest data point)
    max_date = hourly_subset['ts_hour'].max()
    latest_hour = hourly_subset[hourly_subset['ts_hour'] == max_date]

    if latest_hour.empty:
        return None

    latest = latest_hour.iloc[0]

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
                "first_seen": latest["ts_hour"],
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
                "first_seen": latest["ts_hour"],
                "last_seen": latest["ts_hour"],
                "data_points": 1
            }

    return None


def promote_sudden(baseline_df, baseline_30d_df, hourly_df):
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

    # Group by application_id, service, metric
    grouped = hourly_df.groupby(["application_id", "service", "metric"])

    for (app, svc, metric), group in grouped:
        base = get_baseline(baseline_df, app, svc, metric)
        if base is None:
            continue

        baseline_value = float(base.baseline_value) if metric == "success_rate" else float(base.baseline_value_p90) if pd.notna(base.baseline_value_p90) else 0.0
        median_volume = float(base.median_hour_volumne)
        breach_ratio = float(base.breach_ratio)

        baseline_state = classify_baseline(breach_ratio)

        # Detect sudden pattern (only looks at most recent hour)
        sudden_result = detect_sudden_pattern(group, base)

        if sudden_result is None:
            continue

        # --- VOLUME GATE ---
        # Check volume from the latest hour
        max_date = group.ts_hour.max()
        latest_hour = group[group.ts_hour == max_date]

        if latest_hour.empty:
            continue

        window_volume = float(latest_hour.iloc[0].total_requests)
        if not volume_ok(window_volume, median_volume):
            continue

        # --- GET DELTA VALUES ---
        delta_success = sudden_result["delta_success"]
        delta_latency = sudden_result["delta_latency"]

        # Pattern window is the specific hour when sudden event occurred
        pattern_hour = sudden_result["first_seen"]
        pattern_window = f"{pattern_hour.strftime('%Y-%m-%d %H:00')}"

        promoted.append({
            "application_id": app,
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
    Standalone test mode - fetches data and runs sudden pattern detection
    """
    import logging
    from fetch_data import fetch_baseline_data, fetch_baseline_30d_data, fetch_hourly_data

    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)

    logger.info("=" * 80)
    logger.info("SUDDEN DROP/SPIKE PATTERN DETECTION - STANDALONE TEST")
    logger.info("=" * 80)

    # Fetch required data
    logger.info("\n[1/3] Fetching baseline data...")
    baseline_df = fetch_baseline_data()
    logger.info(f"✓ Baseline data loaded: {baseline_df.shape[0]} rows")

    logger.info("\n[2/3] Fetching 30-day baseline data...")
    baseline_30d_df = fetch_baseline_30d_data()
    logger.info(f"✓ 30-day baseline data loaded: {baseline_30d_df.shape[0]} rows")

    logger.info("\n[3/3] Fetching hourly data...")
    hourly_df = fetch_hourly_data()
    logger.info(f"✓ Hourly data loaded: {hourly_df.shape[0]} rows")

    # Run sudden pattern detection
    logger.info("\n" + "=" * 80)
    logger.info("Running sudden pattern detection...")
    logger.info("=" * 80)

    sudden_df = promote_sudden(baseline_df, baseline_30d_df, hourly_df)

    logger.info("\n" + "=" * 80)
    logger.info("RESULTS")
    logger.info("=" * 80)

    print(f"\n✓ Sudden patterns detected: {len(sudden_df)}")

    if len(sudden_df) > 0:
        print(f"\nPattern breakdown:")
        print(sudden_df['pattern_type'].value_counts())

        print(f"\n\nFirst 10 sudden patterns:")
        print(sudden_df.head(10).to_string())

        # Save to CSV
        output_file = "sudden_patterns_test.csv"
        sudden_df.to_csv(output_file, index=False)
        print(f"\n💾 Results saved to: {output_file}")
    else:
        print("\n⚠️  No sudden patterns detected with current thresholds")
        print(f"   - Success drop threshold: >= {CONFIG['SUDDEN_SUCCESS_DROP']}%")
        print(f"   - Latency spike threshold: >= {CONFIG['SUDDEN_LATENCY_SPIKE']}s")

