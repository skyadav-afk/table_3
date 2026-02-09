"""
Drift Pattern Detection Logic
Detects drift_up and drift_down patterns based on recent hourly data
"""

import pandas as pd
import numpy as np
from datetime import datetime
from config import CONFIG


## Common Helpers (reused from daily_weekly.py)

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


## Drift Detection

def detect_drift_pattern(hourly_subset, baseline_row, baseline_30d):
    """
    Detect drift pattern from recent hourly data

    Args:
        hourly_subset: Hourly data filtered for specific app/service/metric
        baseline_row: Baseline stats from ai_baseline_view_2
        baseline_30d: 30-day baseline stats with pre-calculated deltas

    Returns:
        dict with drift pattern info or None
    """
    import logging
    logger = logging.getLogger(__name__)

    is_grid = baseline_row["metric"] == "success_rate" and "grid" in baseline_row["service"].lower()

    # Get last 24 hours of data (actual time-based filtering, not row-based)
    max_date = hourly_subset['ts_hour'].max()
    recent = hourly_subset[hourly_subset['ts_hour'] >= max_date - pd.Timedelta(hours=24)]

    if is_grid:
        logger.info(f"DEBUG detect_drift: recent hours = {len(recent)}, max_date = {max_date}")

    if len(recent) < 12:  # Need at least 12 hours of data
        if is_grid:
            logger.info(f"DEBUG detect_drift: FAIL - not enough hours ({len(recent)} < 12)")
        return None

    # Get baseline values
    if baseline_row["metric"] == "success_rate":
        baseline_value = float(baseline_row.baseline_value)
        baseline_delta = float(baseline_30d.delta_median_success) if baseline_30d is not None and pd.notna(baseline_30d.delta_median_success) else 0.0

        # Calculate delta for success_rate
        delta_values = recent["success_rate_p50"] - baseline_value
        median_delta = delta_values.median()

        # Check if drift is significant (>= 0.25x baseline chronic delta, more lenient for success_rate)
        is_significant = abs(median_delta) >= max(0.3, 0.005 * abs(baseline_delta))

    else:  # latency
        baseline_value = float(baseline_row.baseline_value_p90) if pd.notna(baseline_row.baseline_value_p90) else 0.0
        baseline_delta = float(baseline_30d.delta_median_latency) if baseline_30d is not None and pd.notna(baseline_30d.delta_median_latency) else 0.0

        # Calculate delta for latency
        delta_values = recent["p90_latency"] - baseline_value
        median_delta = delta_values.median()

        # Check if drift is significant (>= 1.5x baseline chronic delta)
        is_significant = abs(median_delta) >= max(0.000001, 1.5 * abs(baseline_delta))

    # Check direction consistency (60% of values going in same direction)
    direction_consistency = (
        (delta_values > 0).mean() > 0.6 or
        (delta_values < 0).mean() > 0.6
    )

    if is_grid:
        logger.info(f"DEBUG detect_drift: median_delta = {median_delta}, threshold = {max(0.3, 0.25 * abs(baseline_delta)) if baseline_row['metric'] == 'success_rate' else max(0.000001, 1.5 * abs(baseline_delta))}")
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
    data_completeness = len(recent) / 24  # How complete is the 24h window
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
    import logging
    logger = logging.getLogger(__name__)

    promoted = []

    # Group by application_id, service, metric (no hour/day_of_week needed for drift)
    grouped = hourly_df.groupby(["application_id", "service", "metric"])

    for (app, svc, metric), group in grouped:
        # Debug logging for success_rate
        if metric == "success_rate" and "grid" in svc.lower():
            logger.info(f"DEBUG: Processing {app}, {svc[:50]}, {metric}")
        base = get_baseline(baseline_df, app, svc, metric)
        if base is None:
            if metric == "success_rate" and "grid" in svc.lower():
                logger.info(f"DEBUG: No baseline found for {svc[:50]}")
            continue

        baseline_value = float(base.baseline_value)
        median_volume = float(base.median_hour_volumne)
        breach_ratio = float(base.breach_ratio)

        baseline_state = classify_baseline(breach_ratio)

        # Get baseline 30d stats
        baseline_30d = get_baseline_30d(baseline_30d_df, app, svc, metric)

        # Detect drift pattern
        drift_result = detect_drift_pattern(group, base, baseline_30d)

        if drift_result is None:
            if metric == "success_rate" and "grid" in svc.lower():
                logger.info(f"DEBUG: drift_result is None for {svc[:50]}")
            continue

        if metric == "success_rate" and "grid" in svc.lower():
            logger.info(f"DEBUG: drift_result = {drift_result}")

        # --- VOLUME GATE ---
        # Use last 30 days for volume calculation
        max_date = group.ts_hour.max()
        volume_window_start = max_date - pd.Timedelta(days=29)

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
            "application_id": app,
            "service": svc,
            "metric": metric,

            "baseline_state": baseline_state,
            "baseline_value": round(baseline_value, 2),

            "pattern_type": drift_result["pattern_type"],
            "pattern_window": pattern_window,

            "delta_success": round(delta_success, 2),
            "delta_latency_p90": round(delta_latency, 4),  # 4 decimals for millisecond precision

            "support_days": drift_result["data_points"],  # Number of hours with data
            "confidence": drift_result["confidence"],

            # For drift patterns, long_term and recency are not applicable
            "long_term": None,
            "recency": None,

            "first_seen": drift_result["first_seen"],
            "last_seen": drift_result["last_seen"],
            "detected_at": datetime.utcnow()
        })

    return pd.DataFrame(promoted)
