"""
Volume-Driven Pattern Detection Logic
Detects volume_driven patterns based on correlation between volume and metrics
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


## Volume-Driven Detection

def detect_volume_pattern(hourly_subset, baseline_row, baseline_30d):
    """
    Detect volume-driven pattern from hourly data

    Args:
        hourly_subset: Hourly data filtered for specific app/service/metric
        baseline_row: Baseline stats from ai_baseline_view_2
        baseline_30d: 30-day baseline stats with pre-calculated deltas

    Returns:
        dict with volume pattern info or None
    """
    import logging
    logger = logging.getLogger(__name__)

    # Need at least 30 data points for reliable correlation
    if len(hourly_subset) < 30:
        return None

    # Get last 30 days of data (time-based filtering)
    max_date = hourly_subset['ts_hour'].max()
    recent = hourly_subset[hourly_subset['ts_hour'] >= max_date - pd.Timedelta(days=30)]

    if len(recent) < 30:  # Need at least 30 hours of data in last 30 days
        return None

    # Calculate correlation between volume and metrics
    x = recent["total_requests"]

    if baseline_row["metric"] == "success_rate":
        y = recent["success_rate_p50"]

        # Check for NaN or insufficient variance
        if x.isna().any() or y.isna().any() or x.std() == 0 or y.std() == 0:
            return None

        corr_value = np.corrcoef(x, y)[0, 1]

        # Volume-driven for success_rate: negative correlation (volume up, success down)
        if corr_value <= -0.5:
            pattern_type = "volume_driven"
            confidence = abs(corr_value)

            # Calculate median delta for this pattern
            baseline_value = float(baseline_row.baseline_value)
            median_delta = (y - baseline_value).median()
        else:
            return None

    else:  # latency
        y = recent["p90_latency"]

        # Check for NaN or insufficient variance
        if x.isna().any() or y.isna().any() or x.std() == 0 or y.std() == 0:
            return None

        corr_value = np.corrcoef(x, y)[0, 1]

        # Volume-driven for latency: positive correlation (volume up, latency up)
        if corr_value >= 0.5:
            pattern_type = "volume_driven"
            confidence = abs(corr_value)

            # Calculate median delta for this pattern
            baseline_value = float(baseline_row.baseline_value_p90) if pd.notna(baseline_row.baseline_value_p90) else 0.0
            median_delta = (y - baseline_value).median()
        else:
            return None

    return {
        "pattern_type": pattern_type,
        "correlation": float(corr_value),
        "median_delta": float(median_delta),
        "confidence": round(confidence, 2),
        "data_points": len(recent),
        "first_seen": recent["ts_hour"].min(),
        "last_seen": recent["ts_hour"].max()
    }


def promote_volume(baseline_df, baseline_30d_df, hourly_df):
    """
    Detect and promote volume-driven patterns

    Args:
        baseline_df: Baseline stats (ai_baseline_view_2)
        baseline_30d_df: 30-day baseline stats with pre-calculated deltas (ai_baseline_stats_30d)
        hourly_df: Hourly metrics data

    Returns:
        DataFrame with promoted volume-driven patterns
    """
    import logging
    logger = logging.getLogger(__name__)

    promoted = []

    # Group by application_id, service, metric (no hour/day_of_week needed for volume)
    grouped = hourly_df.groupby(["application_id", "service", "metric"])

    for (app, svc, metric), group in grouped:
        base = get_baseline(baseline_df, app, svc, metric)
        if base is None:
            continue

        baseline_value = float(base.baseline_value)
        median_volume = float(base.median_hour_volumne)
        breach_ratio = float(base.breach_ratio)

        baseline_state = classify_baseline(breach_ratio)

        # Get baseline 30d stats
        baseline_30d = get_baseline_30d(baseline_30d_df, app, svc, metric)

        # Detect volume-driven pattern
        volume_result = detect_volume_pattern(group, base, baseline_30d)

        if volume_result is None:
            continue

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
            continue

        # --- GET DELTA VALUES ---
        if metric == "success_rate":
            delta_success = volume_result["median_delta"]
            delta_latency = 0.0
        else:  # latency
            delta_success = 0.0
            delta_latency = volume_result["median_delta"]

        # Pattern window is "30 Days" for volume patterns
        pattern_window = "30 Days"

        promoted.append({
            "application_id": app,
            "service": svc,
            "metric": metric,

            "baseline_state": baseline_state,
            "baseline_value": round(baseline_value, 2),

            "pattern_type": volume_result["pattern_type"],
            "pattern_window": pattern_window,

            "delta_success": round(delta_success, 2),
            "delta_latency_p90": round(delta_latency, 4),  # 4 decimals for millisecond precision

            "support_days": min(30, (volume_result["last_seen"] - volume_result["first_seen"]).days + 1),  # Days between first and last seen (capped at 30)
            "confidence": volume_result["confidence"],

            # For volume patterns, long_term and recency are not applicable
            "long_term": None,
            "recency": None,

            "first_seen": volume_result["first_seen"],
            "last_seen": volume_result["last_seen"],
            "detected_at": datetime.utcnow()
        })

    return pd.DataFrame(promoted)
