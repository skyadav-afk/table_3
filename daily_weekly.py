"""
Daily and Weekly Pattern Promotion Logic
Contains functions for promoting seasonal patterns based on staging data
"""

import pandas as pd
import numpy as np
from datetime import datetime
from config import CONFIG


## Baseline Classifier


def classify_baseline(breach_ratio):
    if breach_ratio >= 0.6:
        return "CHRONIC"
    elif breach_ratio >= 0.3:
        return "AT_RISK"
    return "HEALTHY"




## Common Helpers


def get_baseline(baseline_df, app, svc, metric):
    row = baseline_df[
        (baseline_df.application_id == app) &
        (baseline_df.service == svc) &
        (baseline_df.metric == metric)
    ]
    return None if row.empty else row.iloc[0]

def median_delta(hourly_df, app, svc, metric, baseline_value):
    subset = hourly_df[
        (hourly_df.application_id == app) &
        (hourly_df.service == svc) &
        (hourly_df.metric == metric)
    ]
    
    # Keep negative values - they convey important information
    return np.median(subset.success_rate_p50 - baseline_value)

def volume_ok(window_volume, median_volume):
    return window_volume >= CONFIG["VOLUME_THRESHOLD"] * median_volume


## Seasonality Promoter (Daily + Weekly)

def promote_seasonality(staging_df, baseline_df, hourly_df, mode):
    """
    mode = 'daily_candidate' or 'weekly_candidate'

    Daily patterns: Group by hour only (same hour across all days/weeks)
    Weekly patterns: Group by day_of_week + hour (specific day+hour combination)
    """
    promoted = []

    # Normalize day_of_week to 0-6 format (Monday=0, Sunday=6)
    staging_df = staging_df.copy()
    if staging_df['day_of_week'].max() == 7:
        # ISO format detected (1-7), convert to Pandas format (0-6)
        staging_df['day_of_week'] = (staging_df['day_of_week'] - 1) % 7

    # Filter by pattern type
    filtered_df = staging_df[staging_df.pattern_type == mode]

    # Group differently based on pattern type
    if mode == 'daily_candidate':
        # Daily: Group by hour only (not by day_of_week)
        # This ensures one pattern per hour across all weeks
        grouped = filtered_df.groupby(
            ["application_id", "service", "metric", "hour"]
        )
    else:  # weekly_candidate
        # Weekly: Group by day_of_week AND hour
        # This ensures one pattern per specific day+hour combination
        grouped = filtered_df.groupby(
            ["application_id", "service", "metric", "day_of_week", "hour"]
        )

    for group_key, group in grouped:
        # Unpack group_key based on mode
        if mode == 'daily_candidate':
            app, svc, metric, hour = group_key
            dow = None  # Not used for daily patterns
        else:  # weekly_candidate
            app, svc, metric, dow, hour = group_key
        base = get_baseline(baseline_df, app, svc, metric)
        if base is None:
            continue

        baseline_value = float(base.baseline_value)
        median_volume = float(base.median_hour_volumne)
        breach_ratio = float(base.breach_ratio)

        # --- SUPPORT / REPEAT ---
        group["week"] = group.ts_hour.dt.isocalendar().week
        total_weeks = group["week"].nunique()
        bad_weeks = group[group.bad_ratio >= CONFIG["BAD_RATIO_THRESHOLD"]]["week"].nunique()

        repeat_ratio = bad_weeks / max(1, total_weeks)

        if repeat_ratio < CONFIG["REPEAT_THRESHOLD"] or bad_weeks < CONFIG["MIN_SUPPORT"]:
            continue

        # --- VOLUME GATE ---
        window_volume = group.total_requests.median()
        if not volume_ok(window_volume, median_volume):
            continue

        # --- CHRONIC NOISE FILTER ---
        median_d = median_delta(hourly_df, app, svc, metric, baseline_value)
        window_delta = group.delta_success.median()  # Keep negative values

        # Compare absolute values for filtering, but keep original sign
        if abs(window_delta) < CONFIG["DELTA_MULTIPLIER"] * abs(median_d):
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

        promoted.append({
            "application_id": app,
            "service": svc,
            "metric": metric,

            "baseline_state": classify_baseline(breach_ratio),
            "baseline_value": round(baseline_value, 2),

            "pattern_type": pattern_type,
            "pattern_window": pattern_window,

            "delta_success": round(window_delta, 2),
            "delta_latency_p90": round(group.delta_latency_p90.median(), 2),

            "support_days": int(bad_weeks),
            "confidence": round(repeat_ratio, 2),

            "first_seen": group.ts_hour.min(),
            "last_seen": group.ts_hour.max(),
            "detected_at": datetime.utcnow()
        })

    return pd.DataFrame(promoted)

