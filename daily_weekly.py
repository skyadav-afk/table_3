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

def promote_seasonality(staging_df, baseline_df, baseline_30d_df, hourly_df, mode):
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

    # Normalize day_of_week to 0-6 format (Monday=0, Sunday=6)
    staging_df = staging_df.copy()
    if staging_df['day_of_week'].max() == 7:
        # ISO format detected (1-7), convert to Pandas format (0-6)
        staging_df['day_of_week'] = (staging_df['day_of_week'] - 1) % 7

    # For daily patterns, group only by hour (not day_of_week) to get one row per pattern
    # For weekly patterns, keep the original grouping
    if mode == 'daily_candidate':
        grouped = staging_df[
            staging_df.pattern_type == mode
        ].groupby(
            ["application_id", "service", "metric", "hour"]
        )
    else:
        grouped = staging_df[
            staging_df.pattern_type == mode
        ].groupby(
            ["application_id", "service", "metric", "day_of_week", "hour"]
        )

    for group_key, group in grouped:
        # Unpack group_key based on mode
        if mode == 'daily_candidate':
            app, svc, metric, hour = group_key
            dow = None  # Not used for daily patterns
        else:
            app, svc, metric, dow, hour = group_key
        base = get_baseline(baseline_df, app, svc, metric)
        if base is None:
            continue

        baseline_value = float(base.baseline_value)
        median_volume = float(base.median_hour_volumne)
        breach_ratio = float(base.breach_ratio)

        # --- FILTER OUT HEALTHY BASELINE FOR DAILY PATTERNS ---
        baseline_state = classify_baseline(breach_ratio)
        if mode == 'daily_candidate' and baseline_state == "HEALTHY":
            continue

        # --- CALCULATE LONG_TERM AND RECENCY CONFIDENCE (DAILY PATTERNS ONLY) ---
        long_term_confidence = None
        recency_confidence = None
        support_days = None
        repeat_ratio = None

        if mode == 'daily_candidate':
            # Filter hourly_df for this specific app, service, metric, and hour
            hourly_subset = hourly_df[
                (hourly_df.application_id == app) &
                (hourly_df.service == svc) &
                (hourly_df.metric == metric) &
                (hourly_df.hour == hour)
            ].copy()

            # Get the maximum date from hourly_df
            max_date = hourly_subset.ts_hour.max()

            # LONG TERM CONFIDENCE: last 30 days from max date
            long_term_start = max_date - pd.Timedelta(days=29)  # 29 days before + current day = 30 days

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

            # RECENCY CONFIDENCE: last 7 days from max date
            recency_start = max_date - pd.Timedelta(days=6)  # 6 days before + current day = 7 days

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
            if long_term_confidence is None or long_term_confidence < 0.4:
                continue

            if long_term_bad_days < CONFIG["MIN_SUPPORT"]:
                continue
        else:
            # WEEKLY PATTERNS: use week-based logic with ALL historical data
            # Filter hourly_df for this specific app, service, metric, day_of_week, and hour
            hourly_subset = hourly_df[
                (hourly_df.application_id == app) &
                (hourly_df.service == svc) &
                (hourly_df.metric == metric) &
                (hourly_df.day_of_week == dow) &
                (hourly_df.hour == hour)
            ].copy()

            # Get the maximum date from hourly_df
            max_date = hourly_subset.ts_hour.max()

            # Count total weeks from hourly_df where total_windows > 0 at this day_of_week and hour
            # Use year-week format to avoid year boundary issues
            hourly_subset['week_year'] = (
                hourly_subset.ts_hour.dt.isocalendar().year.astype(str) + '-W' +
                hourly_subset.ts_hour.dt.isocalendar().week.astype(str).str.zfill(2)
            )
            valid_weeks = set(hourly_subset[hourly_subset.total_windows > 0]['week_year'])
            total_weeks = len(valid_weeks)

            # Count bad weeks from staging data (only count if week exists in valid_weeks)
            group_copy = group.copy()
            group_copy['week_year'] = (
                group_copy.ts_hour.dt.isocalendar().year.astype(str) + '-W' +
                group_copy.ts_hour.dt.isocalendar().week.astype(str).str.zfill(2)
            )
            bad_weeks_set = set(group_copy[group_copy.bad_ratio >= CONFIG["BAD_RATIO_THRESHOLD"]]['week_year'])
            bad_weeks = len(bad_weeks_set & valid_weeks)  # Intersection ensures bad_weeks <= total_weeks

            support_days = bad_weeks
            repeat_ratio = bad_weeks / max(1, total_weeks) if total_weeks > 0 else 0

            # Check thresholds for weekly patterns
            if repeat_ratio < CONFIG["REPEAT_THRESHOLD"] or bad_weeks < CONFIG["MIN_SUPPORT"]:
                continue

        # --- VOLUME GATE ---
        # Filter group to past 30 days for volume calculation (both daily and weekly)
        max_date_for_group = max_date  # Use the already-calculated max_date

        volume_window_start = max_date_for_group - pd.Timedelta(days=29)  # 30 days

        group_30d_for_volume = group[
            (group.ts_hour >= volume_window_start) &
            (group.ts_hour <= max_date_for_group)
        ]

        window_volume = group_30d_for_volume.total_requests.median()
        if not volume_ok(window_volume, median_volume):
            continue

        # --- GET PRE-CALCULATED DELTA VALUES FROM 30D BASELINE ---
        baseline_30d = get_baseline_30d(baseline_30d_df, app, svc, metric)

        if baseline_30d is not None and pd.notna(baseline_30d.delta_median_success):
            # Use pre-calculated chronic delta
            median_d = float(baseline_30d.delta_median_success)
            delta_latency = float(baseline_30d.delta_median_latency) if pd.notna(baseline_30d.delta_median_latency) else 0.0
        else:
            # Fallback to calculated chronic delta
            median_d = median_delta(hourly_df, app, svc, metric, baseline_value)
            delta_latency = group_30d_for_volume.delta_latency_p90.median()

        # Pattern-specific delta (from past 30 days data)
        window_delta = group_30d_for_volume.delta_success.median()

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
            "application_id": app,
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