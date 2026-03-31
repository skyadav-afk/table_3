from datetime import datetime, timedelta
import numpy as np
import pandas as pd

CONFIG = {
    # ========================================================================
    # BASELINE CLASSIFICATION THRESHOLDS
    # Used in: daily_weekly.py, drift.py, volume.py, sudden.py
    # ========================================================================
    "BASELINE_CHRONIC_THRESHOLD": 0.6,      # breach_ratio >= 0.6 -> CHRONIC
    "BASELINE_AT_RISK_THRESHOLD": 0.3,      # breach_ratio >= 0.3 -> AT_RISK

    # ========================================================================
    # COMMON THRESHOLDS (used across multiple modules)
    # ========================================================================
    "VOLUME_THRESHOLD": 0.3,                # Minimum volume as fraction of median (30%)
                                            # Used in: daily_weekly.py, drift.py, volume.py, sudden.py

    "MIN_SUPPORT": 3,                       # Minimum support days/weeks/hours
                                            # Used in: daily_weekly.py

    "BAD_RATIO_THRESHOLD": 0.4,             # Threshold for counting "bad" occurrences
                                            # Used in: daily_weekly.py

    "DELTA_MULTIPLIER": 1.5,                # Multiplier for chronic noise filter
                                            # Used in: daily_weekly.py

    # ========================================================================
    # DAILY_WEEKLY.PY THRESHOLDS
    # ========================================================================
    # Daily patterns
    "DAILY_LONG_TERM_CONFIDENCE_THRESHOLD": 0.4,    # Minimum long-term confidence for daily patterns
    "DAILY_LONG_TERM_DAYS": 30,                     # Long-term window (days)
    "DAILY_RECENCY_DAYS": 7,                        # Recency window (days)
    "DAILY_VOLUME_GATE_DAYS": 30,                   # Volume gate window (days)

    # Weekly patterns
    "WEEKLY_REPEAT_THRESHOLD": 0.6,                 # Minimum repeat ratio for weekly patterns
    "WEEKLY_VOLUME_GATE_DAYS": 30,                  # Volume gate window (days)

    # ========================================================================
    # DRIFT.PY THRESHOLDS
    # ========================================================================
    "DRIFT_HOURS": 24,                              # Time window for drift detection (hours)
    "DRIFT_MIN_HOURS": 12,                          # Minimum hours of data required
    "DRIFT_DIRECTION_CONSISTENCY": 0.7,             # Direction consistency threshold (60%)
    "DRIFT_VOLUME_GATE_DAYS": 30,                   # Volume gate window (days)

    # Success rate drift thresholds
    "DRIFT_SUCCESS_MIN_THRESHOLD": 0.3,             # Minimum absolute delta (%)
    "DRIFT_SUCCESS_BASELINE_MULTIPLIER": 2,      # Multiplier for baseline chronic delta

    # Latency drift thresholds
    "DRIFT_LATENCY_MIN_THRESHOLD": 0.01,        # Minimum absolute delta (seconds)
    "DRIFT_LATENCY_BASELINE_MULTIPLIER": 2,       # Multiplier for baseline chronic delta

    # ========================================================================
    # VOLUME.PY THRESHOLDS
    # ========================================================================
    "VOLUME_MIN_DATA_POINTS": 30,                   # Minimum data points for correlation
    "VOLUME_TIME_WINDOW_DAYS": 30,                  # Time window for volume analysis (days)
    "VOLUME_GATE_DAYS": 30,                         # Volume gate window (days)

    # Correlation thresholds
    "VOLUME_SUCCESS_CORRELATION_THRESHOLD": -0.6,   # Success rate: volume up -> success down
    "VOLUME_LATENCY_CORRELATION_THRESHOLD": 0.6,    # Latency: volume up -> latency up

    # ========================================================================
    # SUDDEN.PY THRESHOLDS
    # ========================================================================
    "SUDDEN_SUCCESS_DROP": 5.0,                     # Success rate drop threshold (%)
    "SUDDEN_LATENCY_SPIKE": 1.0,                    # Latency spike threshold (seconds)

    # ========================================================================
    # LEGACY/DEPRECATED THRESHOLDS (kept for backward compatibility)
    # ========================================================================
    "LOOKBACK_WEEKS": 4,                            # Legacy - not actively used
    "REPEAT_THRESHOLD": 0.6,                        # Legacy - use WEEKLY_REPEAT_THRESHOLD instead
    "DRIFT_SUCCESS_THRESHOLD": 2.0,                 # Legacy - not actively used
    "DRIFT_LATENCY_THRESHOLD": 0.5,                 # Legacy - not actively used

    # ========================================================================
    # OTHER CONFIGURATION
    # ========================================================================
    "CONFIDENCE_DECAY": 0.9,
    "TTL_DAYS": {
        "daily_seasonal": 45,
        "weekly_seasonal": 90,
        "drift_up": 14,
        "drift_down": 14,
        "sudden_drop": 3,
        "sudden_spike": 3,
        "volume_pattern": 30,
        "chronic": None
    }
}