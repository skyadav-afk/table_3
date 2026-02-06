from datetime import datetime, timedelta				
import numpy as np				
import pandas as pd				
				
CONFIG = {				
    "LOOKBACK_WEEKS": 4,				
    "REPEAT_THRESHOLD": 0.6,				
    "MIN_SUPPORT": 3,				
    "BAD_RATIO_THRESHOLD": 0.4,				
    "VOLUME_THRESHOLD": 0.3,				
    "DELTA_MULTIPLIER": 1.5,				
				
    "DRIFT_HOURS": 24,				
    "DRIFT_SUCCESS_THRESHOLD": 2.0,   # %				
    "DRIFT_LATENCY_THRESHOLD": 0.5,   # seconds				
				
    "SUDDEN_SUCCESS_DROP": 5.0,       # %				
    "SUDDEN_LATENCY_SPIKE": 1.0,      # seconds				
				
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