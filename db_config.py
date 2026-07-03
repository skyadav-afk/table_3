"""
Single source of truth for the ClickHouse connection config and table names.
Every script imports CLICKHOUSE_CONFIG from here instead of hardcoding credentials,
and TABLES instead of hardcoding table/view names.
Values come from the environment (.env) — see .env.example for the required keys.
"""

import os
from dotenv import load_dotenv

load_dotenv()

CLICKHOUSE_CONFIG = {
    'host': os.environ['CLICKHOUSE_HOST'],
    'port': int(os.environ.get('CLICKHOUSE_PORT', '443')),
    'database': os.environ.get('CLICKHOUSE_DATABASE', 'metrics'),
    'username': os.environ['CLICKHOUSE_USERNAME'],
    'password': os.environ['CLICKHOUSE_PASSWORD'],
    'secure': os.environ.get('CLICKHOUSE_SECURE', 'true').lower() == 'true',
    'verify': os.environ.get('CLICKHOUSE_VERIFY', 'false').lower() == 'true',
}

TABLES = {
    'behavior_memory': os.environ.get('TABLE_BEHAVIOR_MEMORY', 'ai_service_behavior_memory'),
    'staging': os.environ.get('TABLE_STAGING', 'ai_detector_staging1'),
    'baseline_view': os.environ.get('TABLE_BASELINE_VIEW', 'ai_baseline_view_2'),
    'baseline_stats_30d': os.environ.get('TABLE_BASELINE_STATS_30D', 'ai_baseline_stats_30d'),
    'hourly': os.environ.get('TABLE_HOURLY', 'ai_service_features_hourly'),
    'metrics_5m': os.environ.get('TABLE_METRICS_5M', 'ai_metrics_5m'),
    'run_log': os.environ.get('TABLE_RUN_LOG', 'ai_pattern_run_log'),
    'probability': os.environ.get('TABLE_PROBABILITY', 'ai_probability'),
}
