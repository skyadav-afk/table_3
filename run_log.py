"""
Shared helper to log pattern script runs into ai_pattern_run_log.
Import and call log_run() at the end of each pattern script's __main__.
"""

import clickhouse_connect
from datetime import datetime

CLICKHOUSE_CONFIG = {
    'host': 'wmsandbox5-clickhouse.watermelon.us',
    'port': 443,
    'database': 'metrics',
    'username': 'admin',
    'password': 'W@terlem0n@123#',
    'secure': True,
    'verify': False,
}

TARGET_TABLE = 'metrics.ai_pattern_run_log'


def log_run(script_name, anchor, started_at, patterns_written, status, error_message=''):
    """
    Insert one row into ai_pattern_run_log.

    Args:
        script_name (str):      e.g. 'daily', 'weekly', 'drift', 'sudden', 'volume'
        anchor (datetime):      the boundary time used by the script
        started_at (datetime):  when the script started
        patterns_written (int): how many rows were inserted into ai_service_behavior_memory
        status (str):           'success' or 'failed'
        error_message (str):    exception message if status == 'failed'
    """
    completed_at = datetime.utcnow()

    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
    client.insert(
        TARGET_TABLE,
        [[script_name, anchor, started_at, completed_at, patterns_written, status, error_message]],
        column_names=['script_name', 'anchor', 'started_at', 'completed_at', 'patterns_written', 'status', 'error_message']
    )
    client.close()
