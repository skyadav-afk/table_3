"""
Single source of truth for the ClickHouse connection config.
Every script imports CLICKHOUSE_CONFIG from here instead of hardcoding credentials.
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
