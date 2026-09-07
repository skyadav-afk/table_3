"""
Creates ai_pattern_run_log table in ClickHouse.
Run once at setup - tracks every pattern script execution.
"""

import logging
import clickhouse_connect

from db_config import CLICKHOUSE_CONFIG, TABLES

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB = CLICKHOUSE_CONFIG['database']
RUN_LOG = f"{DB}.{TABLES['run_log']}"

CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS {RUN_LOG} (
    run_id        UUID          DEFAULT generateUUIDv4(),
    script_name   String,
    anchor        DateTime,
    started_at    DateTime,
    completed_at  DateTime,
    patterns_written UInt32,
    status        String,
    error_message String        DEFAULT ''
)
ENGINE = MergeTree()
ORDER BY (script_name, started_at)
"""

def main():
    logger.info("=" * 70)
    logger.info(f"CREATE {TABLES['run_log']}")
    logger.info("=" * 70)

    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
    version = client.command('SELECT version()')
    logger.info(f"Connected to ClickHouse {version}")

    client.command(CREATE_TABLE_SQL)
    logger.info(f"[OK] {TABLES['run_log']} table created (or already exists)")

    count = client.command(f"SELECT count() FROM {RUN_LOG}")
    logger.info(f"  {TABLES['run_log']}: {count} rows")

    client.close()
    logger.info("[OK] Done.")

if __name__ == "__main__":
    main()
