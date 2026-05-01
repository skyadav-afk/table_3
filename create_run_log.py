"""
Creates ai_pattern_run_log table in ClickHouse.
Run once at setup - tracks every pattern script execution.
"""

import logging
import clickhouse_connect

CLICKHOUSE_CONFIG = {
    'host': 'wmsandbox5-clickhouse.watermelon.us',
    'port': 443,
    'database': 'metrics',
    'username': 'admin',
    'password': 'W@terlem0n@123#',
    'secure': True,
    'verify': False,
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS metrics.ai_pattern_run_log (
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
    logger.info("CREATE ai_pattern_run_log")
    logger.info("=" * 70)

    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
    version = client.command('SELECT version()')
    logger.info(f"Connected to ClickHouse {version}")

    client.command(CREATE_TABLE_SQL)
    logger.info("[OK] ai_pattern_run_log table created (or already exists)")

    cols = client.query("DESCRIBE TABLE metrics.ai_pattern_run_log")
    print(f"\n{'Column':<20} {'Type':<20}")
    print("-" * 42)
    for row in cols.result_rows:
        print(f"{row[0]:<20} {row[1]:<20}")

    client.close()
    logger.info("\n[OK] Done.")

if __name__ == "__main__":
    main()
