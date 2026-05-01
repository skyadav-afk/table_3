"""
One-time setup: creates ai_service_behavior_memory and ai_detector_staging1.
Run before starting the pipeline for the first time.
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

CREATE_BEHAVIOR_MEMORY_SQL = """
CREATE TABLE IF NOT EXISTS metrics.ai_service_behavior_memory
(
    project_id        UInt32,
    application_id    UInt32,
    service_id        UInt64,
    service           String,
    metric            String,
    baseline_state    String,
    baseline_value    Float64,
    pattern_type      String,
    pattern_window    String,
    delta_success     Float64,
    delta_latency_p90 Float64,
    support_days      UInt16,
    confidence        Float64,
    long_term         Nullable(Float64),
    recency           Nullable(Float64),
    first_seen        DateTime,
    last_seen         DateTime,
    detected_at       DateTime
)
ENGINE = ReplacingMergeTree(detected_at)
ORDER BY (project_id, application_id, service_id, metric, pattern_type, pattern_window)
TTL multiIf(
    pattern_type = 'daily',                              detected_at + INTERVAL 45 DAY,
    pattern_type = 'weekly',                             detected_at + INTERVAL 90 DAY,
    pattern_type IN ('drift_up', 'drift_down'),          detected_at + INTERVAL 14 DAY,
    pattern_type IN ('sudden_drop', 'sudden_spike'),     detected_at + INTERVAL 3 DAY,
    pattern_type = 'volume_driven',                      detected_at + INTERVAL 30 DAY,
    detected_at + INTERVAL 9999 DAY
)
"""

CREATE_STAGING_SQL = """
CREATE TABLE IF NOT EXISTS metrics.ai_detector_staging1
(
    project_id        UInt32,
    application_id    UInt32,
    service           String,
    service_id        UInt64,
    metric            String,
    pattern_type      String,
    day_of_week       UInt8,
    hour              UInt8,
    week_of_year      UInt8,
    month             UInt8,
    year              UInt16,
    pattern_window    String,
    delta_success     Float64,
    delta_latency_p90 Nullable(Float64),
    bad_ratio         Float64,
    total_requests    UInt32,
    ts_hour           DateTime,
    detected_at       DateTime
)
ENGINE = MergeTree()
ORDER BY (project_id, application_id, service_id, metric, pattern_type, hour, day_of_week)
"""


def main():
    logger.info("=" * 70)
    logger.info("CREATE DETECTION TABLES")
    logger.info("=" * 70)

    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
    version = client.command('SELECT version()')
    logger.info(f"Connected to ClickHouse {version}")

    logger.info("\nCreating ai_service_behavior_memory...")
    client.command(CREATE_BEHAVIOR_MEMORY_SQL)
    logger.info("[OK] ai_service_behavior_memory ready")

    logger.info("\nCreating ai_detector_staging1...")
    client.command(CREATE_STAGING_SQL)
    logger.info("[OK] ai_detector_staging1 ready")

    logger.info("\nVerifying tables:")
    for table in ['ai_service_behavior_memory', 'ai_detector_staging1']:
        count = client.command(f"SELECT count() FROM metrics.{table}")
        cols = client.command(
            f"SELECT count() FROM system.columns WHERE database = 'metrics' AND table = '{table}'"
        )
        logger.info(f"  {table}: {cols} columns, {count} rows")

    client.close()
    logger.info("\n[OK] Done. Both tables are ready.")


if __name__ == "__main__":
    main()
