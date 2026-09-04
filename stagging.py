"""
Update ai_detector_staging1 with fresh daily and weekly candidates
computed from ai_service_features_hourly.

Run this script to refresh the staging table whenever you want
up-to-date pattern candidates based on the latest hourly data.
"""

import logging
import clickhouse_connect

from db_config import CLICKHOUSE_CONFIG, TABLES

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB = CLICKHOUSE_CONFIG['database']
STAGING = f"{DB}.{TABLES['staging']}"
HOURLY = f"{DB}.{TABLES['hourly']}"
BASELINE_VIEW = f"{DB}.{TABLES['baseline_view']}"

DAILY_CANDIDATE_SQL = f"""
INSERT INTO {STAGING}
SELECT
    f.project_id,
    f.application_id,
    f.service,
    f.service_id,
    f.metric,
    'daily_candidate'                                              AS pattern_type,
    toDayOfWeek(f.ts_hour)                                        AS day_of_week,
    f.hour,
    toWeek(f.ts_hour)                                             AS week_of_year,
    toMonth(f.ts_hour)                                            AS month,
    toYear(f.ts_hour)                                             AS year,
    concat('Daily ', toString(f.hour), '-',
           toString((f.hour + 1) % 24))                           AS pattern_window,
    CASE WHEN f.metric = 'success_rate'
         THEN f.success_rate_p50 - coalesce(b.baseline_value, 0)
         ELSE 0
    END                                                            AS delta_success,
    CASE WHEN f.metric = 'latency'
         THEN coalesce(f.p90_latency, 0) - coalesce(b.baseline_value_p90, 0)
         ELSE 0
    END                                                            AS delta_latency_p90,
    f.breach_ratio                                                AS bad_ratio,
    toUInt32(f.total_requests)                                    AS total_requests,
    f.ts_hour,
    now()                                                         AS detected_at
FROM {HOURLY} f
JOIN {BASELINE_VIEW} b USING (project_id, application_id, service, metric)
WHERE f.breach_ratio >= 0.4
"""

WEEKLY_CANDIDATE_SQL = f"""
INSERT INTO {STAGING}
SELECT
    f.project_id,
    f.application_id,
    f.service,
    f.service_id,
    f.metric,
    'weekly_candidate'                                            AS pattern_type,
    toDayOfWeek(f.ts_hour)                                        AS day_of_week,
    f.hour,
    toWeek(f.ts_hour)                                             AS week_of_year,
    toMonth(f.ts_hour)                                            AS month,
    toYear(f.ts_hour)                                             AS year,
    concat(toString(toDayOfWeek(f.ts_hour)), ' ',
           toString(f.hour), '-', toString((f.hour + 1) % 24))   AS pattern_window,
    CASE WHEN f.metric = 'success_rate'
         THEN f.success_rate_p50 - coalesce(b.baseline_value, 0)
         ELSE 0
    END                                                           AS delta_success,
    CASE
        WHEN f.metric = 'latency' THEN f.p90_latency - b.baseline_value_p90
        ELSE 0
    END                                                           AS delta_latency_p90,
    f.breach_ratio                                               AS bad_ratio,
    toUInt32(f.total_requests)                                   AS total_requests,
    f.ts_hour,
    now()                                                        AS detected_at
FROM {HOURLY} f
JOIN {BASELINE_VIEW} b USING (project_id, application_id, service, metric)
WHERE f.breach_ratio >= 0.4
"""


def main():
    logger.info("=" * 70)
    logger.info(f"UPDATE {TABLES['staging']}")
    logger.info("=" * 70)

    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
    version = client.command('SELECT version()')
    logger.info(f"Connected to ClickHouse {version}")

    # Truncate staging table for a clean refresh
    logger.info(f"\nTruncating {TABLES['staging']}...")
    client.command(f'TRUNCATE TABLE {STAGING}')
    logger.info("[OK] Table truncated")

    # Insert daily candidates
    logger.info("\nInserting daily candidates...")
    client.command(DAILY_CANDIDATE_SQL)
    daily_count = client.command(
        f"SELECT count() FROM {STAGING} WHERE pattern_type = 'daily_candidate'"
    )
    logger.info(f"[OK] Daily candidates inserted: {daily_count}")

    # Insert weekly candidates
    logger.info("\nInserting weekly candidates...")
    client.command(WEEKLY_CANDIDATE_SQL)
    weekly_count = client.command(
        f"SELECT count() FROM {STAGING} WHERE pattern_type = 'weekly_candidate'"
    )
    logger.info(f"[OK] Weekly candidates inserted: {weekly_count}")

    total = client.command(f"SELECT count() FROM {STAGING}")
    logger.info(f"\n[OK] Total rows in {TABLES['staging']}: {total}")
    logger.info(f"  - daily_candidate : {daily_count}")
    logger.info(f"  - weekly_candidate: {weekly_count}")

    # Quick sanity check - show a few rows
    logger.info("\nSample rows:")
    result = client.query(
        "SELECT pattern_type, day_of_week, hour, count() as cnt "
        f"FROM {STAGING} "
        "GROUP BY pattern_type, day_of_week, hour "
        "ORDER BY pattern_type, day_of_week, hour "
        "LIMIT 10"
    )
    print(f"\n{'pattern_type':<20} {'day_of_week':>12} {'hour':>6} {'count':>8}")
    print("-" * 50)
    for row in result.result_rows:
        print(f"{row[0]:<20} {row[1]:>12} {row[2]:>6} {row[3]:>8}")

    client.close()
    logger.info(f"\n[OK] Done. {TABLES['staging']} is up to date.")


if __name__ == "__main__":
    main()