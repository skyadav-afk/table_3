"""
Recreate ai_baseline_stats_30d view and optionally display stats.

Run this script to refresh the 30-day baseline stats view.
"""

import logging
import clickhouse_connect

CLICKHOUSE_CONFIG = {
    'host': 'ec2-47-129-241-41.ap-southeast-1.compute.amazonaws.com',
    'port': 8123,
    'database': 'metrics',
    'username': 'wm_test',
    'password': 'Watermelon@123'
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

CREATE_VIEW_SQL = """
CREATE OR REPLACE VIEW metrics.ai_baseline_stats_30d AS
WITH core AS (
    SELECT *
    FROM metrics.ai_baseline_view_2
),

delta_calc AS (
    SELECT
        h.project_id,
        h.application_id,
        h.service_id,
        h.service,
        h.metric,

        abs(h.success_rate_p50 - c.baseline_value) AS delta_success,
        abs(h.p90_latency - c.baseline_value_p90) AS delta_latency,

        h.ts_hour

    FROM metrics.ai_service_features_hourly h
    INNER JOIN core c
        ON h.project_id     = c.project_id
       AND h.application_id = c.application_id
       AND h.service        = c.service
       AND h.metric         = c.metric

    WHERE h.ts_hour >= (
        SELECT max(ts_hour) - INTERVAL 30 DAY
        FROM metrics.ai_service_features_hourly
    )
)

SELECT
    project_id,
    application_id,
    service_id,
    service,
    metric,
    quantile(0.5)(delta_success) AS delta_median_success,
    quantile(0.5)(delta_latency) AS delta_median_latency,
    count() AS observed_hours_30d,
    count() / (30 * 24.0) AS coverage_ratio_30d

FROM delta_calc
GROUP BY
    project_id,
    application_id,
    service_id,
    service,
    metric;
"""

def main():
    logger.info("=" * 70)
    logger.info("UPDATE ai_baseline_stats_30d")
    logger.info("=" * 70)

    try:
        client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        version = client.command('SELECT version()')
        logger.info(f"Connected to ClickHouse {version}")

        # Recreate the view
        logger.info("\nRecreating ai_baseline_stats_30d view...")
        client.command(CREATE_VIEW_SQL)
        logger.info("[OK] View recreated successfully")

        # Total rows in the view
        total = client.command("SELECT count() FROM metrics.ai_baseline_stats_30d")
        logger.info(f"Total rows in view: {total}")

        # Break down by metric to perform a quick sanity test
        logger.info("\nData Overview:")
        result = client.query(
            "SELECT metric, count() as services, "
            "round(avg(delta_median_success), 4) as avg_success_delta, "
            "round(avg(delta_median_latency), 4) as avg_latency_delta "
            "FROM metrics.ai_baseline_stats_30d "
            "GROUP BY metric ORDER BY metric"
        )
        print(f"\n{'metric':<16} {'services':>10} {'avg_success_delta':>20} {'avg_latency_delta':>20}")
        print("-" * 70)
        for row in result.result_rows:
            print(f"{str(row[0]):<16} {str(row[1]):>10} {str(row[2]):>20} {str(row[3]):>20}")

        client.close()
        logger.info("\n[OK] Done. ai_baseline_stats_30d is up to date.")

    except Exception as e:
        logger.error(f"Failed to update view: {e}")
        raise

if __name__ == "__main__":
    main()
