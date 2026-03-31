"""
Recreate ai_baseline_view_2 and display its current values.

The view is computed over the last 14 days of ai_service_features_hourly.
Run this script to verify the baseline view is correct and up to date.
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
CREATE OR REPLACE VIEW metrics.ai_baseline_view_2 AS
SELECT
    project_id,
    application_id,
    service,
    metric,
    CASE
        WHEN metric = 'success_rate' THEN quantile(0.5)(success_rate_p50)
        WHEN metric = 'latency'      THEN quantile(0.5)(success_rate_p50)
    END AS baseline_value,
    CASE
        WHEN metric = 'success_rate' THEN 0
        WHEN metric = 'latency'      THEN quantile(0.5)(p90_latency)
    END AS baseline_value_p90,
    CASE
        WHEN metric = 'success_rate' THEN quantile(0.5)(total_requests)
        WHEN metric = 'latency'      THEN quantile(0.5)(total_requests)
    END AS median_hour_volumne,
    sum(bad_windows) / sum(total_windows) AS breach_ratio
FROM metrics.ai_service_features_hourly
WHERE ts_hour >= (
    SELECT MAX(ts_hour) FROM metrics.ai_service_features_hourly
) - INTERVAL 14 DAY
GROUP BY project_id, application_id, service, metric
"""


def main():
    logger.info("=" * 70)
    logger.info("UPDATE ai_baseline_view_2")
    logger.info("=" * 70)

    client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
    version = client.command('SELECT version()')
    logger.info(f"Connected to ClickHouse {version}")

    # Recreate the view (picks up any definition changes)
    logger.info("\nRecreating ai_baseline_view_2...")
    client.command(CREATE_VIEW_SQL)
    logger.info("[OK] View recreated")

    # Check the date range being used
    max_ts = client.command("SELECT MAX(ts_hour) FROM metrics.ai_service_features_hourly")
    min_ts = client.command(
        "SELECT MAX(ts_hour) - INTERVAL 14 DAY FROM metrics.ai_service_features_hourly"
    )
    logger.info(f"\nHourly data window: {min_ts}  ->  {max_ts}")

    # Total rows in the view
    total = client.command("SELECT count() FROM metrics.ai_baseline_view_2")
    logger.info(f"Total rows in view: {total}")

    # Breakdown by metric
    result = client.query(
        "SELECT metric, count() as services, "
        "round(avg(baseline_value), 4) as avg_baseline, "
        "round(avg(breach_ratio), 4) as avg_breach_ratio "
        "FROM metrics.ai_baseline_view_2 "
        "GROUP BY metric ORDER BY metric"
    )
    print(f"\n{'metric':<16} {'services':>10} {'avg_baseline':>14} {'avg_breach_ratio':>18}")
    print("-" * 62)
    for row in result.result_rows:
        print(f"{row[0]:<16} {row[1]:>10} {row[2]:>14} {row[3]:>18}")

    # Show CHRONIC services (breach_ratio >= 0.6)
    chronic = client.query(
        "SELECT application_id, service, metric, "
        "round(baseline_value, 4) as baseline_value, "
        "round(breach_ratio, 4) as breach_ratio "
        "FROM metrics.ai_baseline_view_2 "
        "WHERE breach_ratio >= 0.6 "
        "ORDER BY breach_ratio DESC "
        "LIMIT 20"
    )
    chronic_count = client.command(
        "SELECT count() FROM metrics.ai_baseline_view_2 WHERE breach_ratio >= 0.6"
    )
    at_risk_count = client.command(
        "SELECT count() FROM metrics.ai_baseline_view_2 WHERE breach_ratio >= 0.3 AND breach_ratio < 0.6"
    )
    healthy_count = client.command(
        "SELECT count() FROM metrics.ai_baseline_view_2 WHERE breach_ratio < 0.3"
    )

    print(f"\nBaseline state breakdown:")
    print(f"  CHRONIC  (>= 0.6): {chronic_count}")
    print(f"  AT_RISK  (0.3-0.6): {at_risk_count}")
    print(f"  HEALTHY  (< 0.3) : {healthy_count}")

    if chronic.result_rows:
        print(f"\nTop CHRONIC services (breach_ratio >= 0.6):")
        print(f"{'app_id':>8} {'metric':<16} {'baseline':>10} {'breach_ratio':>14}  service")
        print("-" * 90)
        for row in chronic.result_rows:
            svc = row[1][:55] + "..." if len(row[1]) > 55 else row[1]
            print(f"{row[0]:>8} {row[2]:<16} {row[3]:>10} {row[4]:>14}  {svc}")

    client.close()
    logger.info("\n[OK] Done. ai_baseline_view_2 is up to date.")


if __name__ == "__main__":
    main()