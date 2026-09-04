"""
Recreate ai_baseline_stats_30d view and optionally display stats.

Run this script to refresh the 30-day baseline stats view.
"""

import logging
import clickhouse_connect

from db_config import CLICKHOUSE_CONFIG, TABLES

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DB = CLICKHOUSE_CONFIG['database']
VIEW = f"{DB}.{TABLES['baseline_stats_30d']}"
BASELINE_VIEW = f"{DB}.{TABLES['baseline_view']}"
HOURLY = f"{DB}.{TABLES['hourly']}"

CREATE_VIEW_SQL = f"""
CREATE OR REPLACE VIEW {VIEW} AS
WITH tenant_max AS (
    -- ts_hour is the customer's local timezone, and different tenants (project_id +
    -- application_id) can be in different timezones - so each tenant's 30-day
    -- window must be anchored to their OWN latest local data, not one global MAX().
    SELECT project_id, application_id, MAX(ts_hour) AS max_ts_hour
    FROM {HOURLY}
    GROUP BY project_id, application_id
),

core AS (
    -- Merge the per-tenant anchor into the baseline row up front, so delta_calc
    -- below only ever joins hourly against ONE source (avoids ClickHouse CTE
    -- resolution issues when a CTE joins two other CTEs sharing column names)
    SELECT b.*, t.max_ts_hour
    FROM {BASELINE_VIEW} b
    INNER JOIN tenant_max t
        ON b.project_id = t.project_id AND b.application_id = t.application_id
),

delta_calc AS (
    SELECT
        h.project_id,
        h.application_id,
        h.service_id,
        h.service,
        h.metric,

        CASE WHEN h.metric = 'success_rate'
             THEN abs(h.success_rate_p50 - c.baseline_value)
             ELSE 0
        END AS delta_success,
        CASE WHEN h.metric = 'latency'
             THEN abs(h.p90_latency - c.baseline_value_p90)
             ELSE 0
        END AS delta_latency,

        h.ts_hour

    FROM {HOURLY} h
    INNER JOIN core c
        ON h.project_id     = c.project_id
       AND h.application_id = c.application_id
       AND h.service        = c.service
       AND h.metric         = c.metric

    WHERE h.ts_hour >= c.max_ts_hour - INTERVAL 30 DAY
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
    logger.info(f"UPDATE {TABLES['baseline_stats_30d']}")
    logger.info("=" * 70)

    try:
        client = clickhouse_connect.get_client(**CLICKHOUSE_CONFIG)
        version = client.command('SELECT version()')
        logger.info(f"Connected to ClickHouse {version}")

        # Recreate the view
        logger.info(f"\nRecreating {TABLES['baseline_stats_30d']} view...")
        client.command(CREATE_VIEW_SQL)
        logger.info("[OK] View recreated successfully")

        # Total rows in the view
        total = client.command(f"SELECT count() FROM {VIEW}")
        logger.info(f"Total rows in view: {total}")

        # Break down by metric to perform a quick sanity test
        logger.info("\nData Overview:")
        result = client.query(
            "SELECT metric, count() as services, "
            "round(avg(delta_median_success), 4) as avg_success_delta, "
            "round(avg(delta_median_latency), 4) as avg_latency_delta "
            f"FROM {VIEW} "
            "GROUP BY metric ORDER BY metric"
        )
        print(f"\n{'metric':<16} {'services':>10} {'avg_success_delta':>20} {'avg_latency_delta':>20}")
        print("-" * 70)
        for row in result.result_rows:
            print(f"{str(row[0]):<16} {str(row[1]):>10} {str(row[2]):>20} {str(row[3]):>20}")

        client.close()
        logger.info(f"\n[OK] Done. {TABLES['baseline_stats_30d']} is up to date.")

    except Exception as e:
        logger.error(f"Failed to update view: {e}")
        raise

if __name__ == "__main__":
    main()
