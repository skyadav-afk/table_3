"""
ClickHouse Data Fetcher using clickhouse-connect
Fetches data from ai_detector_staging1 table into a pandas DataFrame
"""

import pandas as pd
import clickhouse_connect
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# ClickHouse connection configuration
CLICKHOUSE_CONFIG = {
    'host': 'ec2-47-129-241-41.ap-southeast-1.compute.amazonaws.com',
    'port': 8123,  # HTTP protocol port
    'database': 'metrics',
    'username': 'wm_test',
    'password': 'Watermelon@123'
}

TABLE_NAME = 'ai_detector_staging1'
BASELINE_VIEW = 'ai_baseline_view_2'
BASELINE_VIEW_30D = 'ai_baseline_stats_30d'
HOURLY_TABLE = 'ai_service_features_hourly'  # Will try this first, fallback to ai_service_features_hourly
METRICS_5M_TABLE = 'ai_metrics_5m_v2'  # 5-minute metrics table for volume.py


def fetch_data_to_dataframe():
    """
    Connect to ClickHouse and fetch all data from ai_detector_staging1 table

    Returns:
        pandas.DataFrame: DataFrame containing the fetched data
    """
    try:
        logger.info("Connecting to ClickHouse server...")

        # Create ClickHouse client using HTTP interface
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIG['host'],
            port=CLICKHOUSE_CONFIG['port'],
            database=CLICKHOUSE_CONFIG['database'],
            username=CLICKHOUSE_CONFIG['username'],
            password=CLICKHOUSE_CONFIG['password']
        )

        # Test connection
        version = client.command('SELECT version()')
        logger.info(f"Successfully connected to ClickHouse version: {version}")

        # Build query - fetch all rows (no limit) including service_id
        query = f"SELECT * FROM {TABLE_NAME}"
        logger.info(f"Executing query: {query}")

        # Execute query and get data as pandas DataFrame
        df = client.query_df(query)

        logger.info(f"Query executed successfully. Fetched {len(df)} rows with {len(df.columns)} columns")
        logger.info(f"Columns: {', '.join(df.columns.tolist())}")

        logger.info(f"DataFrame created successfully with shape: {df.shape}")

        # Close connection
        client.close()
        logger.info("Connection closed")

        return df

    except Exception as e:
        logger.error(f"Error fetching data: {str(e)}")
        raise


def fetch_baseline_data():
    """
    Connect to ClickHouse and fetch baseline data from ai_baseline_view_2

    Returns:
        pandas.DataFrame: DataFrame containing the baseline data with columns:
                         application_id, service, metric, baseline_value,
                         baseline_value_p90, median_hour_volumne, breach_ratio
    """
    try:
        logger.info("Connecting to ClickHouse server for baseline data...")

        # Create ClickHouse client using HTTP interface
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIG['host'],
            port=CLICKHOUSE_CONFIG['port'],
            database=CLICKHOUSE_CONFIG['database'],
            username=CLICKHOUSE_CONFIG['username'],
            password=CLICKHOUSE_CONFIG['password']
        )

        # Test connection
        version = client.command('SELECT version()')
        logger.info(f"Successfully connected to ClickHouse version: {version}")

        # Build query for baseline data
        query = f"""
        SELECT
            project_id,
            application_id,
            service,
            metric,
            baseline_value,
            baseline_value_p90,
            median_hour_volumne,
            breach_ratio
        FROM {BASELINE_VIEW}
        """
        logger.info(f"Executing query on {BASELINE_VIEW}...")

        # Execute query and get data as pandas DataFrame
        df = client.query_df(query)

        logger.info(f"Query executed successfully. Fetched {len(df)} rows with {len(df.columns)} columns")
        logger.info(f"Columns: {', '.join(df.columns.tolist())}")

        logger.info(f"Baseline DataFrame created successfully with shape: {df.shape}")

        # Close connection
        client.close()
        logger.info("Connection closed")

        return df

    except Exception as e:
        logger.error(f"Error fetching baseline data: {str(e)}")
        raise


def fetch_baseline_30d_data():
    """
    Connect to ClickHouse and fetch 30-day baseline data from ai_baseline_stats_30d
    Fetches ALL columns and rows from the view

    Returns:
        pandas.DataFrame: DataFrame containing all columns and rows from ai_baseline_stats_30d
    """
    try:
        logger.info("Connecting to ClickHouse server for 30-day baseline data...")

        # Create ClickHouse client using HTTP interface
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIG['host'],
            port=CLICKHOUSE_CONFIG['port'],
            database=CLICKHOUSE_CONFIG['database'],
            username=CLICKHOUSE_CONFIG['username'],
            password=CLICKHOUSE_CONFIG['password']
        )

        # Test connection
        version = client.command('SELECT version()')
        logger.info(f"Successfully connected to ClickHouse version: {version}")

        # Build query for 30-day baseline data - fetch ALL columns and rows
        query = f"SELECT * FROM {BASELINE_VIEW_30D}"
        logger.info(f"Executing query on {BASELINE_VIEW_30D}...")

        # Execute query and get data as pandas DataFrame
        df = client.query_df(query)

        logger.info(f"Query executed successfully. Fetched {len(df)} rows with {len(df.columns)} columns")
        logger.info(f"Columns: {', '.join(df.columns.tolist())}")

        logger.info(f"30-day Baseline DataFrame created successfully with shape: {df.shape}")

        # Close connection
        client.close()
        logger.info("Connection closed")

        return df

    except Exception as e:
        logger.error(f"Error fetching 30-day baseline data: {str(e)}")
        raise


def fetch_hourly_data():
    """
    Connect to ClickHouse and fetch hourly metrics data

    Returns:
        pandas.DataFrame: DataFrame containing hourly metrics with columns:
                         application_id, service, metric, ts_hour,
                         success_rate_p50, p90_latency, total_requests
    """
    try:
        logger.info("Connecting to ClickHouse server for hourly data...")

        # Create ClickHouse client using HTTP interface
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIG['host'],
            port=CLICKHOUSE_CONFIG['port'],
            database=CLICKHOUSE_CONFIG['database'],
            username=CLICKHOUSE_CONFIG['username'],
            password=CLICKHOUSE_CONFIG['password']
        )

        # Test connection
        version = client.command('SELECT version()')
        logger.info(f"Successfully connected to ClickHouse version: {version}")

        # Try to determine which table exists
        table_to_use = None
        for table_name in [HOURLY_TABLE, 'ai_service_features_hourly']:
            try:
                check_query = f"SELECT 1 FROM {table_name} LIMIT 1"
                client.command(check_query)
                table_to_use = table_name
                logger.info(f"Found table: {table_name}")
                break
            except Exception:
                continue

        if not table_to_use:
            raise Exception("Neither service_metrics_hourly_2 nor ai_service_features_hourly table found")

        # Build query for hourly data - include service_id
        query = f"""
        SELECT
            project_id,
            application_id,
            service_id,
            service,
            metric,
            ts_hour,
            success_rate_p50,
            p90_latency,
            total_requests,
            total_windows,
            bad_windows,
            breach_ratio,
            hour,
            day_of_week
        FROM {table_to_use}
        """
        logger.info(f"Executing query on {table_to_use}...")

        # Execute query and get data as pandas DataFrame
        df = client.query_df(query)

        logger.info(f"Query executed successfully. Fetched {len(df)} rows with {len(df.columns)} columns")
        logger.info(f"Columns: {', '.join(df.columns.tolist())}")

        logger.info(f"Hourly DataFrame created successfully with shape: {df.shape}")

        # Close connection
        client.close()
        logger.info("Connection closed")

        return df

    except Exception as e:
        logger.error(f"Error fetching hourly data: {str(e)}")
        raise


def fetch_5m_data():
    """
    Connect to ClickHouse and fetch 5-minute metrics data from ai_metrics_5m_v2
    This data is used specifically for volume.py pattern detection

    Returns:
        pandas.DataFrame: DataFrame containing 5-minute metrics with columns:
                         application_id, service, ts (timestamp),
                         success_rate, p90_latency, total_count,
                         day_of_week, hour, minute_bucket

    Note: The table ai_metrics_5m_v2 does not have a 'metric' column.
          We need to create separate rows for success_rate and latency metrics.
    """
    try:
        logger.info("Connecting to ClickHouse server for 5-minute metrics data...")

        # Create ClickHouse client using HTTP interface
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIG['host'],
            port=CLICKHOUSE_CONFIG['port'],
            database=CLICKHOUSE_CONFIG['database'],
            username=CLICKHOUSE_CONFIG['username'],
            password=CLICKHOUSE_CONFIG['password']
        )

        # Test connection
        version = client.command('SELECT version()')
        logger.info(f"Successfully connected to ClickHouse version: {version}")

        # Build query for 5-minute metrics data - include service_id if available
        # Note: ai_metrics_5m_v2 has 'ts' instead of 'ts_hour', and no 'metric' column
        # Try to include service_id, but handle gracefully if it doesn't exist
        try:
            # First check if service_id column exists
            check_query = f"SELECT * FROM {METRICS_5M_TABLE} LIMIT 1"
            test_df = client.query_df(check_query)
            has_service_id = 'service_id' in test_df.columns
        except Exception:
            has_service_id = False

        if has_service_id:
            query = f"""
            SELECT
                project_id,
                application_id,
                service_id,
                service,
                ts,
                success_rate,
                p90_latency,
                total_count,
                day_of_week,
                hour,
                minute_bucket
            FROM {METRICS_5M_TABLE}
            """
        else:
            query = f"""
            SELECT
                project_id,
                application_id,
                service,
                ts,
                success_rate,
                p90_latency,
                total_count,
                day_of_week,
                hour,
                minute_bucket
            FROM {METRICS_5M_TABLE}
            """
        logger.info(f"Executing query on {METRICS_5M_TABLE}... (service_id: {has_service_id})")

        # Execute query and get data as pandas DataFrame
        df = client.query_df(query)

        logger.info(f"Query executed successfully. Fetched {len(df)} rows with {len(df.columns)} columns")
        logger.info(f"Columns: {', '.join(df.columns.tolist())}")

        # Rename 'ts' to 'ts_hour' for consistency with other data sources
        df = df.rename(columns={'ts': 'ts_hour'})

        logger.info(f"5-Minute Metrics DataFrame created successfully with shape: {df.shape}")

        # Close connection
        client.close()
        logger.info("Connection closed")

        return df

    except Exception as e:
        logger.error(f"Error fetching 5-minute metrics data: {str(e)}")
        raise


def main():
    """
    Main function to test the data fetching for staging, baseline, and hourly data
    """
    try:
        # Fetch staging data
        logger.info("=" * 60)
        logger.info("TASK 1: Fetching Staging Data from ai_detector_staging1")
        logger.info("=" * 60)

        staging_df = fetch_data_to_dataframe()

        # Print staging DataFrame information
        logger.info("\n" + "=" * 60)
        logger.info("STAGING DATA RESULTS")
        logger.info("=" * 60)

        print(f"\n✓ Staging DataFrame Shape: {staging_df.shape}")
        print(f"  - Rows: {staging_df.shape[0]}")
        print(f"  - Columns: {staging_df.shape[1]}")

        print(f"\n✓ Staging Column Names:")
        for i, col in enumerate(staging_df.columns, 1):
            print(f"  {i}. {col}")

        print(f"\n✓ Staging Data Types:")
        print(staging_df.dtypes)

        print(f"\n✓ Staging First 5 Rows:")
        print(staging_df.head())

        print(f"\n✓ Staging DataFrame Info:")
        staging_df.info()

        # Fetch baseline data
        logger.info("\n" + "=" * 60)
        logger.info("TASK 2: Fetching Baseline Data from ai_baseline_view_2")
        logger.info("=" * 60)

        baseline_df = fetch_baseline_data()

        # Print baseline DataFrame information
        logger.info("\n" + "=" * 60)
        logger.info("BASELINE DATA RESULTS")
        logger.info("=" * 60)

        print(f"\n✓ Baseline DataFrame Shape: {baseline_df.shape}")
        print(f"  - Rows: {baseline_df.shape[0]}")
        print(f"  - Columns: {baseline_df.shape[1]}")

        print(f"\n✓ Baseline Column Names:")
        for i, col in enumerate(baseline_df.columns, 1):
            print(f"  {i}. {col}")

        print(f"\n✓ Baseline Data Types:")
        print(baseline_df.dtypes)

        print(f"\n✓ Baseline First 5 Rows:")
        print(baseline_df.head())

        print(f"\n✓ Baseline DataFrame Info:")
        baseline_df.info()

        # Fetch 30-day baseline data
        logger.info("\n" + "=" * 60)
        logger.info("TASK 3: Fetching 30-Day Baseline Data from ai_baseline_stats_30d")
        logger.info("=" * 60)

        baseline_30d_df = fetch_baseline_30d_data()

        # Print 30-day baseline DataFrame information
        logger.info("\n" + "=" * 60)
        logger.info("30-DAY BASELINE DATA RESULTS")
        logger.info("=" * 60)

        print(f"\n✓ 30-Day Baseline DataFrame Shape: {baseline_30d_df.shape}")
        print(f"  - Rows: {baseline_30d_df.shape[0]}")
        print(f"  - Columns: {baseline_30d_df.shape[1]}")

        print(f"\n✓ 30-Day Baseline Column Names:")
        for i, col in enumerate(baseline_30d_df.columns, 1):
            print(f"  {i}. {col}")

        print(f"\n✓ 30-Day Baseline Data Types:")
        print(baseline_30d_df.dtypes)

        print(f"\n✓ 30-Day Baseline First 5 Rows:")
        print(baseline_30d_df.head())

        print(f"\n✓ 30-Day Baseline DataFrame Info:")
        baseline_30d_df.info()

        # Fetch hourly data
        logger.info("\n" + "=" * 60)
        logger.info("TASK 4: Fetching Hourly Data from service_metrics_hourly_2")
        logger.info("=" * 60)

        hourly_df = fetch_hourly_data()

        # Print hourly DataFrame information
        logger.info("\n" + "=" * 60)
        logger.info("HOURLY DATA RESULTS")
        logger.info("=" * 60)

        print(f"\n✓ Hourly DataFrame Shape: {hourly_df.shape}")
        print(f"  - Rows: {hourly_df.shape[0]}")
        print(f"  - Columns: {hourly_df.shape[1]}")

        print(f"\n✓ Hourly Column Names:")
        for i, col in enumerate(hourly_df.columns, 1):
            print(f"  {i}. {col}")

        print(f"\n✓ Hourly Data Types:")
        print(hourly_df.dtypes)

        print(f"\n✓ Hourly First 5 Rows:")
        print(hourly_df.head())

        print(f"\n✓ Hourly DataFrame Info:")
        hourly_df.info()

        # Fetch 5-minute metrics data
        logger.info("\n" + "=" * 60)
        logger.info("TASK 5: Fetching 5-Minute Metrics Data from ai_metrics_5m_v2")
        logger.info("=" * 60)

        metrics_5m_df = fetch_5m_data()

        # Print 5-minute metrics DataFrame information
        logger.info("\n" + "=" * 60)
        logger.info("5-MINUTE METRICS DATA RESULTS")
        logger.info("=" * 60)

        print(f"\n✓ 5-Minute Metrics DataFrame Shape: {metrics_5m_df.shape}")
        print(f"  - Rows: {metrics_5m_df.shape[0]}")
        print(f"  - Columns: {metrics_5m_df.shape[1]}")

        print(f"\n✓ 5-Minute Metrics Column Names:")
        for i, col in enumerate(metrics_5m_df.columns, 1):
            print(f"  {i}. {col}")

        print(f"\n✓ 5-Minute Metrics Data Types:")
        print(metrics_5m_df.dtypes)

        print(f"\n✓ 5-Minute Metrics First 5 Rows:")
        print(metrics_5m_df.head())

        print(f"\n✓ 5-Minute Metrics DataFrame Info:")
        metrics_5m_df.info()

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        print(f"\n✓ Staging Data Shape: {staging_df.shape}")
        print(f"✓ Baseline Data Shape: {baseline_df.shape}")
        print(f"✓ 30-Day Baseline Data Shape: {baseline_30d_df.shape}")
        print(f"✓ Hourly Data Shape: {hourly_df.shape}")
        print(f"✓ 5-Minute Metrics Data Shape: {metrics_5m_df.shape}")

        logger.info("\n" + "=" * 60)
        logger.info("All data fetches completed successfully!")
        logger.info("=" * 60)

        return staging_df, baseline_df, baseline_30d_df, hourly_df, metrics_5m_df

    except Exception as e:
        logger.error(f"Failed to fetch data: {str(e)}")
        raise


if __name__ == "__main__":
    staging_df, baseline_df, baseline_30d_df, hourly_df, metrics_5m_df = main()
