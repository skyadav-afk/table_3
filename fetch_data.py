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
HOURLY_TABLE = 'service_metrics_hourly_2'  # Will try this first, fallback to ai_service_features_hourly


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

        # Build query - fetch all rows (no limit)
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

        # Build query for hourly data
        query = f"""
        SELECT
            application_id,
            service,
            metric,
            ts_hour,
            success_rate_p50,
            p90_latency,
            total_requests
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

        # Fetch hourly data
        logger.info("\n" + "=" * 60)
        logger.info("TASK 3: Fetching Hourly Data from service_metrics_hourly_2")
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

        # Summary
        logger.info("\n" + "=" * 60)
        logger.info("SUMMARY")
        logger.info("=" * 60)
        print(f"\n✓ Staging Data Shape: {staging_df.shape}")
        print(f"✓ Baseline Data Shape: {baseline_df.shape}")
        print(f"✓ Hourly Data Shape: {hourly_df.shape}")

        logger.info("\n" + "=" * 60)
        logger.info("All data fetches completed successfully!")
        logger.info("=" * 60)

        return staging_df, baseline_df, hourly_df

    except Exception as e:
        logger.error(f"Failed to fetch data: {str(e)}")
        raise


if __name__ == "__main__":
    staging_df, baseline_df, hourly_df = main()

