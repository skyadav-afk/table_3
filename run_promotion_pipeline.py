"""
Complete Promotion Pipeline Script
Orchestrates the entire pattern promotion workflow:
1. Fetches data from ClickHouse (staging, baseline, hourly)
2. Runs promotion logic for daily and weekly patterns
3. Combines results and generates final output
4. Writes promoted patterns to ClickHouse table
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import clickhouse_connect

# Import data fetching functions
from fetch_data import fetch_data_to_dataframe, fetch_baseline_data, fetch_baseline_30d_data, fetch_hourly_data

# Import promotion logic
from daily_weekly import promote_seasonality, get_baseline, median_delta, volume_ok, classify_baseline
from drift import promote_drift
from volume import promote_volume

# Import configuration
from config import CONFIG

# ClickHouse connection configuration (same as fetch_data.py)
CLICKHOUSE_CONFIG = {
    'host': 'ec2-47-129-241-41.ap-southeast-1.compute.amazonaws.com',
    'port': 8123,
    'database': 'metrics',
    'username': 'wm_test',
    'password': 'Watermelon@123'
}

TARGET_TABLE = 'ai_service_behavior_memory'

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def write_to_clickhouse(promoted_df):
    """
    Write promoted patterns DataFrame to ClickHouse table ai_service_behavior_memory

    Args:
        promoted_df (pandas.DataFrame): DataFrame containing promoted patterns

    Returns:
        bool: True if write was successful
    """
    try:
        if len(promoted_df) == 0:
            logger.warning("No patterns to write to ClickHouse (DataFrame is empty)")
            return False

        logger.info("\n" + "=" * 80)
        logger.info("Writing Promoted Patterns to ClickHouse")
        logger.info("=" * 80)

        # Use the DataFrame as-is (no additional columns needed)
        df_to_write = promoted_df.copy()

        logger.info(f"\nPreparing to write {len(df_to_write)} rows to {TARGET_TABLE}...")
        logger.info(f"Columns to write: {', '.join(df_to_write.columns.tolist())}")

        # Create ClickHouse client
        logger.info(f"\nConnecting to ClickHouse at {CLICKHOUSE_CONFIG['host']}:{CLICKHOUSE_CONFIG['port']}...")
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_CONFIG['host'],
            port=CLICKHOUSE_CONFIG['port'],
            database=CLICKHOUSE_CONFIG['database'],
            username=CLICKHOUSE_CONFIG['username'],
            password=CLICKHOUSE_CONFIG['password']
        )

        # Test connection
        version = client.command('SELECT version()')
        logger.info(f"✓ Connected to ClickHouse version: {version}")

        # Insert DataFrame into ClickHouse table
        logger.info(f"\nInserting data into {TARGET_TABLE}...")
        client.insert_df(TARGET_TABLE, df_to_write)

        logger.info(f"✓ Successfully wrote {len(df_to_write)} rows to {TARGET_TABLE}")

        # Verify the insert
        count_query = f"SELECT COUNT(*) FROM {TARGET_TABLE}"
        total_count = client.command(count_query)
        logger.info(f"✓ Verified: Total rows in table: {total_count}")

        # Close connection
        client.close()
        logger.info("✓ Connection closed")

        return True

    except Exception as e:
        logger.error(f"\n❌ Failed to write to ClickHouse: {str(e)}")
        raise


def run_promotion_pipeline():
    """
    Main pipeline function that orchestrates the complete promotion workflow
    
    Returns:
        pandas.DataFrame: Final promoted patterns DataFrame
    """
    try:
        logger.info("=" * 80)
        logger.info("STARTING PATTERN PROMOTION PIPELINE")
        logger.info("=" * 80)
        
        # Step 1: Fetch all required data
        logger.info("\n" + "=" * 80)
        logger.info("STEP 1: Fetching Data from ClickHouse")
        logger.info("=" * 80)

        logger.info("\n[1/4] Fetching staging data from ai_detector_staging1...")
        staging_df = fetch_data_to_dataframe()
        logger.info(f"✓ Staging data loaded: {staging_df.shape[0]} rows, {staging_df.shape[1]} columns")

        logger.info("\n[2/4] Fetching baseline data from ai_baseline_view_2...")
        baseline_df = fetch_baseline_data()
        logger.info(f"✓ Baseline data loaded: {baseline_df.shape[0]} rows, {baseline_df.shape[1]} columns")

        logger.info("\n[3/4] Fetching 30-day baseline data from ai_baseline_stats_30d...")
        baseline_30d_df = fetch_baseline_30d_data()
        logger.info(f"✓ 30-day baseline data loaded: {baseline_30d_df.shape[0]} rows, {baseline_30d_df.shape[1]} columns")

        logger.info("\n[4/4] Fetching hourly data from service_metrics_hourly_2...")
        hourly_df = fetch_hourly_data()
        logger.info(f"✓ Hourly data loaded: {hourly_df.shape[0]} rows, {hourly_df.shape[1]} columns")

        # Step 2: Promote daily patterns
        logger.info("\n" + "=" * 80)
        logger.info("STEP 2: Promoting Daily Patterns")
        logger.info("=" * 80)

        logger.info("\nRunning daily pattern promotion logic...")
        daily_promoted_df = promote_seasonality(
            staging_df=staging_df,
            baseline_df=baseline_df,
            baseline_30d_df=baseline_30d_df,
            hourly_df=hourly_df,
            mode="daily_candidate"
        )
        logger.info(f"✓ Daily patterns promoted: {len(daily_promoted_df)} patterns")

        # Step 3: Promote weekly patterns
        logger.info("\n" + "=" * 80)
        logger.info("STEP 3: Promoting Weekly Patterns")
        logger.info("=" * 80)

        logger.info("\nRunning weekly pattern promotion logic...")
        weekly_promoted_df = promote_seasonality(
            staging_df=staging_df,
            baseline_df=baseline_df,
            baseline_30d_df=baseline_30d_df,
            hourly_df=hourly_df,
            mode="weekly_candidate"
        )
        logger.info(f"✓ Weekly patterns promoted: {len(weekly_promoted_df)} patterns")

        # Step 4: Promote drift patterns
        logger.info("\n" + "=" * 80)
        logger.info("STEP 4: Promoting Drift Patterns")
        logger.info("=" * 80)

        logger.info("\nRunning drift pattern detection logic...")
        drift_promoted_df = promote_drift(
            baseline_df=baseline_df,
            baseline_30d_df=baseline_30d_df,
            hourly_df=hourly_df
        )
        logger.info(f"✓ Drift patterns promoted: {len(drift_promoted_df)} patterns")

        # Step 5: Promote volume-driven patterns
        logger.info("\n" + "=" * 80)
        logger.info("STEP 5: Promoting Volume-Driven Patterns")
        logger.info("=" * 80)

        logger.info("\nRunning volume-driven pattern detection logic...")
        volume_promoted_df = promote_volume(
            baseline_df=baseline_df,
            baseline_30d_df=baseline_30d_df,
            hourly_df=hourly_df
        )
        logger.info(f"✓ Volume-driven patterns promoted: {len(volume_promoted_df)} patterns")

        # Step 6: Combine results
        logger.info("\n" + "=" * 80)
        logger.info("STEP 6: Combining Results")
        logger.info("=" * 80)

        logger.info("\nCombining daily, weekly, drift, and volume-driven promoted patterns...")
        promoted_df = pd.concat([daily_promoted_df, weekly_promoted_df, drift_promoted_df, volume_promoted_df], ignore_index=True)
        logger.info(f"✓ Total promoted patterns: {len(promoted_df)} patterns")

        # Step 7: Write to ClickHouse
        logger.info("\n" + "=" * 80)
        logger.info("STEP 7: Writing to ClickHouse")
        logger.info("=" * 80)

        write_success = write_to_clickhouse(promoted_df)

        # Step 8: Display summary
        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE SUMMARY")
        logger.info("=" * 80)
        
        print(f"\n{'='*80}")
        print(f"PROMOTION PIPELINE RESULTS")
        print(f"{'='*80}")
        print(f"\n📊 Data Loaded:")
        print(f"   - Staging records: {staging_df.shape[0]:,}")
        print(f"   - Baseline records: {baseline_df.shape[0]:,}")
        print(f"   - Hourly records: {hourly_df.shape[0]:,}")
        
        print(f"\n🎯 Patterns Promoted:")
        print(f"   - Daily patterns: {len(daily_promoted_df):,}")
        print(f"   - Weekly patterns: {len(weekly_promoted_df):,}")
        print(f"   - Drift patterns: {len(drift_promoted_df):,}")
        print(f"   - Volume-driven patterns: {len(volume_promoted_df):,}")
        print(f"   - Total promoted: {len(promoted_df):,}")
        
        if len(promoted_df) > 0:
            print(f"\n📈 Promoted Patterns Breakdown:")
            print(f"   - Pattern types: {promoted_df['pattern_type'].value_counts().to_dict()}")
            print(f"   - Baseline states: {promoted_df['baseline_state'].value_counts().to_dict()}")
            print(f"   - Unique applications: {promoted_df['application_id'].nunique()}")
            print(f"   - Unique services: {promoted_df['service'].nunique()}")

            print(f"\n💾 ClickHouse Write Status:")
            print(f"   - Write successful: {'✓ Yes' if write_success else '✗ No'}")
            print(f"   - Target table: {TARGET_TABLE}")

            print(f"\n📋 Promoted DataFrame Columns:")
            for i, col in enumerate(promoted_df.columns, 1):
                print(f"   {i}. {col}")

            print(f"\n🔍 First 5 Promoted Patterns:")
            print(promoted_df.head().to_string())

            print(f"\n📊 DataFrame Info:")
            promoted_df.info()
        else:
            print(f"\n⚠️  No patterns were promoted based on current criteria")

        logger.info("\n" + "=" * 80)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("=" * 80)
        
        return promoted_df
        
    except Exception as e:
        logger.error(f"\n❌ Pipeline failed: {str(e)}")
        raise


if __name__ == "__main__":
    # Run the complete promotion pipeline
    promoted_patterns = run_promotion_pipeline()

    # Optionally save to CSV for backup/analysis
    if len(promoted_patterns) > 0:
        output_file = f"promoted_patterns_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        promoted_patterns.to_csv(output_file, index=False)
        logger.info(f"\n💾 Backup CSV saved to: {output_file}")
        print(f"\n💾 Backup CSV saved to: {output_file}")
