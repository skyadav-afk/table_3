"""
Drop and recreate ai_service_behavior_memory table with ONLY the required columns
"""

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
    'port': 8123,
    'database': 'metrics',
    'username': 'wm_test',
    'password': 'Watermelon@123'
}

# Drop table SQL
DROP_TABLE_SQL = "DROP TABLE IF EXISTS ai_service_behavior_memory"

# Create table SQL - ONLY the exact columns from daily_weekly.py (NO memory management columns)
CREATE_TABLE_SQL = """
CREATE TABLE ai_service_behavior_memory
(
    -- Service identifiers
    application_id UInt32,
    service String,
    metric String,

    -- Baseline information
    baseline_state String,
    baseline_value Float64,

    -- Pattern information
    pattern_type String,
    pattern_window String,

    -- Delta metrics
    delta_success Float64,
    delta_latency_p90 Float64,

    -- Pattern confidence metrics
    support_days UInt16,
    confidence Float64,

    -- Temporal tracking (with time)
    first_seen DateTime,
    last_seen DateTime,
    detected_at DateTime
)
ENGINE = MergeTree()
ORDER BY (application_id, service, metric, pattern_type, pattern_window)
SETTINGS index_granularity = 8192
"""


def recreate_table():
    """Drop and recreate the ai_service_behavior_memory table"""
    try:
        logger.info("=" * 80)
        logger.info("Recreating ai_service_behavior_memory Table")
        logger.info("=" * 80)
        
        # Connect to ClickHouse
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
        
        # Drop existing table
        logger.info("\nDropping existing table (if exists)...")
        client.command(DROP_TABLE_SQL)
        logger.info("✓ Table dropped successfully")
        
        # Create new table
        logger.info("\nCreating new table with correct schema...")
        client.command(CREATE_TABLE_SQL)
        logger.info("✓ Table created successfully")
        
        # Verify table structure
        logger.info("\nVerifying table structure...")
        result = client.query("DESCRIBE TABLE ai_service_behavior_memory")
        
        logger.info("\n✓ Table Structure:")
        logger.info("-" * 80)
        logger.info(f"{'Column Name':<25} {'Type':<20} {'Default':<30}")
        logger.info("-" * 80)
        
        for row in result.result_rows:
            col_name = row[0]
            col_type = row[1]
            col_default = row[2] if len(row) > 2 else ''
            logger.info(f"{col_name:<25} {col_type:<20} {col_default:<30}")
        
        logger.info("-" * 80)
        logger.info(f"\nTotal columns: {len(result.result_rows)}")
        
        # Close connection
        client.close()
        logger.info("\n✓ Connection closed")
        
        logger.info("\n" + "=" * 80)
        logger.info("TABLE RECREATED SUCCESSFULLY")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ Failed to recreate table: {str(e)}")
        raise


if __name__ == "__main__":
    recreate_table()

