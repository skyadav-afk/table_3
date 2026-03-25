"""
Create ai_probability table in ClickHouse with the specified schema and sample data.
"""

import clickhouse_connect
import logging
from datetime import datetime


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
DROP_TABLE_SQL = "DROP TABLE IF EXISTS ai_probability"

# Create table SQL
CREATE_TABLE_SQL = """
CREATE TABLE ai_probability
(
    service String,
    service_id UInt32,
    recurrence_probability Float64,
    service_criticality Float64,
    weighted_risk Float64,
    detected_at DateTime DEFAULT now()
)
ENGINE = MergeTree()
ORDER BY (service_id, service)
SETTINGS index_granularity = 8192
"""

# Insert sample data


def create_ai_probability_table():
    """Drop and recreate the ai_probability table and insert sample data"""
    try:
        logger.info("=" * 80)
        logger.info("Initializing ai_probability Table")
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
        logger.info("\nCreating new table with the following schema:")
        logger.info("- service: String")
        logger.info("- service_id: UInt32")
        logger.info("- recurrence_probability: Float64")
        logger.info("- service_criticality: Float64")
        logger.info("- weighted_risk: Float64")
        
        client.command(CREATE_TABLE_SQL)
        logger.info("✓ Table created successfully")
        
        # Verify table structure
        logger.info("\nVerifying table structure...")
        result = client.query("DESCRIBE TABLE ai_probability")
        
        logger.info("\n✓ Table Structure:")
        logger.info("-" * 80)
        logger.info(f"{'Column Name':<25} {'Type':<20} {'Default':<30}")
        logger.info("-" * 80)
        
        for row in result.result_rows:
            col_name = row[0]
            col_type = row[1]
            col_default = row[2] if len(row) > 2 else ''
            logger.info(f"{col_name:<25} {col_type:<20} {col_default:<30}")
        
        # Close connection
        client.close()
        logger.info("\n✓ Connection closed")
        
        logger.info("\n" + "=" * 80)
        logger.info("AI PROBABILITY TABLE CREATED SUCCESSFULLY (EMPTY)")
        logger.info("=" * 80)
        
        return True
        
    except Exception as e:
        logger.error(f"\n❌ Failed to process: {str(e)}")
        raise

if __name__ == "__main__":
    create_ai_probability_table()
