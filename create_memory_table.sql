-- Create ai_service_behavior_memory1 table
-- This table stores promoted seasonal patterns for service behavior tracking

CREATE TABLE IF NOT EXISTS ai_service_behavior_memory1
(
    -- Service identifiers
    application_id String,
    service String,
    metric String,
    
    -- Baseline information
    baseline_state String,  -- HEALTHY, AT_RISK, CHRONIC
    baseline_value Float64,
    
    -- Pattern information
    pattern_type String,  -- daily, weekly
    pattern_window String,  -- e.g., "Monday 14-15"
    
    -- Delta metrics
    delta_success Float64,
    delta_latency_p90 Float64,
    
    -- Pattern confidence metrics
    support_days Int32,
    confidence Float64,
    
    -- Temporal tracking
    first_seen Date,
    last_seen Date,
    detected_at DateTime,
    
    -- Memory management
    created_at DateTime DEFAULT now(),
    ttl_days Int32,
    expires_at DateTime
)
ENGINE = MergeTree()
ORDER BY (application_id, service, metric, pattern_type, pattern_window)
TTL expires_at
SETTINGS index_granularity = 8192;

