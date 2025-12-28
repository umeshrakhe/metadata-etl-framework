-- Incremental Load Management Schema
-- This schema supports the IncrementalLoadManager class functionality

-- Watermarks table for tracking incremental load progress
CREATE TABLE IF NOT EXISTS WATERMARKS (
    pipeline_id VARCHAR(255) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    watermark_column VARCHAR(255) NOT NULL DEFAULT 'updated_at',
    watermark_value TEXT, -- Can store timestamps, IDs, or other values
    update_time TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    strategy VARCHAR(50) NOT NULL DEFAULT 'timestamp',
    metadata JSON, -- Additional metadata like timezone, format, etc.
    PRIMARY KEY (pipeline_id, source_id, watermark_column),
    INDEX idx_pipeline_source (pipeline_id, source_id),
    INDEX idx_update_time (update_time),
    INDEX idx_strategy (strategy)
);

-- Incremental metrics table for tracking load performance
CREATE TABLE IF NOT EXISTS INCREMENTAL_METRICS (
    run_id VARCHAR(36) PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    records_extracted INT NOT NULL DEFAULT 0,
    records_inserted INT NOT NULL DEFAULT 0,
    records_updated INT NOT NULL DEFAULT 0,
    records_deleted INT NOT NULL DEFAULT 0,
    duplicates_found INT NOT NULL DEFAULT 0,
    late_arrivals INT NOT NULL DEFAULT 0,
    processing_time_seconds DECIMAL(10,2) NOT NULL DEFAULT 0,
    watermark_before TEXT,
    watermark_after TEXT,
    gaps_detected INT NOT NULL DEFAULT 0,
    validation_errors INT NOT NULL DEFAULT 0,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_size INT,
    memory_usage_mb DECIMAL(10,2),
    cpu_usage_percent DECIMAL(5,2),
    INDEX idx_pipeline_source (pipeline_id, source_id),
    INDEX idx_timestamp (timestamp),
    INDEX idx_run_id (run_id)
);

-- CDC events table for storing change data capture events
CREATE TABLE IF NOT EXISTS CDC_EVENTS (
    event_id VARCHAR(36) PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    source_table VARCHAR(255) NOT NULL,
    operation VARCHAR(10) NOT NULL, -- INSERT, UPDATE, DELETE
    primary_key JSON NOT NULL, -- Key values as JSON
    before_values JSON, -- Old values for UPDATE/DELETE
    after_values JSON, -- New values for INSERT/UPDATE
    commit_timestamp TIMESTAMP NOT NULL,
    transaction_id VARCHAR(255),
    lsn VARCHAR(255), -- Log Sequence Number
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at TIMESTAMP NULL,
    error_message TEXT,
    retry_count INT NOT NULL DEFAULT 0,
    INDEX idx_pipeline_table (pipeline_id, source_table),
    INDEX idx_commit_timestamp (commit_timestamp),
    INDEX idx_operation (operation),
    INDEX idx_processed (processed),
    INDEX idx_lsn (lsn)
);

-- Data gaps table for tracking detected gaps in incremental loads
CREATE TABLE IF NOT EXISTS DATA_GAPS (
    gap_id VARCHAR(36) PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP NOT NULL,
    expected_records INT NOT NULL,
    actual_records INT NOT NULL,
    gap_ratio DECIMAL(10,4) NOT NULL,
    detected_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolved_at TIMESTAMP NULL,
    resolution_notes TEXT,
    INDEX idx_pipeline_source (pipeline_id, source_id),
    INDEX idx_detected_at (detected_at),
    INDEX idx_resolved (resolved)
);

-- SCD history table for Slowly Changing Dimensions
CREATE TABLE IF NOT EXISTS SCD_HISTORY (
    scd_id VARCHAR(36) PRIMARY KEY,
    target_table VARCHAR(255) NOT NULL,
    business_key_hash VARCHAR(64) NOT NULL, -- Hash of business key for performance
    business_key_values JSON NOT NULL, -- Actual business key values
    effective_start_date TIMESTAMP NOT NULL,
    effective_end_date TIMESTAMP NULL,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    change_type VARCHAR(20) NOT NULL, -- NEW, UPDATED, CLOSED
    changed_by VARCHAR(255),
    change_reason VARCHAR(255),
    record_data JSON NOT NULL, -- Complete record data
    INDEX idx_table_key (target_table, business_key_hash),
    INDEX idx_effective_dates (effective_start_date, effective_end_date),
    INDEX idx_current (is_current),
    INDEX idx_change_type (change_type)
);

-- Late arriving data table for handling out-of-order data
CREATE TABLE IF NOT EXISTS LATE_ARRIVING_DATA (
    late_id VARCHAR(36) PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    original_watermark TEXT,
    actual_watermark TEXT,
    record_data JSON NOT NULL,
    arrived_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    processed BOOLEAN NOT NULL DEFAULT FALSE,
    processed_at TIMESTAMP NULL,
    processing_attempts INT NOT NULL DEFAULT 0,
    last_error TEXT,
    INDEX idx_pipeline_source (pipeline_id, source_id),
    INDEX idx_arrived_at (arrived_at),
    INDEX idx_processed (processed)
);

-- Incremental load configuration table
CREATE TABLE IF NOT EXISTS INCREMENTAL_CONFIG (
    config_id VARCHAR(36) PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    incremental_strategy VARCHAR(50) NOT NULL DEFAULT 'timestamp',
    watermark_column VARCHAR(255),
    sequence_column VARCHAR(255),
    partition_column VARCHAR(255),
    cdc_type VARCHAR(50), -- log_based, trigger_based, api_based
    cdc_config JSON, -- CDC-specific configuration
    batch_size INT DEFAULT 10000,
    timeout_seconds INT DEFAULT 3600,
    enable_gap_detection BOOLEAN DEFAULT TRUE,
    enable_duplicate_handling BOOLEAN DEFAULT TRUE,
    expected_interval VARCHAR(50) DEFAULT '1 hour', -- For gap detection
    tolerance_percent DECIMAL(5,2) DEFAULT 5.0, -- Completeness tolerance
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_pipeline_source (pipeline_id, source_id),
    INDEX idx_strategy (incremental_strategy),
    INDEX idx_cdc_type (cdc_type)
);

-- Duplicate records table for tracking and handling duplicates
CREATE TABLE IF NOT EXISTS DUPLICATE_RECORDS (
    duplicate_id VARCHAR(36) PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    key_columns_hash VARCHAR(64) NOT NULL, -- Hash of key columns
    key_values JSON NOT NULL, -- Actual key values
    record_count INT NOT NULL DEFAULT 1,
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    resolution_status VARCHAR(20) DEFAULT 'pending', -- pending, resolved, ignored
    resolved_at TIMESTAMP NULL,
    resolution_method VARCHAR(50), -- first, last, merge, custom
    resolved_record JSON, -- The resolved record data
    INDEX idx_pipeline_source (pipeline_id, source_id),
    INDEX idx_key_hash (key_columns_hash),
    INDEX idx_resolution_status (resolution_status),
    INDEX idx_last_seen (last_seen)
);

-- Insert default incremental configuration
INSERT IGNORE INTO INCREMENTAL_CONFIG (
    config_id, pipeline_id, source_id, incremental_strategy,
    batch_size, timeout_seconds, enable_gap_detection, enable_duplicate_handling
) VALUES (
    'default_config', 'default', 'default', 'timestamp',
    10000, 3600, TRUE, TRUE
);

-- Create additional indexes for performance
CREATE INDEX IF NOT EXISTS idx_watermarks_recent ON WATERMARKS (pipeline_id, source_id, update_time DESC);
CREATE INDEX IF NOT EXISTS idx_metrics_performance ON INCREMENTAL_METRICS (pipeline_id, source_id, processing_time_seconds);
CREATE INDEX IF NOT EXISTS idx_cdc_unprocessed ON CDC_EVENTS (pipeline_id, processed, commit_timestamp) WHERE processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_scd_current_records ON SCD_HISTORY (target_table, is_current) WHERE is_current = TRUE;
CREATE INDEX IF NOT EXISTS idx_late_unprocessed ON LATE_ARRIVING_DATA (pipeline_id, processed, arrived_at) WHERE processed = FALSE;