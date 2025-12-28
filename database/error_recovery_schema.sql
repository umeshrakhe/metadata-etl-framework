-- Error Recovery and Retry Logic Schema
-- This schema supports the ErrorRecoveryManager class functionality

-- Error details table for comprehensive error logging
CREATE TABLE IF NOT EXISTS ERROR_DETAILS (
    error_id VARCHAR(36) PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    pipeline_id VARCHAR(255) NOT NULL,
    step VARCHAR(255) NOT NULL,
    error_type VARCHAR(20) NOT NULL, -- transient, config, data, system, unknown
    error_message TEXT NOT NULL,
    stack_trace TEXT,
    context JSON,
    retry_count INT NOT NULL DEFAULT 0,
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolution_action VARCHAR(255),
    INDEX idx_run_id (run_id),
    INDEX idx_pipeline_id (pipeline_id),
    INDEX idx_error_type (error_type),
    INDEX idx_timestamp (timestamp),
    INDEX idx_resolved (resolved)
);

-- Checkpoints table for progress recovery
CREATE TABLE IF NOT EXISTS CHECKPOINTS (
    checkpoint_id VARCHAR(36) PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    pipeline_id VARCHAR(255) NOT NULL,
    step VARCHAR(255) NOT NULL,
    checkpoint_data JSON NOT NULL,
    checkpoint_type VARCHAR(20) NOT NULL, -- batch_level, row_level, time_based, custom
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    batch_number INT,
    row_number BIGINT,
    INDEX idx_run_id (run_id),
    INDEX idx_pipeline_id (pipeline_id),
    INDEX idx_step (step),
    INDEX idx_timestamp (timestamp),
    INDEX idx_checkpoint_type (checkpoint_type)
);

-- Quarantined records table for problematic data isolation
CREATE TABLE IF NOT EXISTS QUARANTINED_RECORDS (
    quarantine_id VARCHAR(36) NOT NULL,
    run_id VARCHAR(255) NOT NULL,
    pipeline_id VARCHAR(255) NOT NULL,
    record_data JSON NOT NULL,
    error_reason TEXT NOT NULL,
    error_type VARCHAR(20) NOT NULL,
    quarantined_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retry_count INT NOT NULL DEFAULT 0,
    resolved BOOLEAN NOT NULL DEFAULT FALSE,
    resolution_action VARCHAR(255),
    INDEX idx_quarantine_id (quarantine_id),
    INDEX idx_run_id (run_id),
    INDEX idx_pipeline_id (pipeline_id),
    INDEX idx_error_type (error_type),
    INDEX idx_quarantined_at (quarantined_at),
    INDEX idx_resolved (resolved)
);

-- Circuit breaker states table for service health tracking
CREATE TABLE IF NOT EXISTS CIRCUIT_BREAKER_STATES (
    service_name VARCHAR(255) PRIMARY KEY,
    failure_count INT NOT NULL DEFAULT 0,
    last_failure_time TIMESTAMP NULL,
    state VARCHAR(20) NOT NULL DEFAULT 'closed', -- closed, open, half_open
    next_attempt_time TIMESTAMP NULL,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_state (state),
    INDEX idx_next_attempt (next_attempt_time)
);

-- Error patterns table for trend analysis
CREATE TABLE IF NOT EXISTS ERROR_PATTERNS (
    pattern_id VARCHAR(36) PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    error_type VARCHAR(20) NOT NULL,
    error_message_pattern VARCHAR(500) NOT NULL,
    frequency INT NOT NULL DEFAULT 1,
    first_seen TIMESTAMP NOT NULL,
    last_seen TIMESTAMP NOT NULL,
    avg_resolution_time DECIMAL(10,2),
    recommended_action TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    UNIQUE KEY uk_pattern (pipeline_id, error_type, error_message_pattern(100)),
    INDEX idx_pipeline_id (pipeline_id),
    INDEX idx_error_type (error_type),
    INDEX idx_frequency (frequency),
    INDEX idx_last_seen (last_seen)
);

-- Retry attempts table for tracking retry history
CREATE TABLE IF NOT EXISTS RETRY_ATTEMPTS (
    attempt_id VARCHAR(36) PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    step VARCHAR(255) NOT NULL,
    attempt_number INT NOT NULL,
    error_message TEXT,
    delay_seconds DECIMAL(10,2),
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    successful BOOLEAN NOT NULL DEFAULT FALSE,
    INDEX idx_run_id (run_id),
    INDEX idx_step (step),
    INDEX idx_timestamp (timestamp),
    INDEX idx_successful (successful)
);

-- Error budget tracking table
CREATE TABLE IF NOT EXISTS ERROR_BUDGET (
    budget_id VARCHAR(36) PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    time_window_start TIMESTAMP NOT NULL,
    time_window_end TIMESTAMP NOT NULL,
    total_operations BIGINT NOT NULL DEFAULT 0,
    error_count BIGINT NOT NULL DEFAULT 0,
    budget_limit_percent DECIMAL(5,2) NOT NULL DEFAULT 5.0,
    budget_used_percent DECIMAL(5,2) NOT NULL DEFAULT 0,
    budget_exceeded BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    INDEX idx_pipeline_id (pipeline_id),
    INDEX idx_time_window (time_window_start, time_window_end),
    INDEX idx_budget_exceeded (budget_exceeded)
);

-- Failure rate analysis table
CREATE TABLE IF NOT EXISTS FAILURE_ANALYSIS (
    analysis_id VARCHAR(36) PRIMARY KEY,
    pipeline_id VARCHAR(255) NOT NULL,
    analysis_period_start TIMESTAMP NOT NULL,
    analysis_period_end TIMESTAMP NOT NULL,
    total_runs INT NOT NULL DEFAULT 0,
    failed_runs INT NOT NULL DEFAULT 0,
    failure_rate_percent DECIMAL(5,2) NOT NULL DEFAULT 0,
    trend VARCHAR(20), -- increasing, decreasing, stable
    failure_pattern JSON, -- Hourly/daily failure counts
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    INDEX idx_pipeline_id (pipeline_id),
    INDEX idx_analysis_period (analysis_period_start, analysis_period_end),
    INDEX idx_trend (trend)
);

-- Recovery actions table for tracking automated recovery
CREATE TABLE IF NOT EXISTS RECOVERY_ACTIONS (
    action_id VARCHAR(36) PRIMARY KEY,
    run_id VARCHAR(255) NOT NULL,
    pipeline_id VARCHAR(255) NOT NULL,
    error_id VARCHAR(36),
    action_type VARCHAR(50) NOT NULL, -- retry, rollback, skip, quarantine, fallback
    action_details JSON,
    initiated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP NULL,
    successful BOOLEAN,
    result_message TEXT,
    FOREIGN KEY (error_id) REFERENCES ERROR_DETAILS(error_id) ON DELETE SET NULL,
    INDEX idx_run_id (run_id),
    INDEX idx_pipeline_id (pipeline_id),
    INDEX idx_action_type (action_type),
    INDEX idx_successful (successful)
);

-- System health metrics table
CREATE TABLE IF NOT EXISTS SYSTEM_HEALTH (
    health_id VARCHAR(36) PRIMARY KEY,
    service_name VARCHAR(255) NOT NULL,
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(10,2),
    unit VARCHAR(20),
    timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    status VARCHAR(20) DEFAULT 'healthy', -- healthy, warning, critical
    INDEX idx_service_name (service_name),
    INDEX idx_metric_name (metric_name),
    INDEX idx_timestamp (timestamp),
    INDEX idx_status (status)
);

-- Insert default circuit breaker states
INSERT IGNORE INTO CIRCUIT_BREAKER_STATES (service_name, state) VALUES
('database', 'closed'),
('api', 'closed'),
('file_system', 'closed'),
('network', 'closed');

-- Insert default error budget settings
INSERT IGNORE INTO ERROR_BUDGET (
    budget_id, pipeline_id, time_window_start, time_window_end,
    budget_limit_percent
) VALUES (
    'default_budget', 'default', DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY),
    CURRENT_DATE, 5.0
);

-- Create indexes for performance
CREATE INDEX IF NOT EXISTS idx_error_details_composite ON ERROR_DETAILS (pipeline_id, error_type, timestamp, resolved);
CREATE INDEX IF NOT EXISTS idx_checkpoints_run_step ON CHECKPOINTS (run_id, step, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_quarantined_unresolved ON QUARANTINED_RECORDS (pipeline_id, resolved, quarantined_at) WHERE resolved = FALSE;
CREATE INDEX IF NOT EXISTS idx_error_patterns_active ON ERROR_PATTERNS (pipeline_id, last_seen, frequency DESC);
CREATE INDEX IF NOT EXISTS idx_retry_attempts_run ON RETRY_ATTEMPTS (run_id, attempt_number, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_recovery_actions_pending ON RECOVERY_ACTIONS (successful) WHERE successful IS NULL;