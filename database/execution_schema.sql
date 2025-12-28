-- ETL Execution Schema
-- Runtime execution tracking, monitoring, and performance metrics

-- ETL Execution Metrics Table
-- Stores detailed performance metrics for pipeline executions
CREATE TABLE etl_execution_metrics (
    metric_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    step_id VARCHAR(50),
    metric_type VARCHAR(50) NOT NULL, -- 'performance', 'resource', 'data_quality', 'error'
    metric_name VARCHAR(100) NOT NULL,
    metric_value DECIMAL(15,4),
    metric_unit VARCHAR(20), -- 'seconds', 'bytes', 'count', 'percentage'
    collection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    metadata JSONB, -- Additional metric context
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (step_id) REFERENCES etl_pipeline_steps(step_id) ON DELETE CASCADE
);

-- ETL Resource Usage Table
-- Tracks system resource consumption during executions
CREATE TABLE etl_resource_usage (
    usage_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    step_id VARCHAR(50),
    resource_type VARCHAR(50) NOT NULL, -- 'cpu', 'memory', 'disk', 'network'
    resource_name VARCHAR(100),
    usage_value DECIMAL(10,2),
    usage_unit VARCHAR(20), -- 'percentage', 'bytes', 'mbps'
    peak_usage DECIMAL(10,2),
    average_usage DECIMAL(10,2),
    collection_interval_seconds INTEGER DEFAULT 60,
    collection_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (step_id) REFERENCES etl_pipeline_steps(step_id) ON DELETE CASCADE
);

-- ETL Execution Logs Table
-- Detailed execution logs with different log levels
CREATE TABLE etl_execution_logs (
    log_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    step_id VARCHAR(50),
    log_level VARCHAR(10) NOT NULL, -- 'DEBUG', 'INFO', 'WARN', 'ERROR'
    log_message TEXT NOT NULL,
    log_context JSONB, -- Structured log context
    source_component VARCHAR(100), -- Component that generated the log
    thread_id VARCHAR(50),
    log_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (step_id) REFERENCES etl_pipeline_steps(step_id) ON DELETE CASCADE
);

-- ETL Checkpoint Data Table
-- Stores checkpoint data for resumable executions
CREATE TABLE etl_checkpoint_data (
    checkpoint_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    step_id VARCHAR(50) NOT NULL,
    checkpoint_name VARCHAR(255) NOT NULL,
    checkpoint_data JSONB NOT NULL,
    checkpoint_sequence INTEGER NOT NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    expires_at TIMESTAMP NULL,
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (step_id) REFERENCES etl_pipeline_steps(step_id) ON DELETE CASCADE,
    UNIQUE(execution_id, step_id, checkpoint_name, checkpoint_sequence)
);

-- ETL Execution Events Table
-- Event-driven execution tracking and notifications
CREATE TABLE etl_execution_events (
    event_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    step_id VARCHAR(50),
    event_type VARCHAR(50) NOT NULL, -- 'started', 'completed', 'failed', 'retry', 'checkpoint'
    event_message TEXT,
    event_data JSONB,
    severity VARCHAR(20) DEFAULT 'info', -- 'debug', 'info', 'warn', 'error', 'critical'
    notified BOOLEAN DEFAULT FALSE,
    notification_time TIMESTAMP NULL,
    event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (step_id) REFERENCES etl_pipeline_steps(step_id) ON DELETE CASCADE
);

-- ETL Performance Baselines Table
-- Historical performance baselines for anomaly detection
CREATE TABLE etl_performance_baselines (
    baseline_id VARCHAR(50) PRIMARY KEY,
    pipeline_id VARCHAR(50) NOT NULL,
    step_id VARCHAR(50),
    metric_name VARCHAR(100) NOT NULL,
    baseline_type VARCHAR(20) NOT NULL, -- 'daily', 'weekly', 'monthly'
    baseline_period_start DATE NOT NULL,
    baseline_period_end DATE NOT NULL,
    baseline_value DECIMAL(15,4) NOT NULL,
    standard_deviation DECIMAL(15,4),
    upper_threshold DECIMAL(15,4),
    lower_threshold DECIMAL(15,4),
    sample_count INTEGER NOT NULL,
    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pipeline_id) REFERENCES etl_pipelines(pipeline_id) ON DELETE CASCADE,
    FOREIGN KEY (step_id) REFERENCES etl_pipeline_steps(step_id) ON DELETE CASCADE
);

-- ETL SLA Tracking Table
-- Service Level Agreement monitoring and compliance
CREATE TABLE etl_sla_tracking (
    sla_id VARCHAR(50) PRIMARY KEY,
    pipeline_id VARCHAR(50) NOT NULL,
    sla_name VARCHAR(255) NOT NULL,
    sla_type VARCHAR(50) NOT NULL, -- 'completion_time', 'data_freshness', 'error_rate'
    target_value DECIMAL(10,2),
    target_unit VARCHAR(20), -- 'seconds', 'minutes', 'hours', 'percentage'
    warning_threshold DECIMAL(10,2),
    critical_threshold DECIMAL(10,2),
    measurement_period VARCHAR(20), -- 'daily', 'weekly', 'monthly'
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pipeline_id) REFERENCES etl_pipelines(pipeline_id) ON DELETE CASCADE
);

-- ETL SLA Measurements Table
-- Actual SLA measurements and compliance tracking
CREATE TABLE etl_sla_measurements (
    measurement_id VARCHAR(50) PRIMARY KEY,
    sla_id VARCHAR(50) NOT NULL,
    execution_id VARCHAR(50),
    measurement_date DATE NOT NULL,
    measured_value DECIMAL(10,2),
    target_value DECIMAL(10,2),
    compliance_status VARCHAR(20), -- 'compliant', 'warning', 'breached'
    deviation_percentage DECIMAL(5,2),
    measurement_metadata JSONB,
    recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sla_id) REFERENCES etl_sla_tracking(sla_id) ON DELETE CASCADE,
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE
);

-- ETL Queue Management Table
-- Execution queue management for concurrent pipelines
CREATE TABLE etl_execution_queue (
    queue_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    pipeline_id VARCHAR(50) NOT NULL,
    priority INTEGER DEFAULT 1,
    queue_status VARCHAR(20) NOT NULL, -- 'queued', 'running', 'completed', 'failed', 'cancelled'
    queued_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP NULL,
    completed_at TIMESTAMP NULL,
    worker_id VARCHAR(50), -- ID of the worker processing this execution
    queue_timeout_seconds INTEGER DEFAULT 3600,
    retry_count INTEGER DEFAULT 0,
    max_retries INTEGER DEFAULT 3,
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (pipeline_id) REFERENCES etl_pipelines(pipeline_id) ON DELETE CASCADE
);

-- ETL Worker Status Table
-- Tracks worker nodes and their capacity
CREATE TABLE etl_worker_status (
    worker_id VARCHAR(50) PRIMARY KEY,
    worker_name VARCHAR(255) NOT NULL,
    worker_type VARCHAR(50) NOT NULL, -- 'batch', 'streaming', 'general'
    status VARCHAR(20) NOT NULL, -- 'active', 'inactive', 'maintenance'
    capacity INTEGER NOT NULL, -- Maximum concurrent executions
    current_load INTEGER DEFAULT 0,
    host_info JSONB, -- Host system information
    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    registered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ETL Execution Dependencies Table
-- Runtime dependency tracking for complex execution flows
CREATE TABLE etl_execution_dependencies (
    dependency_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    dependent_execution_id VARCHAR(50) NOT NULL,
    dependency_type VARCHAR(20) NOT NULL, -- 'finish_to_start', 'start_to_start', 'finish_to_finish'
    dependency_status VARCHAR(20) DEFAULT 'pending', -- 'pending', 'satisfied', 'failed'
    satisfied_at TIMESTAMP NULL,
    timeout_seconds INTEGER DEFAULT 3600,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (dependent_execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE
);

-- ETL Alert History Table
-- Historical record of alerts and notifications
CREATE TABLE etl_alert_history (
    alert_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50),
    pipeline_id VARCHAR(50),
    alert_type VARCHAR(50) NOT NULL, -- 'sla_breach', 'execution_failure', 'performance_anomaly'
    alert_severity VARCHAR(20) NOT NULL, -- 'info', 'warning', 'error', 'critical'
    alert_message TEXT NOT NULL,
    alert_details JSONB,
    notification_channels JSONB, -- Email, Slack, PagerDuty, etc.
    notification_status VARCHAR(20), -- 'sent', 'failed', 'pending'
    acknowledged BOOLEAN DEFAULT FALSE,
    acknowledged_by VARCHAR(100),
    acknowledged_at TIMESTAMP NULL,
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (pipeline_id) REFERENCES etl_pipelines(pipeline_id) ON DELETE CASCADE
);

-- Indexes for performance
CREATE INDEX idx_etl_execution_metrics_execution_id ON etl_execution_metrics(execution_id);
CREATE INDEX idx_etl_execution_metrics_type ON etl_execution_metrics(metric_type);
CREATE INDEX idx_etl_execution_metrics_time ON etl_execution_metrics(collection_time);

CREATE INDEX idx_etl_resource_usage_execution_id ON etl_resource_usage(execution_id);
CREATE INDEX idx_etl_resource_usage_type ON etl_resource_usage(resource_type);
CREATE INDEX idx_etl_resource_usage_time ON etl_resource_usage(collection_time);

CREATE INDEX idx_etl_execution_logs_execution_id ON etl_execution_logs(execution_id);
CREATE INDEX idx_etl_execution_logs_level ON etl_execution_logs(log_level);
CREATE INDEX idx_etl_execution_logs_timestamp ON etl_execution_logs(log_timestamp);

CREATE INDEX idx_etl_checkpoint_data_execution_id ON etl_checkpoint_data(execution_id);
CREATE INDEX idx_etl_checkpoint_data_active ON etl_checkpoint_data(is_active);

CREATE INDEX idx_etl_execution_events_execution_id ON etl_execution_events(execution_id);
CREATE INDEX idx_etl_execution_events_type ON etl_execution_events(event_type);
CREATE INDEX idx_etl_execution_events_timestamp ON etl_execution_events(event_timestamp);

CREATE INDEX idx_etl_performance_baselines_pipeline ON etl_performance_baselines(pipeline_id);
CREATE INDEX idx_etl_performance_baselines_metric ON etl_performance_baselines(metric_name);

CREATE INDEX idx_etl_sla_measurements_sla_id ON etl_sla_measurements(sla_id);
CREATE INDEX idx_etl_sla_measurements_date ON etl_sla_measurements(measurement_date);
CREATE INDEX idx_etl_sla_measurements_status ON etl_sla_measurements(compliance_status);

CREATE INDEX idx_etl_execution_queue_status ON etl_execution_queue(queue_status);
CREATE INDEX idx_etl_execution_queue_priority ON etl_execution_queue(priority);
CREATE INDEX idx_etl_execution_queue_worker ON etl_execution_queue(worker_id);

CREATE INDEX idx_etl_worker_status_type ON etl_worker_status(worker_type);
CREATE INDEX idx_etl_worker_status_status ON etl_worker_status(status);
CREATE INDEX idx_etl_worker_status_heartbeat ON etl_worker_status(last_heartbeat);

CREATE INDEX idx_etl_alert_history_type ON etl_alert_history(alert_type);
CREATE INDEX idx_etl_alert_history_severity ON etl_alert_history(alert_severity);
CREATE INDEX idx_etl_alert_history_created ON etl_alert_history(created_at);

-- Comments for documentation
COMMENT ON TABLE etl_execution_metrics IS 'Detailed performance metrics for pipeline executions';
COMMENT ON TABLE etl_resource_usage IS 'System resource consumption tracking';
COMMENT ON TABLE etl_execution_logs IS 'Detailed execution logs with different levels';
COMMENT ON TABLE etl_checkpoint_data IS 'Checkpoint data for resumable executions';
COMMENT ON TABLE etl_execution_events IS 'Event-driven execution tracking';
COMMENT ON TABLE etl_performance_baselines IS 'Historical performance baselines';
COMMENT ON TABLE etl_sla_tracking IS 'Service Level Agreement definitions';
COMMENT ON TABLE etl_sla_measurements IS 'SLA compliance measurements';
COMMENT ON TABLE etl_execution_queue IS 'Execution queue management';
COMMENT ON TABLE etl_worker_status IS 'Worker node status and capacity';
COMMENT ON TABLE etl_execution_dependencies IS 'Runtime execution dependencies';
COMMENT ON TABLE etl_alert_history IS 'Historical alerts and notifications';