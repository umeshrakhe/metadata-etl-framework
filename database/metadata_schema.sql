-- ETL Metadata Schema
-- Core metadata tables for ETL pipeline management and tracking

-- Enable UUID extension for PostgreSQL
-- CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ETL Pipelines Table
-- Stores pipeline definitions and configurations
CREATE TABLE etl_pipelines (
    pipeline_id VARCHAR(50) PRIMARY KEY,
    pipeline_name VARCHAR(255) NOT NULL,
    description TEXT,
    pipeline_type VARCHAR(50) NOT NULL, -- 'batch', 'streaming', 'incremental'
    source_system VARCHAR(100),
    target_system VARCHAR(100),
    owner VARCHAR(100),
    status VARCHAR(20) DEFAULT 'active', -- 'active', 'inactive', 'deprecated'
    priority INTEGER DEFAULT 1,
    schedule_config JSONB, -- Cron expression or schedule details
    retry_config JSONB, -- Retry policies and limits
    notification_config JSONB, -- Alert and notification settings
    tags JSONB, -- Key-value tags for categorization
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ETL Pipeline Steps Table
-- Defines individual steps within a pipeline
CREATE TABLE etl_pipeline_steps (
    step_id VARCHAR(50) PRIMARY KEY,
    pipeline_id VARCHAR(50) NOT NULL,
    step_name VARCHAR(255) NOT NULL,
    step_type VARCHAR(50) NOT NULL, -- 'extract', 'transform', 'load', 'validate'
    step_order INTEGER NOT NULL,
    source_config JSONB, -- Source connection and query details
    target_config JSONB, -- Target connection and table details
    transformation_config JSONB, -- Transformation rules and mappings
    validation_config JSONB, -- Data quality rules
    dependency_steps JSONB, -- Prerequisite steps
    retry_config JSONB, -- Step-specific retry policies
    timeout_seconds INTEGER DEFAULT 3600,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pipeline_id) REFERENCES etl_pipelines(pipeline_id) ON DELETE CASCADE
);

-- ETL Pipeline Executions Table
-- Tracks pipeline run instances and their status
CREATE TABLE etl_pipeline_executions (
    execution_id VARCHAR(50) PRIMARY KEY,
    pipeline_id VARCHAR(50) NOT NULL,
    execution_status VARCHAR(20) NOT NULL, -- 'running', 'completed', 'failed', 'cancelled'
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP NULL,
    duration_seconds INTEGER NULL,
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    triggered_by VARCHAR(100), -- 'schedule', 'manual', 'api', 'event'
    trigger_details JSONB, -- Additional trigger information
    execution_metadata JSONB, -- Runtime metadata and statistics
    environment VARCHAR(50) DEFAULT 'production', -- 'dev', 'staging', 'production'
    cluster_id VARCHAR(50), -- Execution cluster/node identifier
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pipeline_id) REFERENCES etl_pipelines(pipeline_id) ON DELETE CASCADE
);

-- ETL Step Executions Table
-- Tracks individual step executions within pipeline runs
CREATE TABLE etl_step_executions (
    step_execution_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    step_id VARCHAR(50) NOT NULL,
    step_status VARCHAR(20) NOT NULL, -- 'pending', 'running', 'completed', 'failed', 'skipped'
    start_time TIMESTAMP NULL,
    end_time TIMESTAMP NULL,
    duration_seconds INTEGER NULL,
    records_processed INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    step_metadata JSONB, -- Step-specific runtime data
    performance_metrics JSONB, -- CPU, memory, I/O metrics
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (step_id) REFERENCES etl_pipeline_steps(step_id) ON DELETE CASCADE
);

-- ETL Data Lineage Table
-- Tracks data flow and transformations through the pipeline
CREATE TABLE etl_data_lineage (
    lineage_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    step_id VARCHAR(50) NOT NULL,
    source_table VARCHAR(255),
    source_columns JSONB,
    target_table VARCHAR(255),
    target_columns JSONB,
    transformation_rules JSONB,
    record_count INTEGER,
    data_quality_score DECIMAL(3,2), -- 0.00 to 1.00
    lineage_metadata JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (step_id) REFERENCES etl_pipeline_steps(step_id) ON DELETE CASCADE
);

-- ETL Pipeline Dependencies Table
-- Manages pipeline dependencies and execution order
CREATE TABLE etl_pipeline_dependencies (
    dependency_id VARCHAR(50) PRIMARY KEY,
    pipeline_id VARCHAR(50) NOT NULL,
    dependent_pipeline_id VARCHAR(50) NOT NULL,
    dependency_type VARCHAR(20) NOT NULL, -- 'hard', 'soft', 'conditional'
    condition_config JSONB, -- Conditions for dependency satisfaction
    timeout_seconds INTEGER DEFAULT 3600,
    status VARCHAR(20) DEFAULT 'active',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pipeline_id) REFERENCES etl_pipelines(pipeline_id) ON DELETE CASCADE,
    FOREIGN KEY (dependent_pipeline_id) REFERENCES etl_pipelines(pipeline_id) ON DELETE CASCADE
);

-- ETL Configuration Templates Table
-- Stores reusable configuration templates
CREATE TABLE etl_config_templates (
    template_id VARCHAR(50) PRIMARY KEY,
    template_name VARCHAR(255) NOT NULL,
    template_type VARCHAR(50) NOT NULL, -- 'pipeline', 'step', 'connection'
    template_config JSONB NOT NULL,
    description TEXT,
    version VARCHAR(20) DEFAULT '1.0',
    is_default BOOLEAN DEFAULT FALSE,
    tags JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ETL Connection Definitions Table
-- Stores connection configurations for various systems
CREATE TABLE etl_connections (
    connection_id VARCHAR(50) PRIMARY KEY,
    connection_name VARCHAR(255) NOT NULL,
    connection_type VARCHAR(50) NOT NULL, -- 'database', 'api', 'file', 'message_queue'
    connection_config JSONB NOT NULL, -- Encrypted connection details
    connection_status VARCHAR(20) DEFAULT 'active', -- 'active', 'inactive', 'testing'
    test_query TEXT, -- Query to test connection
    last_tested TIMESTAMP NULL,
    last_successful_test TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ETL Transformation Rules Table
-- Stores reusable transformation rules and mappings
CREATE TABLE etl_transformation_rules (
    rule_id VARCHAR(50) PRIMARY KEY,
    rule_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(50) NOT NULL, -- 'mapping', 'filter', 'aggregation', 'validation'
    rule_config JSONB NOT NULL,
    input_schema JSONB, -- Expected input structure
    output_schema JSONB, -- Expected output structure
    description TEXT,
    version VARCHAR(20) DEFAULT '1.0',
    tags JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ETL Quality Rules Table
-- Stores data quality validation rules
CREATE TABLE etl_quality_rules (
    rule_id VARCHAR(50) PRIMARY KEY,
    rule_name VARCHAR(255) NOT NULL,
    rule_type VARCHAR(50) NOT NULL, -- 'completeness', 'accuracy', 'consistency', 'timeliness'
    rule_config JSONB NOT NULL,
    severity VARCHAR(20) DEFAULT 'medium', -- 'low', 'medium', 'high', 'critical'
    threshold_config JSONB, -- Pass/fail thresholds
    description TEXT,
    version VARCHAR(20) DEFAULT '1.0',
    tags JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ETL Audit Log Table
-- Comprehensive audit trail for all ETL operations
CREATE TABLE etl_audit_log (
    audit_id VARCHAR(50) PRIMARY KEY,
    entity_type VARCHAR(50) NOT NULL, -- 'pipeline', 'execution', 'step', 'connection'
    entity_id VARCHAR(50) NOT NULL,
    operation VARCHAR(50) NOT NULL, -- 'create', 'update', 'delete', 'execute', 'cancel'
    operation_details JSONB,
    user_id VARCHAR(100),
    ip_address VARCHAR(45),
    user_agent TEXT,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    session_id VARCHAR(50)
);

-- Indexes for performance
CREATE INDEX idx_etl_pipeline_executions_pipeline_id ON etl_pipeline_executions(pipeline_id);
CREATE INDEX idx_etl_pipeline_executions_status ON etl_pipeline_executions(execution_status);
CREATE INDEX idx_etl_pipeline_executions_start_time ON etl_pipeline_executions(start_time);

CREATE INDEX idx_etl_step_executions_execution_id ON etl_step_executions(execution_id);
CREATE INDEX idx_etl_step_executions_step_id ON etl_step_executions(step_id);
CREATE INDEX idx_etl_step_executions_status ON etl_step_executions(step_status);

CREATE INDEX idx_etl_data_lineage_execution_id ON etl_data_lineage(execution_id);
CREATE INDEX idx_etl_data_lineage_step_id ON etl_data_lineage(step_id);

CREATE INDEX idx_etl_audit_log_entity ON etl_audit_log(entity_type, entity_id);
CREATE INDEX idx_etl_audit_log_timestamp ON etl_audit_log(timestamp);
CREATE INDEX idx_etl_audit_log_user ON etl_audit_log(user_id);

CREATE INDEX idx_etl_pipelines_status ON etl_pipelines(status);
CREATE INDEX idx_etl_pipelines_owner ON etl_pipelines(owner);
CREATE INDEX idx_etl_pipeline_steps_pipeline_id ON etl_pipeline_steps(pipeline_id);

-- Comments for documentation
COMMENT ON TABLE etl_pipelines IS 'Core pipeline definitions and configurations';
COMMENT ON TABLE etl_pipeline_steps IS 'Individual steps within pipelines';
COMMENT ON TABLE etl_pipeline_executions IS 'Pipeline execution instances and tracking';
COMMENT ON TABLE etl_step_executions IS 'Individual step execution tracking';
COMMENT ON TABLE etl_data_lineage IS 'Data flow and transformation tracking';
COMMENT ON TABLE etl_pipeline_dependencies IS 'Pipeline dependency management';
COMMENT ON TABLE etl_config_templates IS 'Reusable configuration templates';
COMMENT ON TABLE etl_connections IS 'System connection configurations';
COMMENT ON TABLE etl_transformation_rules IS 'Reusable transformation rules';
COMMENT ON TABLE etl_quality_rules IS 'Data quality validation rules';
COMMENT ON TABLE etl_audit_log IS 'Comprehensive audit trail';