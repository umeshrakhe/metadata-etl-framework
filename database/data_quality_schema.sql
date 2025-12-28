-- ETL Data Quality Schema
-- Data quality rules, validations, profiling, and monitoring

-- ETL Data Quality Profiles Table
-- Stores data profiling results and statistics
CREATE TABLE etl_data_profiles (
    profile_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    step_id VARCHAR(50),
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255),
    profile_type VARCHAR(50) NOT NULL, -- 'table', 'column', 'pattern'
    profile_data JSONB NOT NULL, -- Detailed profiling statistics
    record_count INTEGER,
    null_count INTEGER,
    distinct_count INTEGER,
    profile_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (step_id) REFERENCES etl_pipeline_steps(step_id) ON DELETE CASCADE
);

-- ETL Data Quality Rules Table (extends etl_quality_rules)
-- Stores data quality validation rules with execution context
CREATE TABLE etl_dq_rules (
    dq_rule_id VARCHAR(50) PRIMARY KEY,
    rule_name VARCHAR(255) NOT NULL,
    rule_category VARCHAR(50) NOT NULL, -- 'completeness', 'accuracy', 'consistency', 'validity', 'timeliness'
    rule_type VARCHAR(50) NOT NULL, -- 'sql', 'expression', 'threshold', 'pattern', 'reference'
    rule_definition JSONB NOT NULL, -- Rule logic and parameters
    severity_level VARCHAR(20) DEFAULT 'medium', -- 'low', 'medium', 'high', 'critical'
    enabled BOOLEAN DEFAULT TRUE,
    sample_size INTEGER, -- Number of records to validate (NULL = all)
    execution_context JSONB, -- When/how to execute this rule
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ETL Data Quality Rule Executions Table
-- Tracks execution of data quality rules
CREATE TABLE etl_dq_rule_executions (
    dq_execution_id VARCHAR(50) PRIMARY KEY,
    execution_id VARCHAR(50) NOT NULL,
    step_id VARCHAR(50),
    dq_rule_id VARCHAR(50) NOT NULL,
    rule_status VARCHAR(20) NOT NULL, -- 'passed', 'failed', 'error', 'skipped'
    records_checked INTEGER DEFAULT 0,
    records_failed INTEGER DEFAULT 0,
    failure_percentage DECIMAL(5,2),
    execution_time_seconds DECIMAL(8,2),
    error_message TEXT,
    rule_results JSONB, -- Detailed validation results
    execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (execution_id) REFERENCES etl_pipeline_executions(execution_id) ON DELETE CASCADE,
    FOREIGN KEY (step_id) REFERENCES etl_pipeline_steps(step_id) ON DELETE CASCADE,
    FOREIGN KEY (dq_rule_id) REFERENCES etl_dq_rules(dq_rule_id) ON DELETE CASCADE
);

-- ETL Data Quality Violations Table
-- Detailed records of data quality violations
CREATE TABLE etl_dq_violations (
    violation_id VARCHAR(50) PRIMARY KEY,
    dq_execution_id VARCHAR(50) NOT NULL,
    dq_rule_id VARCHAR(50) NOT NULL,
    table_name VARCHAR(255) NOT NULL,
    column_name VARCHAR(255),
    record_identifier JSONB, -- How to identify the violating record
    violation_value TEXT, -- The actual violating value
    expected_value TEXT, -- What the value should be
    violation_type VARCHAR(50) NOT NULL, -- 'null_value', 'invalid_format', 'out_of_range', etc.
    severity VARCHAR(20) DEFAULT 'medium',
    quarantine_action VARCHAR(20), -- 'none', 'flag', 'remove', 'correct'
    correction_applied BOOLEAN DEFAULT FALSE,
    correction_details JSONB,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (dq_execution_id) REFERENCES etl_dq_rule_executions(dq_execution_id) ON DELETE CASCADE,
    FOREIGN KEY (dq_rule_id) REFERENCES etl_dq_rules(dq_rule_id) ON DELETE CASCADE
);

-- ETL Data Quality Dashboards Table
-- Dashboard configurations for data quality monitoring
CREATE TABLE etl_dq_dashboards (
    dashboard_id VARCHAR(50) PRIMARY KEY,
    dashboard_name VARCHAR(255) NOT NULL,
    dashboard_config JSONB NOT NULL, -- Dashboard layout and widgets
    pipeline_filter JSONB, -- Which pipelines to include
    time_range_config JSONB, -- Default time ranges
    refresh_interval_minutes INTEGER DEFAULT 60,
    is_public BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ETL Data Quality Metrics Table
-- Aggregated data quality metrics over time
CREATE TABLE etl_dq_metrics (
    metric_id VARCHAR(50) PRIMARY KEY,
    pipeline_id VARCHAR(50),
    table_name VARCHAR(255),
    metric_date DATE NOT NULL,
    completeness_score DECIMAL(5,4), -- 0.0000 to 1.0000
    accuracy_score DECIMAL(5,4),
    consistency_score DECIMAL(5,4),
    validity_score DECIMAL(5,4),
    timeliness_score DECIMAL(5,4),
    overall_score DECIMAL(5,4),
    total_records INTEGER,
    valid_records INTEGER,
    invalid_records INTEGER,
    rules_executed INTEGER,
    rules_passed INTEGER,
    rules_failed INTEGER,
    execution_time_avg DECIMAL(8,2),
    metadata JSONB,
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pipeline_id) REFERENCES etl_pipelines(pipeline_id) ON DELETE CASCADE
);

-- ETL Data Quality Anomalies Table
-- Detected anomalies in data quality patterns
CREATE TABLE etl_dq_anomalies (
    anomaly_id VARCHAR(50) PRIMARY KEY,
    pipeline_id VARCHAR(50),
    table_name VARCHAR(255),
    anomaly_type VARCHAR(50) NOT NULL, -- 'spike', 'drop', 'trend_change', 'outlier'
    anomaly_description TEXT NOT NULL,
    severity VARCHAR(20) DEFAULT 'medium',
    detected_metrics JSONB, -- Metrics that triggered the anomaly
    baseline_metrics JSONB, -- Expected baseline values
    deviation_percentage DECIMAL(5,2),
    affected_records INTEGER,
    anomaly_start_date DATE,
    anomaly_end_date DATE,
    investigation_status VARCHAR(20) DEFAULT 'open', -- 'open', 'investigating', 'resolved', 'false_positive'
    investigation_notes TEXT,
    resolved_by VARCHAR(100),
    resolved_at TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (pipeline_id) REFERENCES etl_pipelines(pipeline_id) ON DELETE CASCADE
);

-- ETL Data Quality Reference Data Table
-- Reference datasets for data quality validation
CREATE TABLE etl_dq_reference_data (
    reference_id VARCHAR(50) PRIMARY KEY,
    reference_name VARCHAR(255) NOT NULL,
    reference_type VARCHAR(50) NOT NULL, -- 'lookup_table', 'valid_values', 'domain_rules'
    reference_data JSONB NOT NULL, -- The reference data content
    version VARCHAR(20) DEFAULT '1.0',
    is_active BOOLEAN DEFAULT TRUE,
    refresh_schedule JSONB, -- When to refresh the reference data
    source_system VARCHAR(100),
    last_refreshed TIMESTAMP NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ETL Data Quality Rule Templates Table
-- Predefined rule templates for common validations
CREATE TABLE etl_dq_rule_templates (
    template_id VARCHAR(50) PRIMARY KEY,
    template_name VARCHAR(255) NOT NULL,
    template_category VARCHAR(50) NOT NULL,
    template_description TEXT,
    template_config JSONB NOT NULL, -- Template configuration
    applicable_data_types JSONB, -- Which data types this template works for
    complexity_level VARCHAR(20) DEFAULT 'simple', -- 'simple', 'medium', 'complex'
    usage_count INTEGER DEFAULT 0,
    is_official BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ETL Data Quality Scorecards Table
-- Scorecards for data quality assessment across dimensions
CREATE TABLE etl_dq_scorecards (
    scorecard_id VARCHAR(50) PRIMARY KEY,
    scorecard_name VARCHAR(255) NOT NULL,
    scorecard_config JSONB NOT NULL, -- Scorecard structure and weights
    target_score DECIMAL(5,4), -- Target overall score
    evaluation_period VARCHAR(20) DEFAULT 'monthly', -- 'daily', 'weekly', 'monthly', 'quarterly'
    dimensions JSONB, -- Quality dimensions and their weights
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- ETL Data Quality Scorecard Results Table
-- Results of scorecard evaluations
CREATE TABLE etl_dq_scorecard_results (
    result_id VARCHAR(50) PRIMARY KEY,
    scorecard_id VARCHAR(50) NOT NULL,
    evaluation_date DATE NOT NULL,
    overall_score DECIMAL(5,4),
    dimension_scores JSONB, -- Scores for each dimension
    pipeline_scores JSONB, -- Scores by pipeline
    trend_analysis JSONB, -- Score trends over time
    recommendations JSONB, -- Suggested improvements
    evaluated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (scorecard_id) REFERENCES etl_dq_scorecards(scorecard_id) ON DELETE CASCADE
);

-- ETL Data Quality Incident Management Table
-- Tracking and resolution of data quality incidents
CREATE TABLE etl_dq_incidents (
    incident_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL, -- 'low', 'medium', 'high', 'critical'
    status VARCHAR(20) NOT NULL, -- 'open', 'investigating', 'resolved', 'closed'
    affected_pipelines JSONB,
    affected_tables JSONB,
    root_cause TEXT,
    impact_assessment JSONB,
    resolution_steps JSONB,
    prevention_measures JSONB,
    assigned_to VARCHAR(100),
    priority INTEGER DEFAULT 1,
    due_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP NULL,
    created_by VARCHAR(100),
    updated_by VARCHAR(100)
);

-- Indexes for performance
CREATE INDEX idx_etl_data_profiles_execution_id ON etl_data_profiles(execution_id);
CREATE INDEX idx_etl_data_profiles_table ON etl_data_profiles(table_name);
CREATE INDEX idx_etl_data_profiles_timestamp ON etl_data_profiles(profile_timestamp);

CREATE INDEX idx_etl_dq_rule_executions_execution_id ON etl_dq_rule_executions(execution_id);
CREATE INDEX idx_etl_dq_rule_executions_rule_id ON etl_dq_rule_executions(dq_rule_id);
CREATE INDEX idx_etl_dq_rule_executions_status ON etl_dq_rule_executions(rule_status);

CREATE INDEX idx_etl_dq_violations_execution_id ON etl_dq_violations(dq_execution_id);
CREATE INDEX idx_etl_dq_violations_rule_id ON etl_dq_violations(dq_rule_id);
CREATE INDEX idx_etl_dq_violations_type ON etl_dq_violations(violation_type);

CREATE INDEX idx_etl_dq_metrics_pipeline ON etl_dq_metrics(pipeline_id);
CREATE INDEX idx_etl_dq_metrics_table ON etl_dq_metrics(table_name);
CREATE INDEX idx_etl_dq_metrics_date ON etl_dq_metrics(metric_date);

CREATE INDEX idx_etl_dq_anomalies_pipeline ON etl_dq_anomalies(pipeline_id);
CREATE INDEX idx_etl_dq_anomalies_type ON etl_dq_anomalies(anomaly_type);
CREATE INDEX idx_etl_dq_anomalies_status ON etl_dq_anomalies(investigation_status);

CREATE INDEX idx_etl_dq_scorecard_results_scorecard ON etl_dq_scorecard_results(scorecard_id);
CREATE INDEX idx_etl_dq_scorecard_results_date ON etl_dq_scorecard_results(evaluation_date);

CREATE INDEX idx_etl_dq_incidents_status ON etl_dq_incidents(status);
CREATE INDEX idx_etl_dq_incidents_severity ON etl_dq_incidents(severity);
CREATE INDEX idx_etl_dq_incidents_assigned ON etl_dq_incidents(assigned_to);

-- Comments for documentation
COMMENT ON TABLE etl_data_profiles IS 'Data profiling results and statistics';
COMMENT ON TABLE etl_dq_rules IS 'Data quality validation rules';
COMMENT ON TABLE etl_dq_rule_executions IS 'Execution tracking for quality rules';
COMMENT ON TABLE etl_dq_violations IS 'Detailed data quality violations';
COMMENT ON TABLE etl_dq_dashboards IS 'Data quality dashboard configurations';
COMMENT ON TABLE etl_dq_metrics IS 'Aggregated data quality metrics';
COMMENT ON TABLE etl_dq_anomalies IS 'Detected data quality anomalies';
COMMENT ON TABLE etl_dq_reference_data IS 'Reference datasets for validation';
COMMENT ON TABLE etl_dq_rule_templates IS 'Predefined rule templates';
COMMENT ON TABLE etl_dq_scorecards IS 'Data quality assessment scorecards';
COMMENT ON TABLE etl_dq_scorecard_results IS 'Scorecard evaluation results';
COMMENT ON TABLE etl_dq_incidents IS 'Data quality incident management';