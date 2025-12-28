# ETL Framework Knowledge Base

## Overview

This document provides comprehensive documentation for the Metadata-Driven ETL Framework, detailing each module, its purpose, functionality, and usage patterns.

## Architecture Overview

The ETL framework is organized into the following key components:

- **API Layer**: REST and CLI interfaces for system interaction
- **Orchestrator**: Pipeline execution coordination and management
- **Transform Layer**: Data transformation engines and connectors
- **Quality Layer**: Data validation and anomaly detection
- **Monitoring Layer**: Performance monitoring and SLA management
- **Utilities**: Common functionality for security, database operations, and error handling

## Module Documentation

### 1. API Layer

#### `api/rest_api.py`
**Purpose**: RESTful API for programmatic access to ETL framework functionality.

**Key Features**:
- Pipeline management endpoints
- Execution monitoring and control
- System status and health checks
- Supports both Flask and FastAPI frameworks

**Main Classes**:
- `ETLRestAPI`: Main API class with endpoints for:
  - `GET /pipelines` - List pipelines
  - `POST /pipelines/{id}/execute` - Execute pipeline
  - `GET /runs/{id}/status` - Get execution status
  - `GET /health` - System health check

**Usage**:
```python
from api.rest_api import ETLRestAPI
api = ETLRestAPI(db_connection, orchestrator)
api.run()  # Starts the API server
```

#### `api/cli.py`
**Purpose**: Command-line interface for ETL framework operations.

**Key Features**:
- Pipeline execution from command line
- Configuration validation
- Batch processing capabilities
- Interactive mode support

### 2. Orchestrator Layer

#### `orchestrator/orchestrator_manager.py`
**Purpose**: Coordinates pipeline runs and manages the complete ETL workflow.

**Key Features**:
- Pipeline execution orchestration
- Metadata loading and validation
- Error handling and retry logic
- Real-time status updates

**Main Class**: `OrchestratorManager`
- **Dependencies**: config_loader, connector_factory, engine_selector, transform_engine, dq_engine, sla_monitor, alert_manager, db_connection
- **Main Methods**:
  - `execute_pipeline()`: Main pipeline execution method
  - `load_configuration()`: Loads pipeline metadata
  - `create_execution_record()`: Creates run tracking records

#### `orchestrator/config_loader.py`
**Purpose**: Loads and validates pipeline configurations from metadata store.

**Key Features**:
- Configuration validation
- Environment-specific settings
- Dynamic configuration loading
- Schema validation

**Main Class**: `ConfigLoader`
- **Dependencies**: db_connection
- **Main Methods**:
  - `load_pipeline_config()`: Loads pipeline configuration
  - `validate_config()`: Validates configuration schema
  - `get_connection_config()`: Retrieves connection settings

#### `orchestrator/pipeline_scheduler.py`
**Purpose**: Schedules and manages automated pipeline executions.

**Key Features**:
- Cron-based scheduling
- Dependency management
- Execution queuing
- Resource allocation

**Main Class**: `PipelineScheduler`
- **Main Methods**:
  - `schedule_pipeline()`: Schedules pipeline execution
  - `get_next_runs()`: Determines next execution times
  - `check_dependencies()`: Validates execution prerequisites

### 3. Transform Layer

#### `transform/transform_engine.py`
**Purpose**: Executes transformation pipelines using configurable engines (Pandas/Polars/Dask).

**Key Features**:
- Multiple engine support (Pandas, Polars, Dask, DuckDB)
- Various transformation types (filter, aggregate, join, etc.)
- Execution logging and monitoring
- Error handling and recovery

**Main Class**: `TransformEngine`
- **Dependencies**: db_connection
- **Supported Transformations**:
  - Filter: Data filtering with conditions
  - Aggregate: Group-by aggregations
  - Join: Multi-source data joining
  - Map: Column transformations
  - Pivot: Data restructuring
  - Custom UDF: User-defined functions

**Usage**:
```python
engine = TransformEngine(db_connection)
result = engine.execute_transformations(data, transform_config, engine="pandas")
```

#### `transform/engine_selector.py`
**Purpose**: Automatically selects optimal processing engine based on data characteristics.

**Key Features**:
- Data size analysis
- Memory requirement estimation
- Engine availability checking
- Performance optimization

**Main Class**: `EngineSelector`
- **Dependencies**: db_connection, config_loader
- **Selection Criteria**:
  - Data size thresholds
  - Available memory
  - Transformation complexity
  - Engine capabilities

#### `transform/connector_factory.py`
**Purpose**: Factory for creating data connectors with connection pooling and retry logic.

**Key Features**:
- Multiple connector types (Relational DB, NoSQL, Files, APIs, Cloud Storage)
- Connection pooling
- Automatic retry with backoff
- Encryption support

**Note**: Currently, all connector implementations are contained within this single file. For better organization, individual connector classes should be moved to separate files in the `connectors/` directory.

**Supported Connectors** (currently implemented in connector_factory.py):
- **RelationalDBConnector**: PostgreSQL, MySQL, SQL Server, Oracle
- **NoSQLConnector**: MongoDB, Cassandra
- **FileConnector**: CSV, Excel, Parquet, JSON
- **APIConnector**: REST APIs, SOAP services
- **CloudStorageConnector**: S3, Azure Blob, Google Cloud Storage

**Main Class**: `ConnectorFactory`
- **Dependencies**: config_loader, db_connection
- **Key Methods**:
  - `create_source_connector()`: Creates source connectors
  - `create_target_connector()`: Creates target connectors
  - `extract_data()`: Orchestrates data extraction with retry
  - `load_data()`: Orchestrates data loading with retry

**Recommended Refactoring**:
The `connectors/` folder should contain:
- `base_connector.py`: BaseConnector abstract class
- `relational_connector.py`: RelationalDBConnector
- `nosql_connector.py`: NoSQLConnector
- `file_connector.py`: FileConnector
- `api_connector.py`: APIConnector
- `cloud_connector.py`: CloudStorageConnector

### 4. Quality Layer

#### `quality/dq_engine.py`
**Purpose**: Comprehensive data quality validation including profiling, rule-based checks, and anomaly detection.

**Key Features**:
- Data profiling and statistics
- Rule-based validation (completeness, validity, uniqueness, consistency)
- Multiple anomaly detection methods
- Quality scoring and reporting

**Main Components**:
- **ProfileManager**: Generates data profiling statistics
- **RuleEngine**: Executes rule-based validations
- **AnomalyManager**: Detects data anomalies
- **ResultsManager**: Stores and reports validation results

**Main Class**: `DQEngine`
- **Dependencies**: db_connection, config_loader
- **Validation Types**:
  - Completeness: Null value checks
  - Validity: Range, pattern, format checks
  - Uniqueness: Duplicate detection
  - Consistency: Cross-field validation

**Anomaly Detection Methods**:
- Z-Score: Statistical outlier detection
- Isolation Forest: Machine learning-based detection
- IQR: Interquartile range method

#### `quality/anomaly_manager.py`
**Purpose**: Specialized anomaly detection functionality.

**Note**: Integrated into `dq_engine.py` as `AnomalyManager` class.

#### `quality/dq_rule_examples.py`
**Purpose**: Examples and templates for data quality rules.

**Contents**:
- Rule templates for common validations
- Example configurations
- Best practices for rule definition

### 5. Monitoring Layer

#### `monitoring/sla_monitor.py`
**Purpose**: Monitors SLA compliance for pipeline executions.

**Key Features**:
- SLA definition management
- Execution time monitoring
- Data quality SLA checks
- Success rate tracking
- Compliance reporting

**Main Class**: `SLAMonitor`
- **Dependencies**: db_connection, alert_manager
- **SLA Types**:
  - Duration: Execution time limits
  - Success Rate: Pipeline success percentages
  - Data Quality: Quality score thresholds
  - Data Freshness: Data age limits

#### `monitoring/alert_manager.py`
**Purpose**: Manages alerts and notifications for system events.

**Key Features**:
- Multiple notification channels (Email, Slack, SMS)
- Alert prioritization
- Escalation policies
- Alert history tracking

**Main Class**: `AlertManager`
- **Alert Types**:
  - Pipeline failures
  - SLA violations
  - System errors
  - Performance issues

#### `monitoring/audit_logger.py`
**Purpose**: Comprehensive audit logging for compliance and debugging.

**Key Features**:
- Structured logging
- Audit trail maintenance
- Compliance reporting
- Debug information capture

#### `monitoring/performance_monitor.py`
**Purpose**: Monitors system performance and resource usage.

**Key Features**:
- CPU and memory monitoring
- Execution time tracking
- Resource utilization alerts
- Performance trend analysis

### 6. Utilities Layer

#### `utils/database_utils.py`
**Purpose**: Common database operations and connection management.

**Key Features**:
- Multi-database support (PostgreSQL, MySQL, SQLite)
- Connection pooling
- Query execution utilities
- Transaction management

**Main Classes**:
- `DatabaseUtils`: Core database operations
- `ConnectionConfig`: Connection configuration
- `DatabaseType`: Supported database types

**Supported Operations**:
- Connection management
- Query execution
- Bulk operations
- Schema introspection

#### `utils/security_manager.py`
**Purpose**: Comprehensive security management for the ETL framework.

**Key Features**:
- Data encryption/decryption
- Credential management
- Access control
- Audit logging
- Secret storage integration

**Security Components**:
- **EncryptionManager**: Data encryption services
- **CredentialManager**: Secure credential storage
- **AccessControl**: Role-based access control
- **AuditLogger**: Security event logging

**Supported Secret Stores**:
- AWS Secrets Manager
- Azure Key Vault
- HashiCorp Vault
- Local encrypted storage

#### `utils/error_recovery.py`
**Purpose**: Error handling and recovery mechanisms.

**Key Features**:
- Error classification
- Recovery strategies
- Retry logic with backoff
- Error reporting and alerting

#### `utils/incremental_load_manager.py`
**Purpose**: Manages incremental data loading with change detection.

**Key Features**:
- Change data capture
- Incremental sync strategies
- State management
- Conflict resolution

**Incremental Methods**:
- Timestamp-based
- Change tracking
- Log-based CDC
- Trigger-based

#### `utils/schema_manager.py`
**Purpose**: Database schema management and migrations.

**Key Features**:
- Schema versioning
- Migration management
- Schema validation
- Metadata management

#### `utils/data_lineage_tracker.py`
**Purpose**: Tracks data lineage and dependencies.

**Key Features**:
- Data flow tracking
- Dependency mapping
- Impact analysis
- Lineage visualization

## Database Schema

The framework uses the following database tables:

### Core Tables
- `PIPELINES`: Pipeline definitions and metadata
- `PIPELINE_RUNS`: Execution history and status
- `DATA_SOURCES`: Source system configurations
- `DATA_TARGETS`: Target system configurations

### Quality Tables
- `QUALITY_SCORES`: Data quality measurements
- `PROFILING_STATS`: Data profiling results
- `COMPLETENESS_CHECKS`: Completeness validation results
- `VALIDITY_CHECKS`: Validity validation results
- `UNIQUENESS_CHECKS`: Uniqueness validation results
- `ANOMALY_DETECTION`: Anomaly detection results

### Monitoring Tables
- `SLA_DEFINITIONS`: SLA configuration
- `SLA_COMPLIANCE`: SLA compliance records
- `ALERTS`: System alerts and notifications
- `AUDIT_LOG`: Audit trail entries

### Security Tables
- `USERS`: User accounts
- `ROLES`: User roles and permissions
- `CREDENTIALS`: Encrypted credentials
- `ACCESS_LOG`: Access control events

## Configuration

### Pipeline Configuration Structure
```yaml
pipeline:
  id: "customer_etl"
  name: "Customer Data ETL"
  description: "Extract and transform customer data"
  sources:
    - id: "customer_db"
      type: "postgresql"
      connection: "customer_db_conn"
      query: "SELECT * FROM customers WHERE updated_at > '{{last_run}}'"
  transforms:
    - id: "clean_data"
      type: "filter"
      params:
        condition: "age > 0 AND email IS NOT NULL"
    - id: "aggregate"
      type: "aggregate"
      params:
        groupby_cols: ["region"]
        agg_functions:
          revenue: "sum"
  targets:
    - id: "warehouse"
      type: "postgresql"
      connection: "warehouse_conn"
      table: "customer_summary"
  quality:
    rules:
      - type: "completeness"
        column: "email"
        threshold: 95
      - type: "validity"
        column: "email"
        type: "pattern"
        pattern: "^[^@]+@[^@]+\\.[^@]+$"
  sla:
    max_duration_minutes: 60
    min_success_rate: 0.95
```

## Usage Examples

### Basic Pipeline Execution
```python
from orchestrator.orchestrator_manager import OrchestratorManager

# Initialize components
orchestrator = OrchestratorManager(
    config_loader=config_loader,
    connector_factory=connector_factory,
    # ... other dependencies
)

# Execute pipeline
result = orchestrator.execute_pipeline("customer_etl", "manual", "user123")
print(f"Pipeline execution completed with status: {result['status']}")
```

### Data Quality Validation
```python
from quality.dq_engine import DQEngine

dq_engine = DQEngine(db_connection, config_loader)
results = dq_engine.execute_dq_validation(data, "customer_profile", run_id)
print(f"Quality score: {results['quality_score']}")
```

### Custom Transformation
```python
from transform.transform_engine import TransformEngine

engine = TransformEngine(db_connection)

# Define transformation
transform_config = {
    "id": "custom_transform",
    "steps": [
        {
            "type": "filter",
            "params": {"condition": "status == 'active'"}
        },
        {
            "type": "map",
            "params": {
                "mapping": {"old_column": "new_column"}
            }
        }
    ]
}

result = engine.execute_transformations(data, transform_config, "pandas")
```

## Best Practices

### Pipeline Design
1. **Modular Design**: Break complex pipelines into smaller, reusable components
2. **Error Handling**: Implement comprehensive error handling and recovery
3. **Monitoring**: Enable detailed logging and monitoring for all pipelines
4. **Testing**: Thoroughly test pipelines with various data scenarios

### Data Quality
1. **Early Validation**: Validate data quality as early as possible in the pipeline
2. **Progressive Quality**: Implement quality gates at each transformation step
3. **Monitoring**: Continuously monitor data quality metrics
4. **Feedback Loops**: Use quality metrics to improve data sources

### Security
1. **Encryption**: Encrypt sensitive data at rest and in transit
2. **Access Control**: Implement role-based access control
3. **Audit Logging**: Maintain comprehensive audit trails
4. **Credential Management**: Use secure credential storage systems

### Performance
1. **Engine Selection**: Choose appropriate processing engines for data size
2. **Resource Management**: Monitor and optimize resource usage
3. **Parallel Processing**: Leverage parallel processing where appropriate
4. **Caching**: Implement caching for frequently accessed data

## Troubleshooting

### Common Issues

#### Connection Failures
- Check connection configurations
- Verify network connectivity
- Review authentication credentials
- Check firewall and security group settings

#### Memory Issues
- Monitor memory usage during pipeline execution
- Consider using appropriate processing engines for large datasets
- Implement data partitioning for large volumes
- Review transformation logic for memory efficiency

#### Performance Problems
- Profile pipeline execution to identify bottlenecks
- Optimize queries and transformations
- Consider parallel processing options
- Review system resource allocation

#### Data Quality Issues
- Review data quality rules and thresholds
- Check data source quality
- Implement data cleansing steps
- Monitor quality metrics over time

## API Reference

### REST API Endpoints

#### Pipeline Management
- `GET /api/v1/pipelines` - List all pipelines
- `GET /api/v1/pipelines/{id}` - Get pipeline details
- `POST /api/v1/pipelines` - Create new pipeline
- `PUT /api/v1/pipelines/{id}` - Update pipeline
- `DELETE /api/v1/pipelines/{id}` - Delete pipeline

#### Execution Management
- `POST /api/v1/pipelines/{id}/execute` - Execute pipeline
- `GET /api/v1/runs/{id}` - Get execution details
- `GET /api/v1/runs/{id}/status` - Get execution status
- `POST /api/v1/runs/{id}/cancel` - Cancel execution

#### Monitoring
- `GET /api/v1/health` - System health check
- `GET /api/v1/metrics` - System metrics
- `GET /api/v1/alerts` - Recent alerts

### CLI Commands

```bash
# Execute pipeline
python -m api.cli execute --pipeline-id customer_etl

# Validate configuration
python -m api.cli validate --config-file pipeline.yaml

# Check system status
python -m api.cli status

# List pipelines
python -m api.cli list-pipelines
```

## Contributing

### Code Standards
1. Follow PEP 8 style guidelines
2. Include comprehensive docstrings
3. Write unit tests for all new functionality
4. Update documentation for API changes

### Testing
1. Unit tests for individual components
2. Integration tests for component interactions
3. Performance tests for scalability validation
4. Data validation tests for quality assurance

### Documentation
1. Update module docstrings for code changes
2. Maintain API documentation
3. Provide usage examples
4. Document configuration options

---

*This knowledge base is maintained as part of the ETL Framework documentation. Last updated: December 2025*</content>
<parameter name="filePath">d:\development\2026\metadata-etl-framework\ETL_Framework_Knowledge_Base.md