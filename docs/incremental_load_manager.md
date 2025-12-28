# Incremental Load Manager

The IncrementalLoadManager class provides comprehensive incremental data loading capabilities for ETL pipelines, supporting multiple strategies including timestamp-based, sequence-based, CDC-based, snapshot-based, and delta lake loading.

## Features

### Incremental Strategies
- **Timestamp-based**: Track changes using last_modified_date or created_date columns
- **Sequence-based**: Track changes using auto-increment IDs or sequence numbers
- **CDC-based**: Capture insert/update/delete operations using Change Data Capture
- **Snapshot-based**: Full snapshot loading with date partitioning
- **Delta Lake**: Merge operations with deduplication for lakehouse architectures

### Watermark Management
- Persistent watermark storage in metadata tables
- Support for multiple watermark columns per source
- Timezone-aware watermark handling
- Watermark reset capabilities for reprocessing

### Data Consistency
- Late-arriving data detection and handling
- Duplicate record detection and removal
- Gap detection in incremental streams
- Completeness validation with configurable tolerances

### Advanced Loading Patterns
- Slowly Changing Dimension (SCD) Type 2 implementation
- Incremental merge/upsert operations
- Data correction handling for backdated updates
- Query optimization for incremental extraction

### Change Data Capture
- Log-based CDC integration
- Trigger-based CDC support
- API-based CDC for SaaS applications
- CDC event processing and application

## Usage Examples

### Basic Watermark Management

```python
from utils.incremental_load_manager import IncrementalLoadManager

manager = IncrementalLoadManager(db_connection=db)

# Get last watermark
watermark = manager.get_last_watermark("sales_pipeline", "orders_source")
last_value = watermark.watermark_value if watermark else None

# Update watermark after successful load
manager.update_watermark(
    pipeline_id="sales_pipeline",
    source_id="orders_source",
    new_watermark="2024-01-15T10:00:00",
    watermark_column="updated_at",
    strategy="timestamp"
)
```

### Timestamp-Based Incremental Loading

```python
# Configure timestamp-based incremental loading
config = {
    'incremental_strategy': 'timestamp',
    'table': 'user_events',
    'timestamp_column': 'event_time'
}

# Extract incremental data
data = manager.extract_incremental_data(config, watermark)

# Process the data
if data is not None and len(data) > 0:
    # Handle late arriving data
    on_time_data, late_count = manager.handle_late_arriving_data(
        data, 'event_time', watermark.watermark_value
    )

    # Merge into target
    metrics = manager.merge_incremental_load(
        target_table="target_events",
        data=on_time_data,
        merge_keys=['event_id'],
        load_type="upsert"
    )
```

### Sequence-Based Incremental Loading

```python
# Configure sequence-based incremental loading
config = {
    'incremental_strategy': 'sequence',
    'table': 'transactions',
    'sequence_column': 'transaction_id'
}

# Extract incremental data
data = manager.extract_incremental_data(config, watermark)
```

### Change Data Capture

```python
# Configure CDC source
cdc_config = {
    'cdc_type': 'log_based',
    'table': 'customer_data',
    'primary_key': 'customer_id'
}

# Setup CDC
manager.configure_cdc_source(cdc_config)

# Read CDC events
cdc_events = manager.read_cdc_stream(cdc_config, from_lsn="0x0001A5F")

# Apply changes to target
metrics = manager.apply_cdc_changes("target_customers", cdc_events)
```

### Slowly Changing Dimensions

```python
# Implement SCD Type 2
scd_data = pd.DataFrame({
    'customer_id': [1, 2, 3],
    'name': ['Alice', 'Bob', 'Charlie'],
    'address': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
    'effective_date': ['2024-01-15'] * 3
})

metrics = manager.implement_scd_type2(
    target_table="customer_scd",
    data=scd_data,
    business_key='customer_id',
    effective_date='effective_date'
)
```

### Data Quality and Validation

```python
# Validate completeness
validation = manager.validate_incremental_completeness(
    expected_count=1000,
    actual_count=950,
    tolerance_percent=5.0
)

if not validation['is_complete']:
    print(f"Load incomplete: {validation['difference_percent']}% missing")

# Detect gaps in data
gaps = manager.detect_data_gaps(
    pipeline_id="sales_pipeline",
    source_id="orders_source",
    watermark_column="order_date"
)

# Deduplicate data
clean_data, duplicates_removed = manager.deduplicate_data(
    data, key_columns=['customer_id', 'order_date']
)
```

### Query Optimization

```python
# Optimize incremental query
base_query = "SELECT * FROM sales_orders WHERE region = 'US'"
optimized_query = manager.optimize_incremental_query(
    base_query,
    watermark,
    partition_column="order_date"
)
```

## Database Schema

The IncrementalLoadManager requires several database tables:

- `WATERMARKS`: Incremental load progress tracking
- `INCREMENTAL_METRICS`: Load performance and statistics
- `CDC_EVENTS`: Change Data Capture event storage
- `DATA_GAPS`: Detected gaps in incremental streams
- `SCD_HISTORY`: Slowly Changing Dimension history
- `LATE_ARRIVING_DATA`: Out-of-order data handling
- `INCREMENTAL_CONFIG`: Pipeline configuration storage
- `DUPLICATE_RECORDS`: Duplicate tracking and resolution

See `database/incremental_schema.sql` for the complete schema.

## Configuration

Incremental loading can be configured via the `INCREMENTAL_CONFIG` table or directly in code:

```python
config = {
    'batch_size': 10000,              # Records per batch
    'timeout_seconds': 3600,          # Load timeout
    'enable_gap_detection': True,     # Detect data gaps
    'enable_duplicate_handling': True # Handle duplicates
}
```

## Incremental Strategies

### Timestamp-Based
Best for: Event data, transaction logs, any data with reliable timestamps
Pros: Flexible, handles out-of-order inserts
Cons: Requires accurate timestamp columns

### Sequence-Based
Best for: Auto-increment IDs, sequence generators
Pros: Simple, guaranteed ordering
Cons: Only works with sequential ID generation

### CDC-Based
Best for: Real-time data synchronization, audit requirements
Pros: Captures all changes, real-time
Cons: Complex setup, performance overhead

### Snapshot-Based
Best for: Partitioned data, daily snapshots
Pros: Simple partitioning, easy recovery
Cons: May miss intra-day changes

### Delta Lake
Best for: Lakehouse architectures, complex merges
Pros: ACID transactions, time travel
Cons: Requires Delta Lake infrastructure

## Error Handling

The IncrementalLoadManager includes comprehensive error handling:

- Graceful handling of missing optional dependencies
- Detailed logging of incremental operations
- Automatic retry logic for transient failures
- Data validation and consistency checks
- Rollback capabilities for failed loads

## Performance Optimization

- Query optimization with partition pruning
- Batch processing with configurable sizes
- Memory-efficient data processing
- Parallel loading capabilities
- Index recommendations for watermark columns

## Monitoring and Metrics

The manager provides comprehensive metrics:

- Records extracted/inserted/updated/deleted
- Processing time and throughput
- Data quality metrics (duplicates, gaps, late arrivals)
- Completeness validation results
- Performance benchmarks

## Integration with ETL Framework

The IncrementalLoadManager integrates with other framework components:

- **ConnectorFactory**: Database connections for source/target systems
- **OrchestratorManager**: Pipeline execution and scheduling
- **DataQualityEngine**: Validation of incremental data
- **AuditLogger**: Comprehensive audit trails
- **PerformanceMonitor**: Load performance tracking

## Best Practices

1. **Choose the Right Strategy**: Match incremental strategy to data characteristics
2. **Monitor Watermarks**: Regularly check watermark progress and detect stalls
3. **Handle Late Data**: Implement proper late-arriving data handling
4. **Validate Completeness**: Always validate load completeness
5. **Optimize Queries**: Use partition pruning and appropriate indexes
6. **Monitor Performance**: Track load times and resource usage
7. **Plan for Recovery**: Design for restartability and error recovery
8. **Audit Changes**: Maintain comprehensive audit trails

## Dependencies

### Required
- `pandas`: Data manipulation and analysis
- `pyarrow`: Arrow-based data processing (optional)

### Optional
- `polars`: Alternative DataFrame library for better performance

Install optional dependencies as needed:

```bash
pip install polars pyarrow
```

## Troubleshooting

### Common Issues

1. **Watermark Not Updating**: Check database permissions and transaction handling
2. **Late Arriving Data**: Adjust watermark delay windows or implement proper handling
3. **Performance Issues**: Optimize queries, increase batch sizes, add indexes
4. **Data Gaps**: Review source system change patterns and adjust gap detection
5. **Duplicate Records**: Implement proper deduplication logic and key selection

### Debugging

Enable detailed logging to troubleshoot issues:

```python
import logging
logging.getLogger('IncrementalLoadManager').setLevel(logging.DEBUG)
```

Check metrics and audit logs for detailed operation information.