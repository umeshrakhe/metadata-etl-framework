# Error Recovery Manager

The ErrorRecoveryManager class provides comprehensive error handling, retry logic, and recovery mechanisms for ETL pipelines, including error classification, circuit breaker patterns, checkpoint recovery, and failure analysis.

## Features

### Error Classification
- **Transient**: Network timeouts, connection issues, temporary failures
- **Configuration**: Invalid credentials, missing permissions, config errors
- **Data**: Format issues, constraint violations, type mismatches
- **System**: Resource exhaustion, infrastructure failures
- **Unknown**: Unclassified errors

### Retry Strategies
- **Fixed Delay**: Constant wait time between retries
- **Exponential Backoff**: Increasing delay with exponential growth
- **Jitter**: Random variation to prevent thundering herd problems
- **Circuit Breaker**: Stop retries after failure threshold

### Recovery Mechanisms
- **Checkpoint Recovery**: Resume from saved progress points
- **Record Quarantine**: Isolate problematic data for later processing
- **Fallback Strategies**: Alternative actions when primary fails
- **Error Escalation**: Raise alerts for critical failures

### Monitoring & Analysis
- **Error Pattern Analysis**: Identify recurring failure patterns
- **Error Budget Tracking**: Monitor error rates against acceptable limits
- **Failure Rate Analysis**: Trend analysis of pipeline reliability
- **Recovery Action Recommendations**: Automated suggestions for fixes

## Usage Examples

### Basic Retry Logic

```python
from utils.error_recovery import ErrorRecoveryManager, RetryStrategy

manager = ErrorRecoveryManager()

def unreliable_operation():
    # Operation that might fail
    return api_call_that_might_fail()

# Execute with retry
result = manager.execute_with_retry(
    unreliable_operation,
    max_retries=3,
    backoff_factor=2.0,
    retry_strategy=RetryStrategy.EXPONENTIAL_BACKOFF
)
```

### Circuit Breaker Pattern

```python
# Implement circuit breaker for external service
result = manager.implement_circuit_breaker(
    external_service_call,
    service_name="api_service",
    failure_threshold=5,
    timeout=300  # 5 minutes
)
```

### Error Classification and Handling

```python
try:
    risky_operation()
except Exception as e:
    error_type = manager.classify_error(e)

    if manager.should_retry(error_type, retry_count=0):
        # Retry the operation
        pass
    else:
        # Handle non-retryable error
        manager.log_error_details(run_id, e, context)
        raise
```

### Checkpoint and Recovery

```python
# Save progress checkpoint
checkpoint_id = manager.checkpoint_progress(
    run_id="pipeline_run_123",
    step="data_processing",
    progress_data={
        "records_processed": 50000,
        "last_id": 50000,
        "batch_number": 5
    }
)

# Resume from checkpoint
checkpoint = manager.resume_from_checkpoint("pipeline_run_123")
if checkpoint:
    # Continue processing from saved point
    start_from = checkpoint.checkpoint_data["last_id"]
```

### Record Quarantine

```python
# Quarantine problematic records
bad_records = [
    {"id": 1, "data": "invalid_format"},
    {"id": 2, "data": "constraint_violation"}
]

quarantine_id = manager.quarantine_bad_records(
    run_id="pipeline_run_123",
    bad_data=bad_records,
    error_reason="Data validation failed",
    error_type=ErrorType.DATA
)

# Reprocess quarantined records later
metrics = manager.reprocess_quarantined_records(
    run_id="pipeline_run_123",
    quarantine_id=quarantine_id
)
```

### Error Pattern Analysis

```python
# Analyze error patterns
patterns = manager.get_error_patterns(
    pipeline_id="data_pipeline",
    days=7
)

for pattern in patterns:
    print(f"Pattern: {pattern.error_message_pattern}")
    print(f"Frequency: {pattern.frequency}")
    print(f"Recommendation: {pattern.recommended_action}")
```

### Error Budget Monitoring

```python
# Check error budget status
budget_status = manager.get_error_budget_status(
    pipeline_id="critical_pipeline",
    time_window_days=30
)

if budget_status["budget_exceeded"]:
    print(f"Error budget exceeded: {budget_status['error_rate_percent']}%")
    # Take corrective action
```

### Fallback Strategies

```python
def primary_database_operation():
    return connect_to_primary_db().execute(query)

def fallback_database_operation():
    return connect_to_backup_db().execute(query)

# Execute with fallback
result = manager.implement_fallback_strategy(
    primary_database_operation,
    fallback_database_operation
)
```

### Using Decorators

```python
# Automatic retry decorator
@manager.retry_on_failure(max_retries=3, backoff_factor=1.5)
def api_call():
    return requests.get("https://api.example.com/data")

# Circuit breaker decorator
@manager.circuit_breaker("external_api", failure_threshold=3)
def external_service_call():
    return call_external_service()

# Use decorated functions normally
data = api_call()  # Automatic retry on failure
result = external_service_call()  # Circuit breaker protection
```

## Database Schema

The ErrorRecoveryManager requires several database tables:

- `ERROR_DETAILS`: Comprehensive error logging and tracking
- `CHECKPOINTS`: Progress checkpoints for recovery
- `QUARANTINED_RECORDS`: Isolated problematic records
- `CIRCUIT_BREAKER_STATES`: Service health tracking
- `ERROR_PATTERNS`: Pattern analysis and recommendations
- `RETRY_ATTEMPTS`: Retry attempt history
- `ERROR_BUDGET`: Error budget tracking
- `FAILURE_ANALYSIS`: Failure rate trend analysis
- `RECOVERY_ACTIONS`: Automated recovery tracking
- `SYSTEM_HEALTH`: System resource monitoring

See `database/error_recovery_schema.sql` for the complete schema.

## Configuration

Error recovery can be configured via the `config` parameter:

```python
config = {
    'max_retries': 3,              # Default retry attempts
    'backoff_factor': 2.0,         # Exponential backoff multiplier
    'initial_delay': 1.0,          # Initial retry delay (seconds)
    'max_delay': 300.0,            # Maximum retry delay (seconds)
    'jitter_range': 0.1,           # Jitter randomization factor
    'circuit_breaker_threshold': 5, # Failures before circuit opens
    'circuit_breaker_timeout': 300, # Circuit open duration (seconds)
    'checkpoint_batch_size': 1000,  # Records per checkpoint
    'checkpoint_interval_minutes': 5 # Time-based checkpoint interval
}
```

## Error Classification Logic

The manager automatically classifies errors based on message patterns:

- **Transient**: timeout, connection lost, lock timeout, temporary failure
- **Configuration**: invalid config, missing credential, permission denied
- **Data**: constraint violation, invalid format, type mismatch
- **System**: out of memory, disk full, database down
- **Unknown**: Any error not matching above patterns

## Retry Decision Matrix

| Error Type | Retry | Max Attempts | Reasoning |
|------------|-------|--------------|-----------|
| Transient | Yes | Unlimited | Temporary network/service issues |
| Configuration | No | 0 | Requires human intervention |
| Data | No | 0 | Data quality issues need fixing |
| System | Limited | 1-2 | May indicate infrastructure problems |
| Unknown | Yes | Default | Conservative approach |

## Circuit Breaker States

- **Closed**: Normal operation, requests pass through
- **Open**: Failure threshold exceeded, requests fail fast
- **Half-Open**: Testing if service recovered, limited requests

## Checkpoint Strategies

- **Batch Level**: Checkpoint after processing each batch
- **Row Level**: Checkpoint after N rows processed
- **Time Based**: Checkpoint every N minutes
- **Custom**: Application-defined checkpoint logic

## Best Practices

### Retry Configuration
1. **Start Conservative**: Begin with lower retry counts and backoff factors
2. **Monitor Impact**: Track retry overhead on system performance
3. **Use Jitter**: Prevent synchronized retry storms
4. **Set Timeouts**: Avoid indefinite hanging operations

### Circuit Breaker Usage
1. **Tune Thresholds**: Set appropriate failure thresholds for your services
2. **Monitor State**: Track circuit breaker state changes
3. **Fast Recovery**: Use half-open state to test recovery
4. **Alert on Opens**: Notify when circuits open frequently

### Checkpoint Strategy
1. **Balance Granularity**: More checkpoints = better recovery but slower performance
2. **Consider Data Volume**: Larger batches may need more frequent checkpoints
3. **Test Recovery**: Regularly test checkpoint recovery scenarios
4. **Monitor Storage**: Checkpoints consume storage space

### Error Handling
1. **Classify Errors**: Use error classification for appropriate handling
2. **Log Context**: Include sufficient context for debugging
3. **Quarantine Wisely**: Don't quarantine too much data
4. **Monitor Patterns**: Regularly review error patterns and trends

## Performance Considerations

- **Retry Overhead**: Each retry consumes resources and time
- **Circuit Breaker**: Fast-fail prevents resource waste
- **Checkpoint Frequency**: Balance recovery speed vs performance
- **Quarantine Storage**: Isolated records consume storage
- **Pattern Analysis**: Regular analysis may impact performance

## Integration with ETL Framework

The ErrorRecoveryManager integrates with other framework components:

- **OrchestratorManager**: Pipeline execution with error recovery
- **DataQualityEngine**: Data validation error handling
- **AuditLogger**: Error event logging and tracking
- **PerformanceMonitor**: Error impact on performance metrics
- **AlertManager**: Error notifications and escalations

## Monitoring and Alerting

The manager provides comprehensive monitoring:

- **Error Rates**: Track error frequency and trends
- **Recovery Success**: Monitor recovery action effectiveness
- **Circuit States**: Track service health status
- **Quarantine Growth**: Monitor quarantined data accumulation
- **Budget Compliance**: Alert on error budget violations

## Troubleshooting

### Common Issues

1. **Excessive Retries**: Tune retry parameters or improve error classification
2. **Circuit Stuck Open**: Check service health or adjust thresholds
3. **Large Checkpoints**: Reduce checkpoint frequency or data size
4. **Quarantine Growth**: Review data quality or quarantine criteria
5. **Pattern False Positives**: Refine error message pattern matching

### Debugging

Enable detailed logging for troubleshooting:

```python
import logging
logging.getLogger('ErrorRecoveryManager').setLevel(logging.DEBUG)
```

Check database tables for error details, retry attempts, and recovery actions.

## Dependencies

### Required
- `uuid`: For generating unique identifiers

### Optional
- `pandas`: For DataFrame-based record quarantine
- `psutil`: For system resource monitoring

Install optional dependencies as needed:

```bash
pip install pandas psutil
```

## Future Enhancements

- **Machine Learning**: Predictive error classification
- **Auto-tuning**: Dynamic retry parameter adjustment
- **Distributed Coordination**: Multi-node error coordination
- **Advanced Analytics**: Root cause analysis and prediction
- **Integration APIs**: Third-party monitoring system integration