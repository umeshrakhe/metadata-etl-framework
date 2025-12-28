import logging
import time
import random
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable, Tuple, Union
from dataclasses import dataclass, asdict
from enum import Enum
import traceback
import functools
import json
import uuid

# Optional imports
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False

logger = logging.getLogger("ErrorRecoveryManager")


class ErrorType(Enum):
    """Classification of error types."""
    TRANSIENT = "transient"      # Temporary issues (network, locks)
    CONFIGURATION = "config"     # Setup/configuration problems
    DATA = "data"               # Data quality/format issues
    SYSTEM = "system"           # Infrastructure/system problems
    UNKNOWN = "unknown"         # Unclassified errors


class RetryStrategy(Enum):
    """Retry strategy types."""
    FIXED_DELAY = "fixed_delay"
    EXPONENTIAL_BACKOFF = "exponential_backoff"
    JITTER = "jitter"
    CIRCUIT_BREAKER = "circuit_breaker"


class CheckpointStrategy(Enum):
    """Checkpoint strategy types."""
    BATCH_LEVEL = "batch_level"
    ROW_LEVEL = "row_level"
    TIME_BASED = "time_based"
    CUSTOM = "custom"


@dataclass
class ErrorDetails:
    """Detailed error information."""
    error_id: str
    run_id: str
    pipeline_id: str
    step: str
    error_type: ErrorType
    error_message: str
    stack_trace: str
    context: Dict[str, Any]
    retry_count: int
    timestamp: datetime
    resolved: bool = False
    resolution_action: Optional[str] = None


@dataclass
class Checkpoint:
    """Progress checkpoint for recovery."""
    checkpoint_id: str
    run_id: str
    pipeline_id: str
    step: str
    checkpoint_data: Dict[str, Any]
    checkpoint_type: str
    timestamp: datetime
    batch_number: Optional[int] = None
    row_number: Optional[int] = None


@dataclass
class QuarantinedRecord:
    """Quarantined problematic record."""
    quarantine_id: str
    run_id: str
    pipeline_id: str
    record_data: Dict[str, Any]
    error_reason: str
    error_type: ErrorType
    quarantined_at: datetime
    retry_count: int = 0
    resolved: bool = False
    resolution_action: Optional[str] = None


@dataclass
class CircuitBreakerState:
    """Circuit breaker state."""
    service_name: str
    failure_count: int
    last_failure_time: Optional[datetime]
    state: str  # closed, open, half_open
    next_attempt_time: Optional[datetime]


@dataclass
class ErrorPattern:
    """Error pattern analysis."""
    pattern_id: str
    pipeline_id: str
    error_type: ErrorType
    error_message_pattern: str
    frequency: int
    first_seen: datetime
    last_seen: datetime
    avg_resolution_time: Optional[float]
    recommended_action: str


class ErrorRecoveryManager:
    """
    Comprehensive error recovery and retry logic for ETL pipelines.
    Handles error classification, retry strategies, circuit breakers, and recovery mechanisms.
    """

    def __init__(self, db_connection: Any = None, config: Optional[Dict[str, Any]] = None):
        self.db = db_connection
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)

        # Default configurations
        self.default_max_retries = self.config.get('max_retries', 3)
        self.default_backoff_factor = self.config.get('backoff_factor', 2.0)
        self.default_initial_delay = self.config.get('initial_delay', 1.0)
        self.default_max_delay = self.config.get('max_delay', 300.0)
        self.default_jitter_range = self.config.get('jitter_range', 0.1)

        # Circuit breaker settings
        self.circuit_breaker_failure_threshold = self.config.get('circuit_breaker_threshold', 5)
        self.circuit_breaker_timeout = self.config.get('circuit_breaker_timeout', 300)  # seconds

        # Checkpoint settings
        self.checkpoint_batch_size = self.config.get('checkpoint_batch_size', 1000)
        self.checkpoint_interval_minutes = self.config.get('checkpoint_interval_minutes', 5)

        # Circuit breaker states
        self.circuit_breakers: Dict[str, CircuitBreakerState] = {}

        # Thread locks for thread safety
        self.circuit_breaker_lock = threading.Lock()

    def execute_with_retry(self, function: Callable, max_retries: Optional[int] = None,
                          backoff_factor: Optional[float] = None, retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF,
                          *args, **kwargs) -> Any:
        """Execute a function with retry logic."""
        max_retries = max_retries or self.default_max_retries
        backoff_factor = backoff_factor or self.default_backoff_factor

        last_exception = None

        for attempt in range(max_retries + 1):
            try:
                return function(*args, **kwargs)
            except Exception as e:
                last_exception = e
                error_type = self.classify_error(e)

                if attempt < max_retries and self.should_retry(error_type, attempt):
                    delay = self.calculate_backoff_delay(attempt, backoff_factor, retry_strategy)
                    self.logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay:.2f} seconds...")
                    time.sleep(delay)
                else:
                    break

        # All retries exhausted
        self.logger.error(f"All {max_retries + 1} attempts failed. Last error: {last_exception}")
        raise last_exception

    def classify_error(self, exception: Exception) -> ErrorType:
        """Classify an exception into error categories."""
        error_message = str(exception).lower()
        exception_type = type(exception).__name__.lower()

        # Transient errors
        transient_patterns = [
            'timeout', 'connection lost', 'connection reset', 'connection refused',
            'lock timeout', 'deadlock', 'temporary failure', 'service unavailable',
            'network', 'socket', 'http 5', '502', '503', '504'
        ]

        # Configuration errors
        config_patterns = [
            'invalid config', 'missing credential', 'permission denied', 'access denied',
            'authentication failed', 'unauthorized', 'invalid key', 'config error',
            'no such file', 'file not found', 'directory not found', 'credentials provided'
        ]

        # Data errors
        data_patterns = [
            'constraint violation', 'foreign key', 'unique constraint', 'check constraint',
            'invalid format', 'type mismatch', 'value error', 'data error',
            'parsing error', 'validation error', 'schema mismatch'
        ]

        # System errors
        system_patterns = [
            'out of memory', 'disk full', 'no space left', 'database down',
            'server error', 'internal error', 'system error', 'memory error',
            'ioerror', 'oserror', 'broken pipe'
        ]

        # Check patterns
        if any(pattern in error_message or pattern in exception_type for pattern in transient_patterns):
            return ErrorType.TRANSIENT
        elif any(pattern in error_message or pattern in exception_type for pattern in config_patterns):
            return ErrorType.CONFIGURATION
        elif any(pattern in error_message or pattern in exception_type for pattern in data_patterns):
            return ErrorType.DATA
        elif any(pattern in error_message or pattern in exception_type for pattern in system_patterns):
            return ErrorType.SYSTEM
        else:
            return ErrorType.UNKNOWN

    def should_retry(self, error_type: ErrorType, retry_count: int) -> bool:
        """Determine if an error should be retried."""
        # Don't retry configuration or data errors
        if error_type in [ErrorType.CONFIGURATION, ErrorType.DATA]:
            return False

        # Don't retry system errors after a few attempts
        if error_type == ErrorType.SYSTEM and retry_count >= 1:
            return False

        # Retry transient and unknown errors
        return error_type in [ErrorType.TRANSIENT, ErrorType.UNKNOWN]

    def calculate_backoff_delay(self, retry_count: int, backoff_factor: float,
                               strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF) -> float:
        """Calculate delay for retry based on strategy."""
        if strategy == RetryStrategy.FIXED_DELAY:
            delay = self.default_initial_delay
        elif strategy == RetryStrategy.EXPONENTIAL_BACKOFF:
            delay = self.default_initial_delay * (backoff_factor ** retry_count)
        elif strategy == RetryStrategy.JITTER:
            base_delay = self.default_initial_delay * (backoff_factor ** retry_count)
            jitter = random.uniform(-self.default_jitter_range, self.default_jitter_range)
            delay = base_delay * (1 + jitter)
        else:
            delay = self.default_initial_delay

        # Cap the delay
        return min(delay, self.default_max_delay)

    def implement_circuit_breaker(self, function: Callable, service_name: str,
                                failure_threshold: Optional[int] = None,
                                timeout: Optional[int] = None, *args, **kwargs) -> Any:
        """Execute function with circuit breaker pattern."""
        failure_threshold = failure_threshold or self.circuit_breaker_failure_threshold
        timeout = timeout or self.circuit_breaker_timeout

        with self.circuit_breaker_lock:
            if service_name not in self.circuit_breakers:
                self.circuit_breakers[service_name] = CircuitBreakerState(
                    service_name=service_name,
                    failure_count=0,
                    last_failure_time=None,
                    state="closed",
                    next_attempt_time=None
                )

            breaker = self.circuit_breakers[service_name]

            # Check if circuit is open
            if breaker.state == "open":
                if breaker.next_attempt_time and datetime.utcnow() < breaker.next_attempt_time:
                    raise Exception(f"Circuit breaker open for {service_name}")
                else:
                    # Half-open state
                    breaker.state = "half_open"

            try:
                result = function(*args, **kwargs)

                # Success - reset circuit breaker
                if breaker.state == "half_open":
                    breaker.state = "closed"
                    breaker.failure_count = 0
                    breaker.last_failure_time = None
                    breaker.next_attempt_time = None

                return result

            except Exception as e:
                # Failure - update circuit breaker
                breaker.failure_count += 1
                breaker.last_failure_time = datetime.utcnow()

                if breaker.failure_count >= failure_threshold:
                    breaker.state = "open"
                    breaker.next_attempt_time = datetime.utcnow() + timedelta(seconds=timeout)

                raise e

    def checkpoint_progress(self, run_id: str, step: str, progress_data: Dict[str, Any],
                          checkpoint_type: CheckpointStrategy = CheckpointStrategy.BATCH_LEVEL,
                          batch_number: Optional[int] = None, row_number: Optional[int] = None) -> str:
        """Save progress checkpoint for recovery."""
        checkpoint = Checkpoint(
            checkpoint_id=str(uuid.uuid4()),
            run_id=run_id,
            pipeline_id=progress_data.get('pipeline_id', ''),
            step=step,
            checkpoint_data=progress_data,
            checkpoint_type=checkpoint_type.value,
            timestamp=datetime.utcnow(),
            batch_number=batch_number,
            row_number=row_number
        )

        success = self._save_checkpoint_db(checkpoint)

        if success:
            self.logger.info(f"Checkpoint saved for run {run_id}, step {step}")
            return checkpoint.checkpoint_id
        else:
            self.logger.error(f"Failed to save checkpoint for run {run_id}")
            return ""

    def resume_from_checkpoint(self, run_id: str) -> Optional[Checkpoint]:
        """Resume processing from the last checkpoint."""
        checkpoint = self._get_last_checkpoint_db(run_id)

        if checkpoint:
            self.logger.info(f"Resuming from checkpoint for run {run_id}: step {checkpoint.step}")
            return checkpoint
        else:
            self.logger.info(f"No checkpoint found for run {run_id}")
            return None

    def quarantine_bad_records(self, run_id: str, bad_data: Union[List[Dict[str, Any]], 'pd.DataFrame'],
                             error_reason: str, error_type: ErrorType = ErrorType.DATA) -> str:
        """Quarantine problematic records for later analysis."""
        quarantine_id = str(uuid.uuid4())

        if isinstance(bad_data, list):
            records = bad_data
        elif PANDAS_AVAILABLE and isinstance(bad_data, pd.DataFrame):
            records = bad_data.to_dict('records')
        else:
            records = [bad_data] if isinstance(bad_data, dict) else []

        quarantined_count = 0

        for record in records:
            quarantined_record = QuarantinedRecord(
                quarantine_id=quarantine_id,
                run_id=run_id,
                pipeline_id="",  # Will be filled from context
                record_data=record,
                error_reason=error_reason,
                error_type=error_type,
                quarantined_at=datetime.utcnow()
            )

            if self._save_quarantined_record_db(quarantined_record):
                quarantined_count += 1

        self.logger.info(f"Quarantined {quarantined_count} records for run {run_id}")
        return quarantine_id

    def reprocess_quarantined_records(self, run_id: str, quarantine_id: str) -> Dict[str, int]:
        """Reprocess quarantined records with fixes."""
        records = self._get_quarantined_records_db(run_id, quarantine_id)

        metrics = {"successful": 0, "failed": 0, "total": len(records)}

        for record in records:
            try:
                # Attempt to reprocess (this would be pipeline-specific)
                # For now, just mark as resolved
                self._update_quarantined_record_db(record.quarantine_id, resolved=True,
                                                 resolution_action="reprocessed")
                metrics["successful"] += 1

            except Exception as e:
                self.logger.exception(f"Failed to reprocess quarantined record: {e}")
                record.retry_count += 1
                self._update_quarantined_record_db(record.quarantine_id, retry_count=record.retry_count)
                metrics["failed"] += 1

        self.logger.info(f"Reprocessed quarantined records: {metrics}")
        return metrics

    def get_error_patterns(self, pipeline_id: str, days: int = 7) -> List[ErrorPattern]:
        """Analyze error patterns and trends."""
        since_date = datetime.utcnow() - timedelta(days=days)

        patterns = self._analyze_error_patterns_db(pipeline_id, since_date)

        self.logger.info(f"Found {len(patterns)} error patterns for pipeline {pipeline_id}")
        return patterns

    def recommend_recovery_action(self, error_type: ErrorType, context: Dict[str, Any]) -> str:
        """Recommend recovery action based on error type and context."""
        recommendations = {
            ErrorType.TRANSIENT: [
                "Wait and retry with exponential backoff",
                "Check network connectivity",
                "Verify service availability",
                "Increase timeout settings"
            ],
            ErrorType.CONFIGURATION: [
                "Verify configuration files",
                "Check credentials and permissions",
                "Validate connection strings",
                "Review environment variables"
            ],
            ErrorType.DATA: [
                "Validate data formats and schemas",
                "Check for data quality issues",
                "Review data transformation logic",
                "Implement data cleansing steps"
            ],
            ErrorType.SYSTEM: [
                "Check system resources (CPU, memory, disk)",
                "Verify database connectivity",
                "Review system logs",
                "Consider scaling resources"
            ],
            ErrorType.UNKNOWN: [
                "Review error details and stack trace",
                "Check application logs",
                "Consider code review",
                "Implement additional error handling"
            ]
        }

        actions = recommendations.get(error_type, recommendations[ErrorType.UNKNOWN])
        return actions[0] if actions else "Investigate error details"

    def escalate_error(self, error: Exception, escalation_level: str = "warning") -> None:
        """Escalate error to appropriate level."""
        error_details = {
            "error": str(error),
            "escalation_level": escalation_level,
            "timestamp": datetime.utcnow().isoformat()
        }

        if escalation_level == "critical":
            self.logger.critical(f"Critical error escalated: {error}", extra=error_details)
            # Could send alerts, notifications, etc.
        elif escalation_level == "warning":
            self.logger.warning(f"Warning error escalated: {error}", extra=error_details)
        else:
            self.logger.error(f"Error escalated: {error}", extra=error_details)

    def log_error_details(self, run_id: str, error: Exception, context: Dict[str, Any],
                         stack_trace: Optional[str] = None) -> str:
        """Log detailed error information."""
        error_type = self.classify_error(error)

        error_details = ErrorDetails(
            error_id=str(uuid.uuid4()),
            run_id=run_id,
            pipeline_id=context.get('pipeline_id', ''),
            step=context.get('step', ''),
            error_type=error_type,
            error_message=str(error),
            stack_trace=stack_trace or traceback.format_exc(),
            context=context,
            retry_count=context.get('retry_count', 0),
            timestamp=datetime.utcnow()
        )

        success = self._save_error_details_db(error_details)

        if success:
            self.logger.error(f"Error logged: {error_details.error_id} - {error}")
            return error_details.error_id
        else:
            self.logger.error(f"Failed to log error: {error}")
            return ""

    def notify_on_repeated_failure(self, pipeline_id: str, failure_count: int,
                                 threshold: int = 3) -> bool:
        """Notify when repeated failures occur."""
        if failure_count >= threshold:
            self.logger.warning(f"Pipeline {pipeline_id} has {failure_count} repeated failures")
            # Could send notifications, alerts, etc.
            return True
        return False

    def implement_fallback_strategy(self, primary_action: Callable, fallback_action: Callable,
                                  *args, **kwargs) -> Any:
        """Execute primary action with fallback strategy."""
        try:
            return primary_action(*args, **kwargs)
        except Exception as primary_error:
            self.logger.warning(f"Primary action failed: {primary_error}. Trying fallback...")

            try:
                return fallback_action(*args, **kwargs)
            except Exception as fallback_error:
                self.logger.error(f"Fallback action also failed: {fallback_error}")
                raise primary_error  # Raise original error

    def get_error_budget_status(self, pipeline_id: str, time_window_days: int = 30) -> Dict[str, Any]:
        """Get error budget status for a pipeline."""
        since_date = datetime.utcnow() - timedelta(days=time_window_days)

        # Get error statistics
        error_stats = self._get_error_statistics_db(pipeline_id, since_date)

        # Calculate error budget (example: 5% error rate allowed)
        total_operations = error_stats.get('total_operations', 1)
        error_count = error_stats.get('error_count', 0)
        error_rate = (error_count / total_operations) * 100

        budget_status = {
            "pipeline_id": pipeline_id,
            "time_window_days": time_window_days,
            "total_operations": total_operations,
            "error_count": error_count,
            "error_rate_percent": error_rate,
            "budget_limit_percent": 5.0,
            "budget_remaining_percent": max(0, 5.0 - error_rate),
            "budget_exceeded": error_rate > 5.0
        }

        return budget_status

    def analyze_failure_rate(self, pipeline_id: str, hours: int = 24) -> Dict[str, Any]:
        """Analyze failure rate trends."""
        since_time = datetime.utcnow() - timedelta(hours=hours)

        failure_analysis = self._analyze_failure_rates_db(pipeline_id, since_time)

        # Calculate trends
        recent_failures = failure_analysis.get('recent_failures', [])
        if len(recent_failures) >= 2:
            # Simple trend analysis
            first_half = sum(recent_failures[:len(recent_failures)//2])
            second_half = sum(recent_failures[len(recent_failures)//2:])

            trend = "increasing" if second_half > first_half else "decreasing" if second_half < first_half else "stable"
            failure_analysis["trend"] = trend

        return failure_analysis

    # Decorator for automatic retry
    def retry_on_failure(self, max_retries: Optional[int] = None,
                        backoff_factor: Optional[float] = None,
                        retry_strategy: RetryStrategy = RetryStrategy.EXPONENTIAL_BACKOFF):
        """Decorator to add retry logic to functions."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return self.execute_with_retry(
                    func, max_retries, backoff_factor, retry_strategy, *args, **kwargs
                )
            return wrapper
        return decorator

    # Decorator for circuit breaker
    def circuit_breaker(self, service_name: str, failure_threshold: Optional[int] = None,
                       timeout: Optional[int] = None):
        """Decorator to add circuit breaker to functions."""
        def decorator(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return self.implement_circuit_breaker(
                    func, service_name, failure_threshold, timeout, *args, **kwargs
                )
            return wrapper
        return decorator

    # Private helper methods

    def _save_error_details_db(self, error_details: ErrorDetails) -> bool:
        """Save error details to database."""
        if not self.db:
            return False

        try:
            query = """
                INSERT INTO ERROR_DETAILS (
                    error_id, run_id, pipeline_id, step, error_type, error_message,
                    stack_trace, context, retry_count, timestamp, resolved, resolution_action
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            params = (
                error_details.error_id,
                error_details.run_id,
                error_details.pipeline_id,
                error_details.step,
                error_details.error_type.value,
                error_details.error_message,
                error_details.stack_trace,
                json.dumps(error_details.context),
                error_details.retry_count,
                error_details.timestamp,
                error_details.resolved,
                error_details.resolution_action
            )

            self.db.begin()
            self.db.execute(query, params)
            self.db.commit()

            return True

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Database error saving error details: {e}")
            return False

    def _save_checkpoint_db(self, checkpoint: Checkpoint) -> bool:
        """Save checkpoint to database."""
        if not self.db:
            return False

        try:
            query = """
                INSERT INTO CHECKPOINTS (
                    checkpoint_id, run_id, pipeline_id, step, checkpoint_data,
                    checkpoint_type, timestamp, batch_number, row_number
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            params = (
                checkpoint.checkpoint_id,
                checkpoint.run_id,
                checkpoint.pipeline_id,
                checkpoint.step,
                json.dumps(checkpoint.checkpoint_data),
                checkpoint.checkpoint_type,
                checkpoint.timestamp,
                checkpoint.batch_number,
                checkpoint.row_number
            )

            self.db.begin()
            self.db.execute(query, params)
            self.db.commit()

            return True

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Database error saving checkpoint: {e}")
            return False

    def _get_last_checkpoint_db(self, run_id: str) -> Optional[Checkpoint]:
        """Get last checkpoint from database."""
        if not self.db:
            return None

        try:
            query = """
                SELECT checkpoint_id, run_id, pipeline_id, step, checkpoint_data,
                       checkpoint_type, timestamp, batch_number, row_number
                FROM CHECKPOINTS
                WHERE run_id = %s
                ORDER BY timestamp DESC LIMIT 1
            """

            self.db.execute(query, (run_id,))
            row = self.db.fetchone()

            if row:
                return Checkpoint(
                    checkpoint_id=row[0],
                    run_id=row[1],
                    pipeline_id=row[2],
                    step=row[3],
                    checkpoint_data=json.loads(row[4]) if row[4] else {},
                    checkpoint_type=row[5],
                    timestamp=row[6],
                    batch_number=row[7],
                    row_number=row[8]
                )

        except Exception as e:
            self.logger.exception(f"Database error getting checkpoint: {e}")

        return None

    def _save_quarantined_record_db(self, record: QuarantinedRecord) -> bool:
        """Save quarantined record to database."""
        if not self.db:
            return False

        try:
            query = """
                INSERT INTO QUARANTINED_RECORDS (
                    quarantine_id, run_id, pipeline_id, record_data, error_reason,
                    error_type, quarantined_at, retry_count, resolved, resolution_action
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            params = (
                record.quarantine_id,
                record.run_id,
                record.pipeline_id,
                json.dumps(record.record_data),
                record.error_reason,
                record.error_type.value,
                record.quarantined_at,
                record.retry_count,
                record.resolved,
                record.resolution_action
            )

            self.db.begin()
            self.db.execute(query, params)
            self.db.commit()

            return True

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Database error saving quarantined record: {e}")
            return False

    def _get_quarantined_records_db(self, run_id: str, quarantine_id: str) -> List[QuarantinedRecord]:
        """Get quarantined records from database."""
        if not self.db:
            return []

        try:
            query = """
                SELECT quarantine_id, run_id, pipeline_id, record_data, error_reason,
                       error_type, quarantined_at, retry_count, resolved, resolution_action
                FROM QUARANTINED_RECORDS
                WHERE run_id = %s AND quarantine_id = %s AND resolved = FALSE
            """

            self.db.execute(query, (run_id, quarantine_id))
            rows = self.db.fetchall()

            records = []
            for row in rows:
                records.append(QuarantinedRecord(
                    quarantine_id=row[0],
                    run_id=row[1],
                    pipeline_id=row[2],
                    record_data=json.loads(row[3]) if row[3] else {},
                    error_reason=row[4],
                    error_type=ErrorType(row[5]),
                    quarantined_at=row[6],
                    retry_count=row[7],
                    resolved=row[8],
                    resolution_action=row[9]
                ))

            return records

        except Exception as e:
            self.logger.exception(f"Database error getting quarantined records: {e}")
            return []

    def _update_quarantined_record_db(self, quarantine_id: str, resolved: bool = False,
                                    retry_count: int = 0, resolution_action: Optional[str] = None) -> bool:
        """Update quarantined record in database."""
        if not self.db:
            return False

        try:
            query = """
                UPDATE QUARANTINED_RECORDS
                SET resolved = %s, retry_count = %s, resolution_action = %s
                WHERE quarantine_id = %s
            """

            params = (resolved, retry_count, resolution_action, quarantine_id)

            self.db.begin()
            self.db.execute(query, params)
            self.db.commit()

            return True

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Database error updating quarantined record: {e}")
            return False

    def _analyze_error_patterns_db(self, pipeline_id: str, since_date: datetime) -> List[ErrorPattern]:
        """Analyze error patterns from database."""
        if not self.db:
            return []

        try:
            query = """
                SELECT error_type, error_message, COUNT(*) as frequency,
                       MIN(timestamp) as first_seen, MAX(timestamp) as last_seen
                FROM ERROR_DETAILS
                WHERE pipeline_id = %s AND timestamp >= %s
                GROUP BY error_type, LEFT(error_message, 100)
                HAVING COUNT(*) > 1
                ORDER BY frequency DESC
            """

            self.db.execute(query, (pipeline_id, since_date))
            rows = self.db.fetchall()

            patterns = []
            for row in rows:
                patterns.append(ErrorPattern(
                    pattern_id=str(uuid.uuid4()),
                    pipeline_id=pipeline_id,
                    error_type=ErrorType(row[0]),
                    error_message_pattern=row[1][:100],
                    frequency=row[2],
                    first_seen=row[3],
                    last_seen=row[4],
                    avg_resolution_time=None,  # Would need more complex query
                    recommended_action=self.recommend_recovery_action(ErrorType(row[0]), {})
                ))

            return patterns

        except Exception as e:
            self.logger.exception(f"Database error analyzing error patterns: {e}")
            return []

    def _get_error_statistics_db(self, pipeline_id: str, since_date: datetime) -> Dict[str, Any]:
        """Get error statistics from database."""
        if not self.db:
            return {}

        try:
            # This would need actual execution metrics table
            # For now, return mock data
            return {
                "total_operations": 10000,
                "error_count": 150,
                "error_rate": 1.5
            }

        except Exception as e:
            self.logger.exception(f"Database error getting error statistics: {e}")
            return {}

    def _analyze_failure_rates_db(self, pipeline_id: str, since_time: datetime) -> Dict[str, Any]:
        """Analyze failure rates from database."""
        if not self.db:
            return {}

        try:
            # This would need actual metrics
            # For now, return mock data
            return {
                "pipeline_id": pipeline_id,
                "time_window_hours": 24,
                "total_runs": 50,
                "failed_runs": 5,
                "failure_rate": 10.0,
                "recent_failures": [1, 0, 1, 2, 1],  # Mock hourly failures
                "trend": "stable"
            }

        except Exception as e:
            self.logger.exception(f"Database error analyzing failure rates: {e}")
            return {}