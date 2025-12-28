#!/usr/bin/env python3
"""
Error Recovery Manager Example
Demonstrates the usage of the ErrorRecoveryManager class for retry logic,
error classification, circuit breakers, and recovery strategies.
"""

import logging
import sys
import os
import time
import random

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils.error_recovery import ErrorRecoveryManager, ErrorType, RetryStrategy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def demonstrate_error_classification():
    """Demonstrate error classification functionality."""
    print("\n=== Error Classification Demo ===")

    manager = ErrorRecoveryManager()

    # Test different types of errors
    test_errors = [
        ("Connection timeout occurred", ErrorType.TRANSIENT),
        ("Invalid credentials provided", ErrorType.CONFIGURATION),
        ("Foreign key constraint violation", ErrorType.DATA),
        ("Out of memory error", ErrorType.SYSTEM),
        ("Unknown error occurred", ErrorType.UNKNOWN)
    ]

    for error_msg, expected_type in test_errors:
        # Create a mock exception
        error = Exception(error_msg)
        classified_type = manager.classify_error(error)

        status = "✓" if classified_type == expected_type else "✗"
        print(f"{status} '{error_msg}' -> {classified_type.value} (expected: {expected_type.value})")


def demonstrate_retry_logic():
    """Demonstrate retry logic with different strategies."""
    print("\n=== Retry Logic Demo ===")

    manager = ErrorRecoveryManager()

    def failing_function(attempt_number):
        """Function that fails first few attempts."""
        if attempt_number < 3:
            raise Exception("Temporary network error")
        return f"Success on attempt {attempt_number}"

    # Test different retry strategies
    strategies = [
        ("Fixed Delay", RetryStrategy.FIXED_DELAY),
        ("Exponential Backoff", RetryStrategy.EXPONENTIAL_BACKOFF),
        ("Jitter", RetryStrategy.JITTER)
    ]

    for strategy_name, strategy in strategies:
        print(f"\nTesting {strategy_name}:")

        start_time = time.time()
        try:
            result = manager.execute_with_retry(
                failing_function, max_retries=5, backoff_factor=1.5,
                retry_strategy=strategy, attempt_number=1
            )
            elapsed = time.time() - start_time
            print(f"  ✓ Success: {result} (took {elapsed:.2f}s)")
        except Exception as e:
            elapsed = time.time() - start_time
            print(f"  ✗ Failed: {e} (took {elapsed:.2f}s)")


def demonstrate_circuit_breaker():
    """Demonstrate circuit breaker pattern."""
    print("\n=== Circuit Breaker Demo ===")

    manager = ErrorRecoveryManager()

    def unreliable_service():
        """Service that fails randomly."""
        if random.random() < 0.7:  # 70% failure rate
            raise Exception("Service temporarily unavailable")
        return "Service response"

    print("Testing circuit breaker with unreliable service...")

    success_count = 0
    failure_count = 0

    # Make multiple calls
    for i in range(10):
        try:
            result = manager.implement_circuit_breaker(
                unreliable_service, "test_service",
                failure_threshold=3, timeout=5
            )
            success_count += 1
            print(f"Call {i+1}: ✓ {result}")
        except Exception as e:
            failure_count += 1
            print(f"Call {i+1}: ✗ {str(e)[:50]}...")

    print(f"\nResults: {success_count} successes, {failure_count} failures")


def demonstrate_should_retry():
    """Demonstrate retry decision logic."""
    print("\n=== Retry Decision Demo ===")

    manager = ErrorRecoveryManager()

    test_cases = [
        (ErrorType.TRANSIENT, 0, True, "Should retry transient errors"),
        (ErrorType.CONFIGURATION, 0, False, "Should not retry config errors"),
        (ErrorType.DATA, 2, False, "Should not retry data errors"),
        (ErrorType.SYSTEM, 1, True, "Should retry system errors initially"),
        (ErrorType.SYSTEM, 3, False, "Should stop retrying system errors after limit"),
    ]

    for error_type, retry_count, expected, description in test_cases:
        should_retry = manager.should_retry(error_type, retry_count)
        status = "✓" if should_retry == expected else "✗"
        print(f"{status} {description}: {should_retry}")


def demonstrate_backoff_calculation():
    """Demonstrate backoff delay calculations."""
    print("\n=== Backoff Calculation Demo ===")

    manager = ErrorRecoveryManager()

    strategies = [RetryStrategy.FIXED_DELAY, RetryStrategy.EXPONENTIAL_BACKOFF, RetryStrategy.JITTER]

    for strategy in strategies:
        print(f"\n{strategy.value.upper()} Strategy:")
        for retry_count in range(5):
            delay = manager.calculate_backoff_delay(retry_count, 2.0, strategy)
            print(".2f")


def demonstrate_checkpointing():
    """Demonstrate checkpoint and recovery functionality."""
    print("\n=== Checkpointing Demo ===")

    manager = ErrorRecoveryManager()

    run_id = "demo_run_123"
    pipeline_id = "demo_pipeline"

    # Create checkpoint
    progress_data = {
        "pipeline_id": pipeline_id,
        "current_step": "extract",
        "records_processed": 5000,
        "last_record_id": 5000,
        "start_time": "2024-01-15T10:00:00"
    }

    checkpoint_id = manager.checkpoint_progress(
        run_id=run_id,
        step="extract",
        progress_data=progress_data,
        batch_number=5
    )

    if checkpoint_id:
        print(f"✓ Checkpoint saved: {checkpoint_id}")
    else:
        print("✗ Checkpoint save failed (DB not available)")

    # Try to resume from checkpoint
    checkpoint = manager.resume_from_checkpoint(run_id)

    if checkpoint:
        print(f"✓ Resumed from checkpoint: step={checkpoint.step}, batch={checkpoint.batch_number}")
        print(f"  Progress data: {checkpoint.checkpoint_data}")
    else:
        print("✗ No checkpoint found (DB not available)")


def demonstrate_quarantine():
    """Demonstrate record quarantine functionality."""
    print("\n=== Record Quarantine Demo ===")

    manager = ErrorRecoveryManager()

    # Sample bad records
    bad_records = [
        {"id": 1, "name": "Invalid Record 1", "error": "missing required field"},
        {"id": 2, "name": "Invalid Record 2", "error": "invalid data format"},
        {"id": 3, "name": "Invalid Record 3", "error": "constraint violation"}
    ]

    run_id = "demo_run_123"

    # Quarantine bad records
    quarantine_id = manager.quarantine_bad_records(
        run_id=run_id,
        bad_data=bad_records,
        error_reason="Data validation failed",
        error_type=ErrorType.DATA
    )

    if quarantine_id:
        print(f"✓ Records quarantined: {quarantine_id}")
    else:
        print("✗ Quarantine failed (DB not available)")

    # Try to reprocess quarantined records
    if quarantine_id:
        metrics = manager.reprocess_quarantined_records(run_id, quarantine_id)
        print(f"✓ Reprocessing results: {metrics}")


def demonstrate_error_patterns():
    """Demonstrate error pattern analysis."""
    print("\n=== Error Pattern Analysis Demo ===")

    manager = ErrorRecoveryManager()

    pipeline_id = "demo_pipeline"

    # Get error patterns (will be empty without DB)
    patterns = manager.get_error_patterns(pipeline_id, days=7)

    print(f"Found {len(patterns)} error patterns for pipeline {pipeline_id}")

    if not patterns:
        print("No patterns found (DB not available)")
        print("Would analyze patterns like:")
        print("- Frequent 'connection timeout' errors")
        print("- Recurring 'invalid credentials' issues")
        print("- Pattern of 'out of memory' during peak hours")


def demonstrate_recovery_recommendations():
    """Demonstrate recovery action recommendations."""
    print("\n=== Recovery Recommendations Demo ===")

    manager = ErrorRecoveryManager()

    error_types = [ErrorType.TRANSIENT, ErrorType.CONFIGURATION, ErrorType.DATA, ErrorType.SYSTEM]

    for error_type in error_types:
        recommendation = manager.recommend_recovery_action(error_type, {})
        print(f"{error_type.value.upper()}: {recommendation}")


def demonstrate_fallback_strategy():
    """Demonstrate fallback strategy implementation."""
    print("\n=== Fallback Strategy Demo ===")

    manager = ErrorRecoveryManager()

    def primary_action():
        """Primary action that might fail."""
        if random.random() < 0.7:
            raise Exception("Primary service unavailable")
        return "Primary result"

    def fallback_action():
        """Fallback action as backup."""
        return "Fallback result"

    print("Testing fallback strategy...")

    for i in range(5):
        try:
            result = manager.implement_fallback_strategy(
                primary_action, fallback_action
            )
            print(f"Attempt {i+1}: ✓ {result}")
        except Exception as e:
            print(f"Attempt {i+1}: ✗ Both primary and fallback failed: {e}")


def demonstrate_error_budget():
    """Demonstrate error budget tracking."""
    print("\n=== Error Budget Demo ===")

    manager = ErrorRecoveryManager()

    pipeline_id = "demo_pipeline"

    # Get error budget status
    budget_status = manager.get_error_budget_status(pipeline_id, time_window_days=30)

    print(f"Error Budget Status for {pipeline_id}:")
    print(f"  Total Operations: {budget_status.get('total_operations', 'N/A')}")
    print(f"  Error Count: {budget_status.get('error_count', 'N/A')}")
    print(f"  Error Rate: {budget_status.get('error_rate_percent', 'N/A'):.2f}%")
    print(f"  Budget Limit: {budget_status.get('budget_limit_percent', 'N/A')}%")
    print(f"  Budget Remaining: {budget_status.get('budget_remaining_percent', 'N/A'):.2f}%")
    print(f"  Budget Exceeded: {budget_status.get('budget_exceeded', 'N/A')}")


def demonstrate_decorators():
    """Demonstrate decorator usage."""
    print("\n=== Decorator Demo ===")

    manager = ErrorRecoveryManager()

    @manager.retry_on_failure(max_retries=3, backoff_factor=1.5)
    def unreliable_function(attempt=1):
        """Function decorated with retry logic."""
        if attempt < 3:
            raise Exception("Temporary failure")
        return f"Success on attempt {attempt}"

    @manager.circuit_breaker("demo_service", failure_threshold=2)
    def circuit_breaker_function():
        """Function decorated with circuit breaker."""
        if random.random() < 0.6:
            raise Exception("Service error")
        return "Service OK"

    print("Testing retry decorator...")
    try:
        result = unreliable_function(attempt=1)
        print(f"✓ Retry decorator: {result}")
    except Exception as e:
        print(f"✗ Retry decorator failed: {e}")

    print("Testing circuit breaker decorator...")
    for i in range(4):
        try:
            result = circuit_breaker_function()
            print(f"Call {i+1}: ✓ {result}")
        except Exception as e:
            print(f"Call {i+1}: ✗ {str(e)[:30]}...")


def main():
    """Run all error recovery manager demonstrations."""
    print("Error Recovery Manager Demonstration")
    print("=" * 50)

    try:
        demonstrate_error_classification()
        demonstrate_retry_logic()
        demonstrate_circuit_breaker()
        demonstrate_should_retry()
        demonstrate_backoff_calculation()
        demonstrate_checkpointing()
        demonstrate_quarantine()
        demonstrate_error_patterns()
        demonstrate_recovery_recommendations()
        demonstrate_fallback_strategy()
        demonstrate_error_budget()
        demonstrate_decorators()

        print("\n=== Demo Complete ===")
        print("ErrorRecoveryManager provides comprehensive error handling features:")
        print("✓ Error classification (transient, config, data, system)")
        print("✓ Multiple retry strategies (fixed, exponential, jitter)")
        print("✓ Circuit breaker pattern implementation")
        print("✓ Checkpoint and recovery mechanisms")
        print("✓ Record quarantine and reprocessing")
        print("✓ Error pattern analysis and recommendations")
        print("✓ Fallback strategy implementation")
        print("✓ Error budget tracking and failure analysis")
        print("✓ Decorators for automatic retry and circuit breaker")

    except Exception as e:
        logger.exception(f"Demo failed: {e}")
        print(f"Demo failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())