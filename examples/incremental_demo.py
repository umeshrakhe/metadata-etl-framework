#!/usr/bin/env python3
"""
Incremental Load Manager Example
Demonstrates the usage of the IncrementalLoadManager class for various incremental loading strategies.
"""

import logging
import sys
import os
import pandas as pd
from datetime import datetime, timedelta

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from utils.incremental_load_manager import IncrementalLoadManager, IncrementalStrategy

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

logger = logging.getLogger(__name__)


def demonstrate_watermark_management():
    """Demonstrate watermark retrieval and updates."""
    print("\n=== Watermark Management Demo ===")

    manager = IncrementalLoadManager()

    pipeline_id = "demo_pipeline"
    source_id = "demo_source"

    # Get current watermark (will be None initially)
    watermark = manager.get_last_watermark(pipeline_id, source_id)
    print(f"Current watermark: {watermark}")

    # Update watermark
    current_time = datetime.utcnow()
    success = manager.update_watermark(
        pipeline_id=pipeline_id,
        source_id=source_id,
        new_watermark=current_time.isoformat(),
        watermark_column="updated_at",
        strategy="timestamp"
    )
    print(f"Watermark update successful: {success}")

    # Get updated watermark
    watermark = manager.get_last_watermark(pipeline_id, source_id)
    print(f"Updated watermark: {watermark.watermark_value if watermark else None}")


def demonstrate_incremental_strategies():
    """Demonstrate different incremental loading strategies."""
    print("\n=== Incremental Strategies Demo ===")

    manager = IncrementalLoadManager()

    # Timestamp-based configuration
    timestamp_config = {
        'incremental_strategy': 'timestamp',
        'table': 'user_events',
        'timestamp_column': 'event_time'
    }

    # Sequence-based configuration
    sequence_config = {
        'incremental_strategy': 'sequence',
        'table': 'transactions',
        'sequence_column': 'transaction_id'
    }

    # Snapshot-based configuration
    snapshot_config = {
        'incremental_strategy': 'snapshot',
        'table': 'daily_sales',
        'partition_column': 'sale_date'
    }

    print("Configured strategies:")
    print(f"- Timestamp-based: {timestamp_config}")
    print(f"- Sequence-based: {sequence_config}")
    print(f"- Snapshot-based: {snapshot_config}")

    # Note: Actual extraction would require database connection
    print("Note: Actual data extraction requires database connection")


def demonstrate_late_arriving_data():
    """Demonstrate handling of late-arriving data."""
    print("\n=== Late Arriving Data Demo ===")

    manager = IncrementalLoadManager()

    # Create sample data with some late arrivals
    current_watermark = "2024-01-15T10:00:00"

    data = pd.DataFrame({
        'id': [1, 2, 3, 4, 5],
        'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
        'updated_at': [
            '2024-01-15T11:00:00',  # After watermark
            '2024-01-15T09:00:00',  # Before watermark (late)
            '2024-01-15T12:00:00',  # After watermark
            '2024-01-15T08:00:00',  # Before watermark (late)
            '2024-01-15T13:00:00'   # After watermark
        ]
    })

    print(f"Original data ({len(data)} records):")
    print(data)

    # Handle late arriving data
    on_time_data, late_count = manager.handle_late_arriving_data(
        data, 'updated_at', current_watermark
    )

    print(f"Late records found: {late_count}")
    print(f"On-time records remaining: {len(on_time_data)}")
    print("On-time data:")
    print(on_time_data)


def demonstrate_merge_operations():
    """Demonstrate incremental merge operations."""
    print("\n=== Merge Operations Demo ===")

    manager = IncrementalLoadManager()

    # Create sample incremental data
    incremental_data = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'name': ['Alice Updated', 'Bob', 'Charlie New'],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
        'updated_at': ['2024-01-15T10:00:00'] * 3
    })

    print("Incremental data to merge:")
    print(incremental_data)

    # Simulate merge operation (without actual DB)
    target_table = "customers"
    merge_keys = ['customer_id']

    # This would normally perform the actual merge
    print(f"Would merge into table '{target_table}' using keys: {merge_keys}")
    print("Supported load types: upsert, insert_only, update_only")


def demonstrate_scd_type2():
    """Demonstrate Slowly Changing Dimension Type 2 implementation."""
    print("\n=== SCD Type 2 Demo ===")

    manager = IncrementalLoadManager()

    # Create sample SCD data
    scd_data = pd.DataFrame({
        'customer_id': [1, 2, 3],
        'name': ['Alice Johnson', 'Bob Smith', 'Charlie Brown'],
        'address': ['123 Main St', '456 Oak Ave', '789 Pine Rd'],
        'effective_date': ['2024-01-15'] * 3
    })

    print("SCD data to process:")
    print(scd_data)

    # Simulate SCD Type 2 (without actual DB)
    target_table = "customer_scd"
    business_key = 'customer_id'
    effective_date = 'effective_date'

    print(f"Would implement SCD Type 2 for table '{target_table}'")
    print(f"Business key: {business_key}")
    print(f"Effective date column: {effective_date}")


def demonstrate_query_optimization():
    """Demonstrate incremental query optimization."""
    print("\n=== Query Optimization Demo ===")

    manager = IncrementalLoadManager()

    base_query = "SELECT * FROM sales_orders WHERE order_date > '2024-01-01'"

    # Create a mock watermark
    from utils.incremental_load_manager import Watermark
    watermark = Watermark(
        pipeline_id="sales_pipeline",
        source_id="orders_source",
        watermark_column="order_date",
        watermark_value="2024-01-10",
        update_time=datetime.utcnow(),
        strategy="timestamp"
    )

    # Optimize query
    optimized_query = manager.optimize_incremental_query(
        base_query, watermark, partition_column="order_date"
    )

    print(f"Original query: {base_query}")
    print(f"Optimized query: {optimized_query}")


def demonstrate_completeness_validation():
    """Demonstrate incremental completeness validation."""
    print("\n=== Completeness Validation Demo ===")

    manager = IncrementalLoadManager()

    # Test validation scenarios
    test_cases = [
        {"expected": 1000, "actual": 950, "tolerance": 5.0},
        {"expected": 1000, "actual": 920, "tolerance": 5.0},
        {"expected": 1000, "actual": 980, "tolerance": 5.0}
    ]

    for i, case in enumerate(test_cases, 1):
        validation = manager.validate_incremental_completeness(
            case["expected"], case["actual"], case["tolerance"]
        )

        print(f"Test {i}: Expected {case['expected']}, Actual {case['actual']}")
        print(f"  Complete: {validation['is_complete']}")
        print(f"  Difference: {validation['difference_percent']:.2f}%")


def demonstrate_cdc_operations():
    """Demonstrate Change Data Capture operations."""
    print("\n=== CDC Operations Demo ===")

    manager = IncrementalLoadManager()

    # CDC source configuration
    cdc_config = {
        'cdc_type': 'log_based',
        'table': 'user_profiles',
        'primary_key': 'user_id'
    }

    print("CDC configuration:")
    print(cdc_config)

    # Configure CDC (would normally set up triggers/logs)
    configured = manager.configure_cdc_source(cdc_config)
    print(f"CDC configured: {configured}")

    # Read CDC stream (would normally read from change logs)
    print("Would read CDC events from change stream")
    print("Supported operations: INSERT, UPDATE, DELETE")


def demonstrate_gap_detection():
    """Demonstrate data gap detection."""
    print("\n=== Gap Detection Demo ===")

    manager = IncrementalLoadManager()

    pipeline_id = "demo_pipeline"
    source_id = "demo_source"

    # This would normally analyze watermark history
    print(f"Would detect gaps for pipeline {pipeline_id}, source {source_id}")
    print("Gap detection looks for missing data periods based on:")
    print("- Time intervals between watermark updates")
    print("- Expected vs actual record counts")
    print("- Configurable tolerance thresholds")


def demonstrate_deduplication():
    """Demonstrate data deduplication."""
    print("\n=== Deduplication Demo ===")

    manager = IncrementalLoadManager()

    # Create data with duplicates
    data_with_dups = pd.DataFrame({
        'customer_id': [1, 1, 2, 3, 3, 3],
        'name': ['Alice', 'Alice', 'Bob', 'Charlie', 'Charlie', 'Charlie'],
        'email': ['alice@example.com', 'alice@example.com', 'bob@example.com',
                 'charlie@example.com', 'charlie@example.com', 'charlie@example.com']
    })

    print(f"Data with duplicates ({len(data_with_dups)} records):")
    print(data_with_dups)

    # Deduplicate
    deduplicated, removed = manager.deduplicate_data(data_with_dups, ['customer_id'])

    print(f"Duplicates removed: {removed}")
    print(f"Unique records remaining: {len(deduplicated)}")
    print("Deduplicated data:")
    print(deduplicated)


def main():
    """Run all incremental load manager demonstrations."""
    print("Incremental Load Manager Demonstration")
    print("=" * 50)

    try:
        demonstrate_watermark_management()
        demonstrate_incremental_strategies()
        demonstrate_late_arriving_data()
        demonstrate_merge_operations()
        demonstrate_scd_type2()
        demonstrate_query_optimization()
        demonstrate_completeness_validation()
        demonstrate_cdc_operations()
        demonstrate_gap_detection()
        demonstrate_deduplication()

        print("\n=== Demo Complete ===")
        print("IncrementalLoadManager provides comprehensive incremental loading features:")
        print("✓ Multiple incremental strategies (timestamp, sequence, CDC, snapshot, delta lake)")
        print("✓ Watermark management with timezone support")
        print("✓ Late-arriving data handling")
        print("✓ SCD Type 2 implementation")
        print("✓ Query optimization and performance tuning")
        print("✓ Completeness validation and gap detection")
        print("✓ CDC integration and change stream processing")
        print("✓ Data deduplication and consistency validation")
        print("✓ Comprehensive metrics and monitoring")

    except Exception as e:
        logger.exception(f"Demo failed: {e}")
        print(f"Demo failed: {e}")
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())