#!/usr/bin/env python3
"""
Test Utilities for ETL Framework Testing

Common utilities and fixtures for testing ETL components.
"""

import pytest
import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
import sqlite3
import tempfile
import os
import sys
import time
import logging
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Callable, Generator
import psutil

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.database_utils import DatabaseUtils, ConnectionConfig, DatabaseType
from tests.mock_data_generator import MockDataGenerator, TestDataFactory


# Test Database Management
class TestDatabaseManager:
    """Manage test databases for testing."""

    def __init__(self):
        self.databases = {}
        self.temp_files = []

    def setup_test_database(self, db_name: str = "test_db", db_type: DatabaseType = DatabaseType.SQLITE) -> DatabaseUtils:
        """
        Setup a test database.

        Args:
            db_name: Name of the test database
            db_type: Type of database to create

        Returns:
            DatabaseUtils instance for the test database
        """
        if db_type == DatabaseType.SQLITE:
            # Create temporary SQLite database
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.db')
            temp_file.close()
            self.temp_files.append(temp_file.name)

            config = ConnectionConfig(
                host="", port=0, database=temp_file.name,
                username="", password="", db_type=db_type
            )
        else:
            # For other database types, use in-memory or mock
            config = ConnectionConfig(
                host="localhost", port=5432, database=db_name,
                username="test", password="test", db_type=db_type
            )

        db_utils = DatabaseUtils(config)
        self.databases[db_name] = db_utils
        return db_utils

    def teardown_test_database(self, db_name: str):
        """Clean up a test database."""
        if db_name in self.databases:
            db_utils = self.databases[db_name]
            # Close connections if needed
            del self.databases[db_name]

        # Clean up temporary files
        for temp_file in self.temp_files:
            if os.path.exists(temp_file):
                os.unlink(temp_file)
        self.temp_files.clear()

    def load_test_metadata(self, db_utils: DatabaseUtils, pipeline_config: Dict[str, Any]):
        """
        Load test metadata into database.

        Args:
            db_utils: Database utilities instance
            pipeline_config: Pipeline configuration to load
        """
        # Create metadata tables if they don't exist
        self._create_metadata_tables(db_utils)

        # Insert pipeline metadata
        pipeline_id = pipeline_config.get('pipeline_id', 'test_pipeline')
        db_utils.execute_query("""
            INSERT OR REPLACE INTO pipelines (pipeline_id, pipeline_name, config, created_at)
            VALUES (?, ?, ?, ?)
        """, params=(
            pipeline_id,
            pipeline_config.get('pipeline_name', 'Test Pipeline'),
            str(pipeline_config),
            datetime.now().isoformat()
        ))

    def _create_metadata_tables(self, db_utils: DatabaseUtils):
        """Create metadata tables for testing."""
        # Pipelines table
        db_utils.execute_query("""
            CREATE TABLE IF NOT EXISTS pipelines (
                pipeline_id TEXT PRIMARY KEY,
                pipeline_name TEXT,
                config TEXT,
                created_at TEXT,
                updated_at TEXT
            )
        """)

        # Executions table
        db_utils.execute_query("""
            CREATE TABLE IF NOT EXISTS pipeline_executions (
                execution_id TEXT PRIMARY KEY,
                pipeline_id TEXT,
                status TEXT,
                start_time TEXT,
                end_time TEXT,
                records_processed INTEGER,
                error_message TEXT
            )
        """)

        # Data quality results table
        db_utils.execute_query("""
            CREATE TABLE IF NOT EXISTS dq_results (
                run_id TEXT,
                check_type TEXT,
                field_name TEXT,
                passed INTEGER,
                violations INTEGER,
                created_at TEXT
            )
        """)


# Mock Objects and Fixtures
class MockDatabaseConnection:
    """Mock database connection for testing."""

    def __init__(self):
        self.connected = False
        self.queries_executed = []
        self.results = {}

    def connect(self):
        """Mock connect method."""
        self.connected = True
        return self

    def execute_query(self, query: str, params: tuple = None, fetch: bool = False):
        """Mock query execution."""
        self.queries_executed.append((query, params))

        if fetch:
            # Return mock data based on query
            if "SELECT" in query.upper():
                if "COUNT" in query.upper():
                    return pd.DataFrame({'count': [10]})
                else:
                    return pd.DataFrame({
                        'id': [1, 2, 3],
                        'name': ['Alice', 'Bob', 'Charlie'],
                        'value': [100, 200, 300]
                    })
        return None

    def close(self):
        """Mock close method."""
        self.connected = False


def mock_connections(db_type: DatabaseType = DatabaseType.SQLITE) -> Mock:
    """Create mock database connection."""
    mock_conn = Mock()
    mock_conn.connect.return_value = mock_conn
    mock_conn.execute_query.return_value = pd.DataFrame({'test': [1, 2, 3]})
    mock_conn.close.return_value = None
    return mock_conn


# Data Comparison Utilities
def assert_data_equality(expected: pd.DataFrame, actual: pd.DataFrame,
                        ignore_columns: List[str] = None, rtol: float = 1e-5, atol: float = 1e-8):
    """
    Assert that two DataFrames are equal, with options for ignoring columns.

    Args:
        expected: Expected DataFrame
        actual: Actual DataFrame
        ignore_columns: Columns to ignore in comparison
        rtol: Relative tolerance for floating point comparisons
        atol: Absolute tolerance for floating point comparisons
    """
    if ignore_columns:
        expected = expected.drop(columns=ignore_columns, errors='ignore')
        actual = actual.drop(columns=ignore_columns, errors='ignore')

    # Sort columns for consistent comparison
    expected = expected.reindex(sorted(expected.columns), axis=1)
    actual = actual.reindex(sorted(actual.columns), axis=1)

    pd.testing.assert_frame_equal(expected, actual, rtol=rtol, atol=atol)


def assert_schema_match(expected_schema: Dict[str, Any], actual_schema: Dict[str, Any]):
    """
    Assert that two schemas match.

    Args:
        expected_schema: Expected schema definition
        actual_schema: Actual schema definition
    """
    assert set(expected_schema.keys()) == set(actual_schema.keys()), \
        f"Schema columns don't match. Expected: {set(expected_schema.keys())}, Actual: {set(actual_schema.keys())}"

    for column, expected_def in expected_schema.items():
        actual_def = actual_schema[column]

        # Check type
        assert expected_def.get('type') == actual_def.get('type'), \
            f"Type mismatch for column {column}: expected {expected_def.get('type')}, got {actual_def.get('type')}"

        # Check nullable
        if 'nullable' in expected_def:
            assert expected_def['nullable'] == actual_def.get('nullable', True), \
                f"Nullable mismatch for column {column}: expected {expected_def['nullable']}, got {actual_def.get('nullable', True)}"


# Performance Measurement
def measure_execution_time(function: Callable, *args, **kwargs) -> Tuple[Any, float]:
    """
    Measure execution time of a function.

    Args:
        function: Function to measure
        *args: Positional arguments for the function
        **kwargs: Keyword arguments for the function

    Returns:
        Tuple of (function_result, execution_time_seconds)
    """
    start_time = time.time()
    result = function(*args, **kwargs)
    execution_time = time.time() - start_time
    return result, execution_time


@contextmanager
def capture_logs(logger_name: str = None, level: int = logging.DEBUG) -> Generator[List[logging.LogRecord], None, None]:
    """
    Context manager to capture log records.

    Args:
        logger_name: Name of logger to capture (None for root logger)
        level: Logging level to capture

    Yields:
        List of captured log records
    """
    logs = []

    class LogCapture(logging.Handler):
        def emit(self, record):
            logs.append(record)

    handler = LogCapture()
    handler.setLevel(level)

    logger = logging.getLogger(logger_name)
    logger.addHandler(handler)
    logger.setLevel(level)

    try:
        yield logs
    finally:
        logger.removeHandler(handler)


# Error Simulation
def simulate_error(error_type: str) -> Exception:
    """
    Simulate different types of errors for testing.

    Args:
        error_type: Type of error to simulate

    Returns:
        Exception instance
    """
    error_map = {
        'connection_error': ConnectionError("Database connection failed"),
        'timeout_error': TimeoutError("Operation timed out"),
        'permission_error': PermissionError("Access denied"),
        'value_error': ValueError("Invalid value provided"),
        'key_error': KeyError("Key not found"),
        'type_error': TypeError("Type mismatch"),
        'io_error': IOError("File operation failed"),
        'memory_error': MemoryError("Out of memory"),
        'custom_error': RuntimeError("Custom test error")
    }

    return error_map.get(error_type, RuntimeError(f"Unknown error type: {error_type}"))


# Test Pipeline Configuration
def create_test_pipeline_config(overrides: Dict[str, Any] = None) -> Dict[str, Any]:
    """
    Create a test pipeline configuration.

    Args:
        overrides: Configuration overrides

    Returns:
        Pipeline configuration dictionary
    """
    base_config = {
        "pipeline_id": "test_pipeline",
        "pipeline_name": "Test ETL Pipeline",
        "description": "Test pipeline for unit testing",
        "pipeline_type": "batch",
        "source_system": "test_database",
        "target_system": "test_warehouse",
        "extract": {
            "type": "database",
            "connection": "test_source",
            "query": "SELECT * FROM test_table",
            "chunk_size": 1000
        },
        "transform": [
            {
                "type": "filter",
                "condition": "status == 'active'"
            },
            {
                "type": "add_columns",
                "columns": {
                    "processed_at": "current_timestamp",
                    "batch_id": "12345"
                }
            }
        ],
        "load": {
            "type": "database",
            "connection": "test_target",
            "table": "processed_data",
            "mode": "append",
            "batch_size": 1000
        },
        "quality": {
            "enabled": True,
            "rules": [
                {"field": "id", "rule": "not_null"},
                {"field": "amount", "rule": "range", "min": 0, "max": 1000000}
            ],
            "failure_threshold": 0.05
        },
        "monitoring": {
            "enabled": True,
            "alerts": ["failure", "performance"],
            "metrics": ["records_processed", "execution_time"]
        },
        "sla": {
            "max_duration_minutes": 30,
            "max_failure_rate": 0.1,
            "min_success_rate": 0.9
        }
    }

    if overrides:
        # Deep merge overrides
        def deep_merge(base, update):
            for key, value in update.items():
                if isinstance(value, dict) and key in base and isinstance(base[key], dict):
                    deep_merge(base[key], value)
                else:
                    base[key] = value
        deep_merge(base_config, overrides)

    return base_config


# Pytest Fixtures
@pytest.fixture
def test_db_manager():
    """Pytest fixture for test database manager."""
    manager = TestDatabaseManager()
    yield manager
    # Cleanup
    for db_name in list(manager.databases.keys()):
        manager.teardown_test_database(db_name)


@pytest.fixture
def mock_db_connection():
    """Pytest fixture for mock database connection."""
    return mock_connections()


@pytest.fixture
def sample_employee_data():
    """Pytest fixture for sample employee data."""
    generator = MockDataGenerator()
    schema = TestDataFactory.create_employee_schema()
    return generator.generate_valid_test_data(schema, 100)


@pytest.fixture
def sample_sales_data():
    """Pytest fixture for sample sales data."""
    generator = MockDataGenerator()
    schema = TestDataFactory.create_sales_schema()
    return generator.generate_valid_test_data(schema, 50)


@pytest.fixture
def test_pipeline_config():
    """Pytest fixture for test pipeline configuration."""
    return create_test_pipeline_config()


@pytest.fixture
def temp_directory():
    """Pytest fixture for temporary directory."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    # Cleanup
    import shutil
    shutil.rmtree(temp_dir, ignore_errors=True)


# Parametrized Test Data
def pytest_generate_tests(metafunc):
    """Generate parametrized tests for common scenarios."""
    if "data_size" in metafunc.fixturenames:
        metafunc.parametrize("data_size", [100, 1000, 10000], ids=["small", "medium", "large"])

    if "error_type" in metafunc.fixturenames:
        metafunc.parametrize("error_type", [
            "connection_error",
            "timeout_error",
            "value_error",
            "permission_error"
        ], ids=["connection", "timeout", "value", "permission"])


# Test Data Factories
class EmployeeDataFactory:
    """Factory for creating employee test data."""

    @staticmethod
    def create_valid_employees(count: int = 10) -> pd.DataFrame:
        """Create valid employee records."""
        generator = MockDataGenerator()
        schema = TestDataFactory.create_employee_schema()
        return generator.generate_valid_test_data(schema, count)

    @staticmethod
    def create_invalid_employees(count: int = 10, error_types: List[str] = None) -> pd.DataFrame:
        """Create invalid employee records."""
        generator = MockDataGenerator()
        schema = TestDataFactory.create_employee_schema()
        return generator.generate_invalid_test_data(schema, count, error_types)


class SalesDataFactory:
    """Factory for creating sales test data."""

    @staticmethod
    def create_valid_sales(count: int = 10) -> pd.DataFrame:
        """Create valid sales records."""
        generator = MockDataGenerator()
        schema = TestDataFactory.create_sales_schema()
        return generator.generate_valid_test_data(schema, count)

    @staticmethod
    def create_high_value_sales(count: int = 10) -> pd.DataFrame:
        """Create high-value sales records."""
        generator = MockDataGenerator()
        schema = TestDataFactory.create_sales_schema()
        data = generator.generate_valid_test_data(schema, count)

        # Modify to have high values
        data['total_amount'] = data['total_amount'] * 10
        return data


if __name__ == '__main__':
    # Example usage
    manager = TestDatabaseManager()
    db_utils = manager.setup_test_database("example_db")

    # Load test metadata
    config = create_test_pipeline_config()
    manager.load_test_metadata(db_utils, config)

    print("Test database and metadata setup complete")

    # Generate sample data
    employees = EmployeeDataFactory.create_valid_employees(5)
    print(f"Generated {len(employees)} employee records")
    print(employees.head())

    manager.teardown_test_database("example_db")
    print("Test database cleaned up")
    #</content>
#<parameter name="filePath">d:\development\2026\metadata-etl-framework\tests\test_utilities.py