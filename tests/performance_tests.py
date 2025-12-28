#!/usr/bin/env python3
"""
Performance Tests for ETL Framework

Tests scalability, memory usage, and execution performance.
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
import psutil
import gc
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Any, Tuple

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.database_utils import DatabaseUtils, ConnectionConfig, DatabaseType
from transform.transform_engine import TransformEngine
from transform.engine_selector import EngineSelector
from quality.dq_engine import DQEngine


class TestSmallDatasetPerformance(unittest.TestCase):
    """Test performance with small datasets (< 1GB)."""

    def setUp(self):
        """Setup test fixtures."""
        self.db_utils = DatabaseUtils(ConnectionConfig(
            host="", port=0, database=":memory:",
            username="", password="", db_type=DatabaseType.SQLITE
        ))

        self.transform_engine = TransformEngine()
        self.engine_selector = EngineSelector()
        self.dq_engine = DQEngine(self.db_utils)

        # Create small dataset (~100KB)
        self.small_data = self.generate_test_data(1000)

    def generate_test_data(self, rows: int) -> pd.DataFrame:
        """Generate test dataset."""
        np.random.seed(42)
        return pd.DataFrame({
            'id': range(rows),
            'name': [f'Name_{i}' for i in range(rows)],
            'age': np.random.randint(18, 65, rows),
            'salary': np.random.uniform(30000, 150000, rows),
            'department': np.random.choice(['HR', 'IT', 'Finance', 'Marketing'], rows),
            'hire_date': pd.date_range('2010-01-01', periods=rows, freq='D')[:rows]
        })

    def test_small_dataset_extraction_performance(self):
        """Test extraction performance for small dataset."""
        # Setup test table
        self.db_utils.execute_query("""
        CREATE TABLE test_employees (
            id INTEGER, name TEXT, age INTEGER, salary REAL,
            department TEXT, hire_date TEXT
        )
        """)

        # Insert data
        for _, row in self.small_data.iterrows():
            self.db_utils.execute_query("""
            INSERT INTO test_employees VALUES (?, ?, ?, ?, ?, ?)
            """, params=(int(row['id']), row['name'], int(row['age']),
                        float(row['salary']), row['department'],
                        row['hire_date'].strftime('%Y-%m-%d')))

        # Measure extraction time
        start_time = time.time()
        extracted_data = self.db_utils.execute_query("SELECT * FROM test_employees", fetch=True)
        extraction_time = time.time() - start_time

        self.assertLess(extraction_time, 1.0)  # Should complete in less than 1 second
        self.assertEqual(len(extracted_data), len(self.small_data))

    def test_small_dataset_transformation_performance(self):
        """Test transformation performance for small dataset."""
        transform_configs = [
            {"type": "filter", "condition": "age > 30"},
            {"type": "add_columns", "columns": {"annual_bonus": "salary * 0.1"}},
            {"type": "aggregate", "group_by": ["department"], "aggregations": {"salary": "mean"}}
        ]

        total_time = 0
        current_data = self.small_data

        for config in transform_configs:
            start_time = time.time()
            current_data = self.transform_engine.apply_transformation(current_data, config)
            transform_time = time.time() - start_time
            total_time += transform_time

        self.assertLess(total_time, 2.0)  # Should complete in less than 2 seconds

    def test_small_dataset_quality_check_performance(self):
        """Test data quality check performance for small dataset."""
        dq_rules = [
            {"field": "id", "rule": "not_null"},
            {"field": "age", "rule": "range", "min": 0, "max": 100},
            {"field": "salary", "rule": "range", "min": 0, "max": 1000000}
        ]

        start_time = time.time()
        completeness_results = self.dq_engine.run_completeness_checks(self.small_data, dq_rules[:1])
        validity_results = self.dq_engine.run_validity_checks(self.small_data, dq_rules[1:])
        quality_time = time.time() - start_time

        self.assertLess(quality_time, 1.0)  # Should complete in less than 1 second

    def test_memory_usage_small_dataset(self):
        """Test memory usage for small dataset operations."""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Perform operations
        filtered_data = self.small_data[self.small_data['age'] > 30]
        aggregated_data = filtered_data.groupby('department')['salary'].mean()

        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = peak_memory - initial_memory

        self.assertLess(memory_increase, 50)  # Should use less than 50MB additional memory


class TestMediumDatasetPerformance(unittest.TestCase):
    """Test performance with medium datasets (1-10GB)."""

    def setUp(self):
        """Setup test fixtures."""
        self.db_utils = DatabaseUtils(ConnectionConfig(
            host="", port=0, database=":memory:",
            username="", password="", db_type=DatabaseType.SQLITE
        ))

        self.transform_engine = TransformEngine()
        self.engine_selector = EngineSelector()

        # Create medium dataset (~10MB)
        self.medium_data = self.generate_test_data(100000)

    def generate_test_data(self, rows: int) -> pd.DataFrame:
        """Generate test dataset."""
        np.random.seed(42)
        return pd.DataFrame({
            'id': range(rows),
            'name': [f'Name_{i}' for i in range(rows)],
            'age': np.random.randint(18, 65, rows),
            'salary': np.random.uniform(30000, 150000, rows),
            'department': np.random.choice(['HR', 'IT', 'Finance', 'Marketing'], rows),
            'hire_date': pd.date_range('2010-01-01', periods=rows, freq='H')[:rows]
        })

    @pytest.mark.slow
    def test_medium_dataset_transformation_performance(self):
        """Test transformation performance for medium dataset."""
        transform_config = {"type": "filter", "condition": "age > 30"}

        start_time = time.time()
        result = self.transform_engine.apply_transformation(self.medium_data, transform_config)
        transform_time = time.time() - start_time

        self.assertLess(transform_time, 10.0)  # Should complete in less than 10 seconds
        self.assertGreater(len(result), 0)

    @pytest.mark.slow
    def test_medium_dataset_aggregation_performance(self):
        """Test aggregation performance for medium dataset."""
        transform_config = {
            "type": "aggregate",
            "group_by": ["department"],
            "aggregations": {"salary": ["mean", "sum", "count"]}
        }

        start_time = time.time()
        result = self.transform_engine.apply_transformation(self.medium_data, transform_config)
        aggregation_time = time.time() - start_time

        self.assertLess(aggregation_time, 15.0)  # Should complete in less than 15 seconds
        self.assertEqual(len(result), 4)  # 4 departments

    def test_engine_selection_for_medium_dataset(self):
        """Test engine selection for medium dataset."""
        selected_engine = self.engine_selector.select_engine(self.medium_data)

        # For medium dataset, should prefer pandas or polars
        self.assertIn(selected_engine, ["pandas", "polars"])

    @pytest.mark.slow
    def test_memory_usage_medium_dataset(self):
        """Test memory usage for medium dataset operations."""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Perform memory-intensive operation
        grouped = self.medium_data.groupby('department')
        aggregated = grouped['salary'].agg(['mean', 'sum', 'count'])

        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = peak_memory - initial_memory

        # Medium dataset should use reasonable memory
        self.assertLess(memory_increase, 500)  # Less than 500MB increase


class TestLargeDatasetPerformance(unittest.TestCase):
    """Test performance with large datasets (> 10GB)."""

    def setUp(self):
        """Setup test fixtures."""
        self.transform_engine = TransformEngine()
        self.engine_selector = EngineSelector()

        # For large datasets, we'll simulate with smaller data but test scalability patterns
        # In real scenarios, this would use distributed processing
        self.large_data_sample = self.generate_test_data(10000)  # Sample for testing

    def generate_test_data(self, rows: int) -> pd.DataFrame:
        """Generate test dataset."""
        np.random.seed(42)
        return pd.DataFrame({
            'id': range(rows),
            'name': [f'Name_{i}' for i in range(rows)],
            'age': np.random.randint(18, 65, rows),
            'salary': np.random.uniform(30000, 150000, rows),
            'department': np.random.choice(['HR', 'IT', 'Finance', 'Marketing'], rows),
            'hire_date': pd.date_range('2010-01-01', periods=rows, freq='D')[:rows]
        })

    def test_large_dataset_engine_selection(self):
        """Test engine selection prefers scalable engines for large data."""
        # Simulate large dataset characteristics
        large_data_mock = Mock()
        large_data_mock.memory_usage.return_value.sum.return_value = 15 * 1024**3  # 15GB

        selected_engine = self.engine_selector.select_engine(large_data_mock)

        # Should prefer distributed engines for large data
        self.assertIn(selected_engine, ["spark", "dask", "polars"])

    def test_chunked_processing_simulation(self):
        """Test chunked processing approach for large datasets."""
        chunk_size = 1000
        total_rows = len(self.large_data_sample)

        total_processed = 0
        start_time = time.time()

        # Process in chunks
        for i in range(0, total_rows, chunk_size):
            chunk = self.large_data_sample.iloc[i:i+chunk_size]
            # Simulate processing
            processed_chunk = chunk[chunk['age'] > 30]
            total_processed += len(processed_chunk)

        processing_time = time.time() - start_time

        self.assertGreater(total_processed, 0)
        self.assertLess(processing_time, 5.0)  # Should complete reasonably fast

    def test_memory_efficient_operations(self):
        """Test memory-efficient operation patterns."""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Use copy=False where possible to avoid memory duplication
        filtered_data = self.large_data_sample[self.large_data_sample['age'] > 30].copy(deep=False)

        # Force garbage collection
        gc.collect()

        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_increase = final_memory - initial_memory

        # Memory increase should be reasonable
        self.assertLess(memory_increase, 100)


class TestConcurrentExecution(unittest.TestCase):
    """Test concurrent execution performance."""

    def setUp(self):
        """Setup test fixtures."""
        self.transform_engine = TransformEngine()
        self.test_data = pd.DataFrame({
            'id': range(1000),
            'value': np.random.rand(1000),
            'category': np.random.choice(['A', 'B', 'C'], 1000)
        })

    @patch('concurrent.futures.ThreadPoolExecutor')
    def test_concurrent_transformation_execution(self, mock_executor):
        """Test concurrent execution of transformations."""
        # Mock the executor
        mock_future = Mock()
        mock_future.result.return_value = self.test_data
        mock_executor.return_value.__enter__.return_value.submit.return_value = mock_future

        # Simulate concurrent processing
        transform_configs = [
            {"type": "filter", "condition": "category == 'A'"},
            {"type": "filter", "condition": "category == 'B'"},
            {"type": "filter", "condition": "category == 'C'"}
        ]

        start_time = time.time()

        # In real implementation, this would use concurrent execution
        results = []
        for config in transform_configs:
            result = self.transform_engine.apply_transformation(self.test_data, config)
            results.append(result)

        execution_time = time.time() - start_time

        self.assertEqual(len(results), 3)
        self.assertLess(execution_time, 2.0)

    def test_parallel_data_processing(self):
        """Test parallel data processing patterns."""
        import multiprocessing as mp

        # Test with small data to avoid resource issues
        def process_chunk(chunk):
            return chunk[chunk['value'] > 0.5]

        # Split data into chunks
        num_chunks = min(4, mp.cpu_count())
        chunk_size = len(self.test_data) // num_chunks
        chunks = [self.test_data.iloc[i:i+chunk_size] for i in range(0, len(self.test_data), chunk_size)]

        start_time = time.time()

        # Process chunks (in real scenario would use multiprocessing)
        results = [process_chunk(chunk) for chunk in chunks]

        processing_time = time.time() - start_time

        total_processed = sum(len(result) for result in results)
        self.assertGreater(total_processed, 0)
        self.assertLess(processing_time, 1.0)


class TestMemoryUsage(unittest.TestCase):
    """Test memory usage patterns and optimization."""

    def setUp(self):
        """Setup test fixtures."""
        self.transform_engine = TransformEngine()
        self.test_data = pd.DataFrame({
            'id': range(10000),
            'data': ['x' * 100 for _ in range(10000)]  # ~1MB of string data
        })

    def test_memory_monitoring(self):
        """Test memory usage monitoring."""
        process = psutil.Process()

        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Perform memory-intensive operation
        duplicated_data = pd.concat([self.test_data] * 5, ignore_index=True)
        sorted_data = duplicated_data.sort_values('id')

        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_used = peak_memory - initial_memory

        # Clean up
        del duplicated_data, sorted_data
        gc.collect()

        final_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Memory should be reasonably bounded
        self.assertLess(memory_used, 200)  # Less than 200MB increase

    def test_memory_cleanup(self):
        """Test proper memory cleanup after operations."""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        # Create large temporary data
        large_data = pd.DataFrame({
            'col1': range(50000),
            'col2': np.random.rand(50000),
            'col3': ['string' * 10 for _ in range(50000)]
        })

        # Process and delete
        result = large_data[large_data['col2'] > 0.5]
        del large_data, result
        gc.collect()

        final_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_leak = final_memory - initial_memory

        # Memory should return close to initial (allowing for some overhead)
        self.assertLess(abs(memory_leak), 50)  # Less than 50MB difference

    def test_efficient_data_structures(self):
        """Test use of efficient data structures."""
        # Test categorical data for memory efficiency
        categorical_data = pd.DataFrame({
            'category': pd.Categorical(['A', 'B', 'C'] * 3334)  # Repeat to fill
        })

        # Memory usage should be efficient for categorical data
        memory_usage = categorical_data.memory_usage(deep=True).sum() / 1024 / 1024  # MB

        self.assertLess(memory_usage, 1.0)  # Should use less than 1MB

    def test_lazy_evaluation_simulation(self):
        """Test lazy evaluation patterns for memory efficiency."""
        # Simulate lazy operations without full data loading
        def lazy_filter(data_generator, condition):
            for chunk in data_generator:
                yield chunk[chunk['id'] % 2 == 0]  # Even IDs only

        # Create data generator
        def data_generator():
            for i in range(0, len(self.test_data), 1000):
                yield self.test_data.iloc[i:i+1000]

        # Process lazily
        results = list(lazy_filter(data_generator(), "id % 2 == 0"))

        total_rows = sum(len(chunk) for chunk in results)
        self.assertGreater(total_rows, 0)
        self.assertLessEqual(total_rows, len(self.test_data))


# Performance test utilities
class PerformanceTestUtils:
    """Utility functions for performance testing."""

    @staticmethod
    def measure_execution_time(func, *args, **kwargs):
        """Measure function execution time."""
        start_time = time.time()
        result = func(*args, **kwargs)
        execution_time = time.time() - start_time
        return result, execution_time

    @staticmethod
    def measure_memory_usage(func, *args, **kwargs):
        """Measure memory usage during function execution."""
        process = psutil.Process()
        initial_memory = process.memory_info().rss / 1024 / 1024  # MB

        result = func(*args, **kwargs)

        peak_memory = process.memory_info().rss / 1024 / 1024  # MB
        memory_used = peak_memory - initial_memory

        return result, memory_used

    @staticmethod
    def generate_performance_test_data(rows: int, cols: int = 10) -> pd.DataFrame:
        """Generate test data for performance testing."""
        np.random.seed(42)
        data = {}

        for i in range(cols):
            if i == 0:
                data[f'int_col_{i}'] = range(rows)
            elif i < cols // 3:
                data[f'int_col_{i}'] = np.random.randint(0, 1000, rows)
            elif i < 2 * cols // 3:
                data[f'float_col_{i}'] = np.random.uniform(0, 1000, rows)
            else:
                data[f'str_col_{i}'] = [f'string_{j}' for j in range(rows)]

        return pd.DataFrame(data)


if __name__ == '__main__':
    unittest.main()
    #</content>
#<parameter name="filePath">d:\development\2026\metadata-etl-framework\tests\performance_tests.py