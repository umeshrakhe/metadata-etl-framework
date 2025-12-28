#!/usr/bin/env python3
"""
Mock Data Generator for ETL Framework Testing

Generates various types of test data for comprehensive testing.
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from typing import Dict, List, Any, Optional, Union
import random
import string
import uuid
from pathlib import Path
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


class MockDataGenerator:
    """Generate mock data for testing ETL pipelines."""

    def __init__(self, seed: int = 42):
        """Initialize the data generator with a random seed."""
        np.random.seed(seed)
        random.seed(seed)
        self.seed = seed

    def generate_valid_test_data(self, schema: Dict[str, Any], rows: int) -> pd.DataFrame:
        """
        Generate valid test data conforming to schema.

        Args:
            schema: Dictionary defining column schemas
            rows: Number of rows to generate

        Returns:
            DataFrame with valid test data
        """
        data = {}

        for column_name, column_schema in schema.items():
            data[column_name] = self._generate_column_data(column_name, column_schema, rows, valid=True)

        return pd.DataFrame(data)

    def generate_invalid_test_data(self, schema: Dict[str, Any], rows: int,
                                 error_types: List[str] = None) -> pd.DataFrame:
        """
        Generate test data with intentional errors.

        Args:
            schema: Dictionary defining column schemas
            rows: Number of rows to generate
            error_types: List of error types to introduce

        Returns:
            DataFrame with invalid test data
        """
        if error_types is None:
            error_types = ['nulls', 'wrong_types', 'out_of_range', 'invalid_format']

        data = {}

        for column_name, column_schema in schema.items():
            # Decide whether to make this column invalid
            if random.random() < 0.3:  # 30% chance of errors
                error_type = random.choice(error_types)
                data[column_name] = self._generate_column_data(
                    column_name, column_schema, rows, valid=False, error_type=error_type
                )
            else:
                data[column_name] = self._generate_column_data(
                    column_name, column_schema, rows, valid=True
                )

        return pd.DataFrame(data)

    def generate_edge_case_data(self, schema: Dict[str, Any], cases: List[str]) -> pd.DataFrame:
        """
        Generate edge case test data.

        Args:
            schema: Dictionary defining column schemas
            cases: List of edge cases to generate

        Returns:
            DataFrame with edge case data
        """
        data = {}

        # Generate one row per edge case
        for case in cases:
            row_data = {}
            for column_name, column_schema in schema.items():
                row_data[column_name] = self._generate_edge_case_value(
                    column_name, column_schema, case
                )
            # Convert single row to DataFrame and append
            case_df = pd.DataFrame([row_data])
            if data:
                data = pd.concat([data, case_df], ignore_index=True)
            else:
                data = case_df

        return data

    def generate_performance_test_data(self, size_gb: float) -> pd.DataFrame:
        """
        Generate test data of specified size for performance testing.

        Args:
            size_gb: Target size in gigabytes

        Returns:
            DataFrame of approximately the target size
        """
        # Estimate rows needed (rough approximation)
        # Average row size is about 1KB
        estimated_rows = int(size_gb * 1024 * 1024)  # 1GB ≈ 1M rows

        # Generate wide table to reach target size
        schema = {
            'id': {'type': 'int64', 'nullable': False},
            'string_col_1': {'type': 'string', 'nullable': True, 'max_length': 100},
            'string_col_2': {'type': 'string', 'nullable': True, 'max_length': 100},
            'string_col_3': {'type': 'string', 'nullable': True, 'max_length': 100},
            'int_col_1': {'type': 'int64', 'nullable': True, 'min': 0, 'max': 1000000},
            'int_col_2': {'type': 'int64', 'nullable': True, 'min': 0, 'max': 1000000},
            'float_col_1': {'type': 'float64', 'nullable': True, 'min': 0.0, 'max': 1000000.0},
            'float_col_2': {'type': 'float64', 'nullable': True, 'min': 0.0, 'max': 1000000.0},
            'date_col': {'type': 'date', 'nullable': True},
            'timestamp_col': {'type': 'datetime64[ns]', 'nullable': True},
            'boolean_col': {'type': 'bool', 'nullable': True}
        }

        return self.generate_valid_test_data(schema, estimated_rows)

    def _generate_column_data(self, column_name: str, column_schema: Dict[str, Any],
                            rows: int, valid: bool = True, error_type: str = None) -> List[Any]:
        """Generate data for a single column."""
        column_type = column_schema.get('type', 'string')
        nullable = column_schema.get('nullable', True)

        if not valid and error_type:
            return self._generate_invalid_column_data(column_name, column_schema, rows, error_type)
        else:
            return self._generate_valid_column_data(column_name, column_schema, rows)

    def _generate_valid_column_data(self, column_name: str, column_schema: Dict[str, Any], rows: int) -> List[Any]:
        """Generate valid data for a column."""
        column_type = column_schema.get('type', 'string')

        if column_type in ['int64', 'int32', 'int']:
            min_val = column_schema.get('min', 0)
            max_val = column_schema.get('max', 1000)
            return np.random.randint(min_val, max_val + 1, rows).tolist()

        elif column_type in ['float64', 'float32', 'float']:
            min_val = column_schema.get('min', 0.0)
            max_val = column_schema.get('max', 1000.0)
            return np.random.uniform(min_val, max_val, rows).tolist()

        elif column_type == 'string':
            max_length = column_schema.get('max_length', 50)
            return [self._random_string(random.randint(1, max_length)) for _ in range(rows)]

        elif column_type == 'date':
            start_date = column_schema.get('start_date', date(2000, 1, 1))
            end_date = column_schema.get('end_date', date.today())
            date_range = (end_date - start_date).days
            return [(start_date + timedelta(days=random.randint(0, date_range))) for _ in range(rows)]

        elif column_type in ['datetime64[ns]', 'timestamp']:
            start_date = column_schema.get('start_date', datetime(2000, 1, 1))
            end_date = column_schema.get('end_date', datetime.now())
            time_range = (end_date - start_date).total_seconds()
            return [(start_date + timedelta(seconds=random.randint(0, int(time_range)))) for _ in range(rows)]

        elif column_type == 'bool':
            return [random.choice([True, False]) for _ in range(rows)]

        elif column_type == 'uuid':
            return [str(uuid.uuid4()) for _ in range(rows)]

        else:
            # Default to string
            return [f'value_{i}' for i in range(rows)]

    def _generate_invalid_column_data(self, column_name: str, column_schema: Dict[str, Any],
                                    rows: int, error_type: str) -> List[Any]:
        """Generate invalid data for a column based on error type."""
        valid_data = self._generate_valid_column_data(column_name, column_schema, rows)

        if error_type == 'nulls':
            # Introduce nulls in non-nullable columns
            num_nulls = max(1, rows // 10)  # 10% nulls
            null_indices = random.sample(range(rows), num_nulls)
            for idx in null_indices:
                valid_data[idx] = None

        elif error_type == 'wrong_types':
            # Mix incompatible types
            column_type = column_schema.get('type', 'string')
            if column_type in ['int64', 'int32', 'int']:
                # Mix strings and floats
                for i in range(rows):
                    if random.random() < 0.2:
                        valid_data[i] = self._random_string(5)
                    elif random.random() < 0.3:
                        valid_data[i] = random.uniform(0, 100)
            elif column_type == 'string':
                # Mix numbers and dates
                for i in range(rows):
                    if random.random() < 0.3:
                        valid_data[i] = random.randint(0, 1000)

        elif error_type == 'out_of_range':
            # Generate values outside valid ranges
            column_type = column_schema.get('type', 'string')
            if column_type in ['int64', 'int32', 'int']:
                min_val = column_schema.get('min', 0)
                max_val = column_schema.get('max', 1000)
                # Generate values outside range
                for i in range(rows):
                    if random.random() < 0.3:
                        valid_data[i] = max_val + random.randint(1, 100)
                    elif random.random() < 0.3:
                        valid_data[i] = min_val - random.randint(1, 100)

        elif error_type == 'invalid_format':
            # Generate invalid formats
            column_type = column_schema.get('type', 'string')
            if column_type == 'date':
                # Invalid date strings
                invalid_formats = ['not-a-date', '99/99/99', '2023-13-45', '']
                for i in range(rows):
                    if random.random() < 0.3:
                        valid_data[i] = random.choice(invalid_formats)

        return valid_data

    def _generate_edge_case_value(self, column_name: str, column_schema: Dict[str, Any], case: str) -> Any:
        """Generate edge case value for a column."""
        column_type = column_schema.get('type', 'string')

        if case == 'empty_string':
            return '' if column_type == 'string' else None
        elif case == 'maximum_value':
            if column_type in ['int64', 'int32', 'int']:
                return column_schema.get('max', 9223372036854775807)
            elif column_type in ['float64', 'float32', 'float']:
                return column_schema.get('max', float('inf'))
            elif column_type == 'string':
                return 'x' * column_schema.get('max_length', 1000)
        elif case == 'minimum_value':
            if column_type in ['int64', 'int32', 'int']:
                return column_schema.get('min', -9223372036854775808)
            elif column_type in ['float64', 'float32', 'float']:
                return column_schema.get('min', -float('inf'))
        elif case == 'unicode_characters':
            return '测试数据🚀' if column_type == 'string' else None
        elif case == 'special_characters':
            return '!@#$%^&*()[]{}|;:,.<>?`~' if column_type == 'string' else None
        elif case == 'null_value':
            return None
        elif case == 'zero':
            if column_type in ['int64', 'int32', 'int']:
                return 0
            elif column_type in ['float64', 'float32', 'float']:
                return 0.0
        else:
            return self._generate_valid_column_data(column_name, column_schema, 1)[0]

    def _random_string(self, length: int) -> str:
        """Generate a random string of specified length."""
        return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


class TestDataFactory:
    """Factory for creating test data fixtures."""

    @staticmethod
    def create_employee_schema() -> Dict[str, Any]:
        """Create employee data schema."""
        return {
            'employee_id': {'type': 'int64', 'nullable': False, 'min': 1, 'max': 999999},
            'first_name': {'type': 'string', 'nullable': False, 'max_length': 50},
            'last_name': {'type': 'string', 'nullable': False, 'max_length': 50},
            'email': {'type': 'string', 'nullable': False, 'max_length': 100},
            'age': {'type': 'int64', 'nullable': True, 'min': 18, 'max': 65},
            'salary': {'type': 'float64', 'nullable': True, 'min': 30000, 'max': 200000},
            'hire_date': {'type': 'date', 'nullable': False},
            'department': {'type': 'string', 'nullable': True, 'max_length': 50},
            'is_active': {'type': 'bool', 'nullable': False}
        }

    @staticmethod
    def create_sales_schema() -> Dict[str, Any]:
        """Create sales data schema."""
        return {
            'sale_id': {'type': 'int64', 'nullable': False, 'min': 1, 'max': 999999999},
            'customer_id': {'type': 'int64', 'nullable': False, 'min': 1, 'max': 999999},
            'product_id': {'type': 'int64', 'nullable': False, 'min': 1, 'max': 99999},
            'quantity': {'type': 'int64', 'nullable': False, 'min': 1, 'max': 1000},
            'unit_price': {'type': 'float64', 'nullable': False, 'min': 0.01, 'max': 99999.99},
            'total_amount': {'type': 'float64', 'nullable': False, 'min': 0.01, 'max': 999999.99},
            'sale_date': {'type': 'datetime64[ns]', 'nullable': False},
            'payment_method': {'type': 'string', 'nullable': True, 'max_length': 20}
        }

    @staticmethod
    def create_inventory_schema() -> Dict[str, Any]:
        """Create inventory data schema."""
        return {
            'product_id': {'type': 'int64', 'nullable': False, 'min': 1, 'max': 99999},
            'product_name': {'type': 'string', 'nullable': False, 'max_length': 200},
            'category': {'type': 'string', 'nullable': True, 'max_length': 50},
            'stock_quantity': {'type': 'int64', 'nullable': False, 'min': 0, 'max': 100000},
            'reorder_point': {'type': 'int64', 'nullable': True, 'min': 0, 'max': 50000},
            'unit_cost': {'type': 'float64', 'nullable': False, 'min': 0.01, 'max': 9999.99},
            'selling_price': {'type': 'float64', 'nullable': False, 'min': 0.01, 'max': 99999.99},
            'last_updated': {'type': 'datetime64[ns]', 'nullable': False}
        }


# Convenience functions
def generate_valid_test_data(schema: Dict[str, Any], rows: int) -> pd.DataFrame:
    """Convenience function to generate valid test data."""
    generator = MockDataGenerator()
    return generator.generate_valid_test_data(schema, rows)


def generate_invalid_test_data(schema: Dict[str, Any], rows: int,
                             error_types: List[str] = None) -> pd.DataFrame:
    """Convenience function to generate invalid test data."""
    generator = MockDataGenerator()
    return generator.generate_invalid_test_data(schema, rows, error_types)


def generate_edge_case_data(schema: Dict[str, Any], cases: List[str]) -> pd.DataFrame:
    """Convenience function to generate edge case data."""
    generator = MockDataGenerator()
    return generator.generate_edge_case_data(schema, cases)


def generate_performance_test_data(size_gb: float) -> pd.DataFrame:
    """Convenience function to generate performance test data."""
    generator = MockDataGenerator()
    return generator.generate_performance_test_data(size_gb)


if __name__ == '__main__':
    # Example usage
    generator = MockDataGenerator()

    # Generate sample employee data
    employee_schema = TestDataFactory.create_employee_schema()
    valid_employees = generator.generate_valid_test_data(employee_schema, 100)
    print(f"Generated {len(valid_employees)} valid employee records")
    print(valid_employees.head())

    # Generate invalid data for testing
    invalid_employees = generator.generate_invalid_test_data(employee_schema, 50)
    print(f"\nGenerated {len(invalid_employees)} invalid employee records")

    # Generate edge cases
    edge_cases = generator.generate_edge_case_data(employee_schema, ['null_value', 'empty_string', 'maximum_value'])
    print(f"\nGenerated {len(edge_cases)} edge case records")
    print(edge_cases)
    #</content>
#<parameter name="filePath">d:\development\2026\metadata-etl-framework\tests\mock_data_generator.py