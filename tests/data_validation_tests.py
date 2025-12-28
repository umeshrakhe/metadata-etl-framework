#!/usr/bin/env python3
"""
Data Validation Tests for ETL Framework

Tests data quality, schema validation, and business rules.
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
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.database_utils import DatabaseUtils, ConnectionConfig, DatabaseType
from quality.dq_engine import DQEngine


class TestSchemaValidation(unittest.TestCase):
    """Test schema validation functionality."""

    def setUp(self):
        """Setup test fixtures."""
        self.db_utils = DatabaseUtils(ConnectionConfig(
            host="", port=0, database=":memory:",
            username="", password="", db_type=DatabaseType.SQLITE
        ))

        self.dq_engine = DQEngine(self.db_utils)

        # Define expected schema
        self.expected_schema = {
            "id": {"type": "int64", "nullable": False},
            "name": {"type": "object", "nullable": False},
            "age": {"type": "int64", "nullable": True},
            "salary": {"type": "float64", "nullable": True},
            "hire_date": {"type": "datetime64[ns]", "nullable": True}
        }

    def test_schema_conformance_valid_data(self):
        """Test schema validation with conforming data."""
        valid_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'salary': [50000.0, 60000.0, 70000.0],
            'hire_date': pd.to_datetime(['2023-01-01', '2023-02-01', '2023-03-01'])
        })

        result = self.dq_engine.validate_schema(valid_data, self.expected_schema)

        self.assertTrue(result["valid"])
        self.assertEqual(len(result["errors"]), 0)

    def test_schema_conformance_missing_columns(self):
        """Test schema validation with missing columns."""
        invalid_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie']
            # Missing age, salary, hire_date columns
        })

        result = self.dq_engine.validate_schema(invalid_data, self.expected_schema)

        self.assertFalse(result["valid"])
        self.assertIn("missing_columns", result["errors"])

    def test_schema_conformance_wrong_types(self):
        """Test schema validation with wrong data types."""
        invalid_data = pd.DataFrame({
            'id': ['1', '2', '3'],  # Should be int, but is string
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'salary': [50000.0, 60000.0, 70000.0],
            'hire_date': ['2023-01-01', '2023-02-01', '2023-03-01']
        })

        result = self.dq_engine.validate_schema(invalid_data, self.expected_schema)

        self.assertFalse(result["valid"])
        self.assertIn("type_mismatches", result["errors"])

    def test_schema_conformance_null_violations(self):
        """Test schema validation with null constraint violations."""
        invalid_data = pd.DataFrame({
            'id': [1, None, 3],  # id should not be null
            'name': ['Alice', None, 'Charlie'],  # name should not be null
            'age': [25, 30, 35],
            'salary': [50000.0, 60000.0, 70000.0],
            'hire_date': pd.to_datetime(['2023-01-01', '2023-02-01', '2023-03-01'])
        })

        result = self.dq_engine.validate_schema(invalid_data, self.expected_schema)

        self.assertFalse(result["valid"])
        self.assertIn("null_violations", result["errors"])

    def test_schema_validation_edge_cases(self):
        """Test schema validation edge cases."""
        # Empty dataframe
        empty_data = pd.DataFrame()
        result = self.dq_engine.validate_schema(empty_data, self.expected_schema)
        self.assertFalse(result["valid"])

        # Dataframe with extra columns
        extra_columns_data = pd.DataFrame({
            'id': [1, 2, 3],
            'name': ['Alice', 'Bob', 'Charlie'],
            'age': [25, 30, 35],
            'extra_col': ['value1', 'value2', 'value3']
        })
        result = self.dq_engine.validate_schema(extra_columns_data, self.expected_schema)
        # Should still be valid if expected columns are present and correct
        self.assertTrue(result["valid"])


class TestDataTypeValidation(unittest.TestCase):
    """Test data type validation functionality."""

    def setUp(self):
        """Setup test fixtures."""
        self.dq_engine = DQEngine(Mock())

        self.test_data = pd.DataFrame({
            'int_col': [1, 2, 3, 'invalid', 5],
            'float_col': [1.1, 2.2, 'invalid', 4.4, 5.5],
            'string_col': ['a', 'b', 'c', 123, 'e'],  # Mixed types
            'date_col': ['2023-01-01', '2023-02-01', 'invalid', '2023-04-01', '2023-05-01'],
            'bool_col': [True, False, 'true', 1, 0]
        })

    def test_integer_type_validation(self):
        """Test integer type validation."""
        result = self.dq_engine.validate_data_types(self.test_data, {'int_col': 'int64'})

        self.assertFalse(result["int_col"]["valid"])
        self.assertEqual(result["int_col"]["invalid_count"], 1)  # 'invalid' string

    def test_float_type_validation(self):
        """Test float type validation."""
        result = self.dq_engine.validate_data_types(self.test_data, {'float_col': 'float64'})

        self.assertFalse(result["float_col"]["valid"])
        self.assertEqual(result["float_col"]["invalid_count"], 1)  # 'invalid' string

    def test_string_type_validation(self):
        """Test string type validation."""
        result = self.dq_engine.validate_data_types(self.test_data, {'string_col': 'object'})

        # Should be valid since pandas treats mixed types as object
        self.assertTrue(result["string_col"]["valid"])

    def test_date_type_validation(self):
        """Test date type validation."""
        result = self.dq_engine.validate_data_types(self.test_data, {'date_col': 'datetime64[ns]'})

        self.assertFalse(result["date_col"]["valid"])
        self.assertEqual(result["date_col"]["invalid_count"], 1)  # 'invalid' string

    def test_boolean_type_validation(self):
        """Test boolean type validation."""
        result = self.dq_engine.validate_data_types(self.test_data, {'bool_col': 'bool'})

        # Mixed types should be considered valid for boolean conversion in some cases
        # This depends on implementation
        self.assertIsInstance(result["bool_col"]["valid"], bool)

    def test_type_validation_with_nulls(self):
        """Test type validation with null values."""
        data_with_nulls = pd.DataFrame({
            'int_col': [1, 2, None, 4, 5]
        })

        result = self.dq_engine.validate_data_types(data_with_nulls, {'int_col': 'int64'})

        # Should handle nulls appropriately
        self.assertIsInstance(result["int_col"]["valid"], bool)


class TestReferentialIntegrity(unittest.TestCase):
    """Test referential integrity validation."""

    def setUp(self):
        """Setup test fixtures."""
        self.db_utils = DatabaseUtils(ConnectionConfig(
            host="", port=0, database=":memory:",
            username="", password="", db_type=DatabaseType.SQLITE
        ))

        self.setup_reference_tables()

    def setup_reference_tables(self):
        """Setup parent-child tables for referential integrity testing."""
        # Parent table
        create_parent_sql = """
        CREATE TABLE departments (
            dept_id INTEGER PRIMARY KEY,
            dept_name TEXT NOT NULL
        )
        """

        # Child table
        create_child_sql = """
        CREATE TABLE employees (
            emp_id INTEGER PRIMARY KEY,
            emp_name TEXT NOT NULL,
            dept_id INTEGER,
            FOREIGN KEY (dept_id) REFERENCES departments(dept_id)
        )
        """

        self.db_utils.execute_query(create_parent_sql)
        self.db_utils.execute_query(create_child_sql)

        # Insert parent data
        insert_parent_sql = """
        INSERT INTO departments (dept_id, dept_name) VALUES
        (1, 'HR'),
        (2, 'IT'),
        (3, 'Finance')
        """
        self.db_utils.execute_query(insert_parent_sql)

        # Insert child data (some with valid FK, some invalid)
        insert_child_sql = """
        INSERT INTO employees (emp_id, emp_name, dept_id) VALUES
        (1, 'Alice', 1),    -- Valid FK
        (2, 'Bob', 2),      -- Valid FK
        (3, 'Charlie', 99), -- Invalid FK
        (4, 'David', NULL)  -- NULL FK (might be allowed)
        """
        self.db_utils.execute_query(insert_child_sql)

    def test_foreign_key_validation(self):
        """Test foreign key referential integrity."""
        # Query to check FK violations
        fk_check_query = """
        SELECT e.emp_id, e.emp_name, e.dept_id
        FROM employees e
        LEFT JOIN departments d ON e.dept_id = d.dept_id
        WHERE e.dept_id IS NOT NULL AND d.dept_id IS NULL
        """

        violations = self.db_utils.execute_query(fk_check_query, fetch=True)

        self.assertEqual(len(violations), 1)  # One FK violation
        self.assertEqual(violations.iloc[0]['emp_name'], 'Charlie')

    def test_parent_child_relationship_validation(self):
        """Test parent-child relationship validation."""
        # Check for orphaned child records
        orphan_check_query = """
        SELECT COUNT(*) as orphan_count
        FROM employees
        WHERE dept_id IS NOT NULL
        AND dept_id NOT IN (SELECT dept_id FROM departments)
        """

        result = self.db_utils.execute_query(orphan_check_query, fetch=True)
        orphan_count = result.iloc[0]['orphan_count']

        self.assertEqual(orphan_count, 1)

    def test_cascade_delete_simulation(self):
        """Test cascade delete behavior simulation."""
        # Count employees before delete
        count_before = self.db_utils.execute_query(
            "SELECT COUNT(*) as count FROM employees", fetch=True
        ).iloc[0]['count']

        # Delete department (should cascade in real FK, but SQLite doesn't enforce by default)
        self.db_utils.execute_query("DELETE FROM departments WHERE dept_id = 1")

        # Count employees after delete
        count_after = self.db_utils.execute_query(
            "SELECT COUNT(*) as count FROM employees", fetch=True
        ).iloc[0]['count']

        # In SQLite without CASCADE, count should remain the same
        self.assertEqual(count_before, count_after)


class TestBusinessRules(unittest.TestCase):
    """Test business rules validation."""

    def setUp(self):
        """Setup test fixtures."""
        self.dq_engine = DQEngine(Mock())

        self.business_data = pd.DataFrame({
            'employee_id': [1, 2, 3, 4, 5],
            'age': [25, 30, 35, 40, 45],
            'salary': [30000, 50000, 70000, 90000, 110000],
            'hire_date': pd.to_datetime(['2020-01-01', '2019-01-01', '2018-01-01', '2017-01-01', '2016-01-01']),
            'termination_date': pd.to_datetime(['2023-01-01', None, None, None, None]),
            'department': ['HR', 'IT', 'Finance', 'IT', 'HR']
        })

    def test_age_range_business_rule(self):
        """Test age range business rule."""
        rule = {
            "field": "age",
            "rule": "range",
            "min": 18,
            "max": 65
        }

        result = self.dq_engine.validate_business_rule(self.business_data, rule)

        self.assertTrue(result["valid"])
        self.assertEqual(result["violations"], 0)

    def test_salary_range_business_rule(self):
        """Test salary range business rule."""
        rule = {
            "field": "salary",
            "rule": "range",
            "min": 25000,
            "max": 150000
        }

        result = self.dq_engine.validate_business_rule(self.business_data, rule)

        self.assertTrue(result["valid"])
        self.assertEqual(result["violations"], 0)

    def test_date_logic_business_rule(self):
        """Test date logic business rule (hire before termination)."""
        # Add invalid data
        invalid_data = self.business_data.copy()
        invalid_data.loc[1, 'hire_date'] = pd.to_datetime('2023-06-01')
        invalid_data.loc[1, 'termination_date'] = pd.to_datetime('2023-01-01')  # Hire after termination

        rule = {
            "rule": "date_comparison",
            "field1": "hire_date",
            "field2": "termination_date",
            "operator": "less_than"
        }

        result = self.dq_engine.validate_business_rule(invalid_data, rule)

        self.assertFalse(result["valid"])
        self.assertEqual(result["violations"], 1)

    def test_conditional_business_rule(self):
        """Test conditional business rule."""
        # Rule: If department is 'IT', salary must be > 40000
        rule = {
            "rule": "conditional",
            "condition": "department == 'IT'",
            "then": "salary > 40000"
        }

        result = self.dq_engine.validate_business_rule(self.business_data, rule)

        self.assertTrue(result["valid"])
        self.assertEqual(result["violations"], 0)

    def test_list_membership_business_rule(self):
        """Test list membership business rule."""
        rule = {
            "field": "department",
            "rule": "in_list",
            "values": ["HR", "IT", "Finance", "Marketing"]
        }

        result = self.dq_engine.validate_business_rule(self.business_data, rule)

        self.assertTrue(result["valid"])
        self.assertEqual(result["violations"], 0)

        # Test with invalid department
        invalid_data = self.business_data.copy()
        invalid_data.loc[0, 'department'] = 'InvalidDept'

        result = self.dq_engine.validate_business_rule(invalid_data, rule)

        self.assertFalse(result["valid"])
        self.assertEqual(result["violations"], 1)


class TestDataCompleteness(unittest.TestCase):
    """Test data completeness validation."""

    def setUp(self):
        """Setup test fixtures."""
        self.dq_engine = DQEngine(Mock())

        self.completeness_data = pd.DataFrame({
            'required_field': ['value1', None, 'value3', None, 'value5'],
            'optional_field': ['opt1', None, 'opt3', None, 'opt5'],
            'always_null_field': [None, None, None, None, None],
            'never_null_field': ['val1', 'val2', 'val3', 'val4', 'val5']
        })

    def test_required_field_completeness(self):
        """Test required field completeness."""
        rules = [{"field": "required_field", "rule": "not_null"}]

        result = self.dq_engine.run_completeness_checks(self.completeness_data, rules)

        self.assertFalse(result["required_field"]["passed"])
        self.assertEqual(result["required_field"]["null_count"], 2)
        self.assertEqual(result["required_field"]["total_count"], 5)

    def test_optional_field_completeness(self):
        """Test optional field completeness (should still report stats)."""
        rules = [{"field": "optional_field", "rule": "not_null"}]

        result = self.dq_engine.run_completeness_checks(self.completeness_data, rules)

        self.assertFalse(result["optional_field"]["passed"])
        self.assertEqual(result["optional_field"]["null_count"], 2)

    def test_always_null_field_completeness(self):
        """Test field that is always null."""
        rules = [{"field": "always_null_field", "rule": "not_null"}]

        result = self.dq_engine.run_completeness_checks(self.completeness_data, rules)

        self.assertFalse(result["always_null_field"]["passed"])
        self.assertEqual(result["always_null_field"]["null_count"], 5)

    def test_never_null_field_completeness(self):
        """Test field that is never null."""
        rules = [{"field": "never_null_field", "rule": "not_null"}]

        result = self.dq_engine.run_completeness_checks(self.completeness_data, rules)

        self.assertTrue(result["never_null_field"]["passed"])
        self.assertEqual(result["never_null_field"]["null_count"], 0)

    def test_multiple_field_completeness(self):
        """Test completeness check for multiple fields."""
        rules = [
            {"field": "required_field", "rule": "not_null"},
            {"field": "never_null_field", "rule": "not_null"},
            {"field": "always_null_field", "rule": "not_null"}
        ]

        result = self.dq_engine.run_completeness_checks(self.completeness_data, rules)

        self.assertFalse(result["required_field"]["passed"])
        self.assertTrue(result["never_null_field"]["passed"])
        self.assertFalse(result["always_null_field"]["passed"])

    def test_completeness_percentage_calculation(self):
        """Test completeness percentage calculation."""
        # Field with 60% completeness (3 not null out of 5)
        mixed_data = pd.DataFrame({
            'mixed_field': ['val1', 'val2', 'val3', None, None]
        })

        rules = [{"field": "mixed_field", "rule": "not_null"}]
        result = self.dq_engine.run_completeness_checks(mixed_data, rules)

        expected_completeness = 60.0  # 3/5 * 100
        self.assertAlmostEqual(result["mixed_field"]["completeness_percent"], expected_completeness)


if __name__ == '__main__':
    unittest.main()
    #</content>
#<parameter name="filePath">d:\development\2026\metadata-etl-framework\tests\data_validation_tests.py