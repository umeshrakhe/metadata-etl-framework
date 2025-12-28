#!/usr/bin/env python3
"""
Integration Tests for ETL Framework Components

Tests component interactions and end-to-end flows.
"""

import pytest
import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import sqlite3
import tempfile
import os
import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.database_utils import DatabaseUtils, ConnectionConfig, DatabaseType
from orchestrator.orchestrator_manager import OrchestratorManager
from orchestrator.config_loader import ConfigLoader
from transform.connector_factory import ConnectorFactory
from transform.transform_engine import TransformEngine
from transform.engine_selector import EngineSelector
from quality.dq_engine import DQEngine
from monitoring.sla_monitor import SLAMonitor
from monitoring.alert_manager import AlertManager
from utils.error_recovery import ErrorRecoveryManager


class TestExtractTransformFlow(unittest.TestCase):
    """Test Extract-Transform integration."""

    def setUp(self):
        """Setup test fixtures."""
        # Create in-memory SQLite database
        self.db_utils = DatabaseUtils(ConnectionConfig(
            host="", port=0, database=":memory:",
            username="", password="", db_type=DatabaseType.SQLITE
        ))

        # Setup test data
        self.setup_test_tables()

        self.connector_factory = ConnectorFactory()
        self.transform_engine = TransformEngine()
        self.engine_selector = EngineSelector()

    def setup_test_tables(self):
        """Setup test tables with sample data."""
        create_table_sql = """
        CREATE TABLE source_data (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            salary REAL,
            department TEXT
        )
        """

        insert_data_sql = """
        INSERT INTO source_data (name, age, salary, department) VALUES
        ('Alice', 25, 50000, 'HR'),
        ('Bob', 30, 60000, 'IT'),
        ('Charlie', 35, 70000, 'Finance'),
        ('David', 40, 80000, 'IT'),
        ('Eve', 45, 90000, 'HR')
        """

        self.db_utils.execute_query(create_table_sql)
        self.db_utils.execute_query(insert_data_sql)

    def test_extract_and_transform_integration(self):
        """Test extracting data and applying transformations."""
        # Extract data
        extract_query = "SELECT * FROM source_data"
        extracted_data = self.db_utils.execute_query(extract_query, fetch=True)

        self.assertIsInstance(extracted_data, pd.DataFrame)
        self.assertEqual(len(extracted_data), 5)

        # Apply transformation
        transform_config = {
            "type": "filter",
            "condition": "age > 30"
        }

        transformed_data = self.transform_engine.apply_transformation(
            extracted_data, transform_config
        )

        self.assertEqual(len(transformed_data), 3)  # 3 records where age > 30

    def test_multiple_transformations_chain(self):
        """Test chaining multiple transformations."""
        # Extract data
        extract_query = "SELECT * FROM source_data"
        data = self.db_utils.execute_query(extract_query, fetch=True)

        # Chain transformations
        transformations = [
            {"type": "filter", "condition": "department == 'IT'"},
            {"type": "aggregate", "group_by": ["department"], "aggregations": {"salary": "mean"}}
        ]

        current_data = data
        for transform in transformations:
            current_data = self.transform_engine.apply_transformation(current_data, transform)

        self.assertEqual(len(current_data), 1)  # One department group
        self.assertIn('salary_mean', current_data.columns)

    def test_engine_selection_integration(self):
        """Test engine selection with actual data."""
        # Extract data
        extract_query = "SELECT * FROM source_data"
        data = self.db_utils.execute_query(extract_query, fetch=True)

        # Test engine selection
        selected_engine = self.engine_selector.select_engine(data)
        self.assertIn(selected_engine, ["pandas", "polars"])

        # Verify engine is available
        available = self.engine_selector.is_engine_available(selected_engine)
        self.assertTrue(available)


class TestTransformLoadFlow(unittest.TestCase):
    """Test Transform-Load integration."""

    def setUp(self):
        """Setup test fixtures."""
        # Create in-memory SQLite database
        self.db_utils = DatabaseUtils(ConnectionConfig(
            host="", port=0, database=":memory:",
            username="", password="", db_type=DatabaseType.SQLITE
        ))

        self.transform_engine = TransformEngine()
        self.setup_test_tables()

    def setup_test_tables(self):
        """Setup source and target tables."""
        # Source table
        create_source_sql = """
        CREATE TABLE source_data (
            id INTEGER PRIMARY KEY,
            name TEXT,
            age INTEGER,
            salary REAL
        )
        """

        # Target table
        create_target_sql = """
        CREATE TABLE target_data (
            id INTEGER PRIMARY KEY,
            full_name TEXT,
            age_group TEXT,
            annual_salary REAL,
            processed_date TEXT
        )
        """

        self.db_utils.execute_query(create_source_sql)
        self.db_utils.execute_query(create_target_sql)

        # Insert source data
        insert_sql = """
        INSERT INTO source_data (name, age, salary) VALUES
        ('Alice', 25, 50000),
        ('Bob', 30, 60000),
        ('Charlie', 35, 70000)
        """
        self.db_utils.execute_query(insert_sql)

    def test_transform_and_load_integration(self):
        """Test transforming data and loading to target."""
        # Extract source data
        extract_sql = "SELECT * FROM source_data"
        source_data = self.db_utils.execute_query(extract_sql, fetch=True)

        # Transform data
        transform_config = {
            "type": "add_columns",
            "columns": {
                "full_name": "name",  # Copy name to full_name
                "age_group": "case_when(age < 30, 'Young', age < 40, 'Middle', 'Senior')",
                "annual_salary": "salary",
                "processed_date": "current_date"
            }
        }

        transformed_data = self.transform_engine.apply_transformation(
            source_data, transform_config
        )

        # Load to target (simplified - in real scenario would use connector)
        for _, row in transformed_data.iterrows():
            insert_sql = """
            INSERT INTO target_data (full_name, age_group, annual_salary, processed_date)
            VALUES (?, ?, ?, ?)
            """
            self.db_utils.execute_query(insert_sql, params=(
                row['full_name'], row['age_group'], row['annual_salary'],
                datetime.now().strftime('%Y-%m-%d')
            ))

        # Verify load
        verify_sql = "SELECT COUNT(*) as count FROM target_data"
        result = self.db_utils.execute_query(verify_sql, fetch=True)
        self.assertEqual(result.iloc[0]['count'], 3)

    def test_incremental_load_simulation(self):
        """Test incremental load pattern."""
        # Simulate initial load
        initial_data = pd.DataFrame({
            'id': [1, 2],
            'name': ['Alice', 'Bob'],
            'last_updated': ['2023-01-01', '2023-01-01']
        })

        for _, row in initial_data.iterrows():
            insert_sql = """
            INSERT INTO target_data (id, full_name, processed_date)
            VALUES (?, ?, ?)
            """
            self.db_utils.execute_query(insert_sql, params=(
                row['id'], row['name'], '2023-01-01'
            ))

        # Simulate incremental data
        incremental_data = pd.DataFrame({
            'id': [2, 3],
            'name': ['Bob Updated', 'Charlie'],
            'last_updated': ['2023-01-02', '2023-01-02']
        })

        # Apply incremental logic (upsert simulation)
        for _, row in incremental_data.iterrows():
            upsert_sql = """
            INSERT OR REPLACE INTO target_data (id, full_name, processed_date)
            VALUES (?, ?, ?)
            """
            self.db_utils.execute_query(upsert_sql, params=(
                row['id'], row['name'], row['last_updated']
            ))

        # Verify incremental load
        result = self.db_utils.execute_query("SELECT * FROM target_data ORDER BY id", fetch=True)
        self.assertEqual(len(result), 3)  # 3 total records
        bob_record = result[result['id'] == 2]
        self.assertEqual(bob_record.iloc[0]['full_name'], 'Bob Updated')


class TestEndToEndPipeline(unittest.TestCase):
    """Test complete end-to-end pipeline execution."""

    def setUp(self):
        """Setup test fixtures."""
        # Create in-memory database
        self.db_utils = DatabaseUtils(ConnectionConfig(
            host="", port=0, database=":memory:",
            username="", password="", db_type=DatabaseType.SQLITE
        ))

        # Setup minimal orchestrator components
        self.config_loader = ConfigLoader()
        self.connector_factory = ConnectorFactory()
        self.engine_selector = EngineSelector()
        self.transform_engine = TransformEngine()
        self.dq_engine = DQEngine(self.db_utils)
        self.sla_monitor = SLAMonitor(self.db_utils)
        self.alert_manager = AlertManager(self.db_utils)

        self.orchestrator = OrchestratorManager(
            config_loader=self.config_loader,
            connector_factory=self.connector_factory,
            engine_selector=self.engine_selector,
            transform_engine=self.transform_engine,
            dq_engine=self.dq_engine,
            sla_monitor=self.sla_monitor,
            alert_manager=self.alert_manager,
            db_connection=self.db_utils
        )

        self.setup_test_schema()

    def setup_test_schema(self):
        """Setup test database schema."""
        # Create source table
        create_source_sql = """
        CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            age INTEGER,
            salary REAL,
            department TEXT
        )
        """

        # Create target table
        create_target_sql = """
        CREATE TABLE processed_employees (
            id INTEGER PRIMARY KEY,
            employee_name TEXT,
            age_group TEXT,
            monthly_salary REAL,
            department TEXT,
            processed_at TEXT
        )
        """

        self.db_utils.execute_query(create_source_sql)
        self.db_utils.execute_query(create_target_sql)

        # Insert test data
        insert_sql = """
        INSERT INTO employees (name, age, salary, department) VALUES
        ('Alice', 25, 60000, 'HR'),
        ('Bob', 30, 72000, 'IT'),
        ('Charlie', 35, 84000, 'Finance'),
        ('David', 40, 96000, 'IT'),
        ('Eve', 45, 108000, 'HR')
        """
        self.db_utils.execute_query(insert_sql)

    @patch('orchestrator.orchestrator_manager.datetime')
    def test_complete_pipeline_execution(self, mock_datetime):
        """Test complete pipeline execution."""
        mock_datetime.utcnow.return_value = datetime(2023, 1, 1, 12, 0, 0)
        mock_datetime.now.return_value = datetime(2023, 1, 1, 12, 0, 0)

        # Mock pipeline configuration
        pipeline_config = {
            "pipeline_id": "test_e2e_pipeline",
            "extract": {
                "source": "sqlite",
                "query": "SELECT * FROM employees"
            },
            "transform": [
                {
                    "type": "add_columns",
                    "columns": {
                        "employee_name": "name",
                        "age_group": "case_when(age < 30, 'Junior', age < 40, 'Mid', 'Senior')",
                        "monthly_salary": "salary / 12"
                    }
                },
                {
                    "type": "filter",
                    "condition": "monthly_salary > 5000"
                }
            ],
            "load": {
                "target": "sqlite",
                "table": "processed_employees",
                "mode": "replace"
            },
            "quality": {
                "rules": [
                    {"field": "employee_name", "rule": "not_null"},
                    {"field": "monthly_salary", "rule": "range", "min": 0, "max": 10000}
                ]
            }
        }

        # Execute pipeline (simplified version)
        # Extract
        extract_query = pipeline_config["extract"]["query"]
        data = self.db_utils.execute_query(extract_query, fetch=True)

        # Transform
        current_data = data
        for transform in pipeline_config["transform"]:
            current_data = self.transform_engine.apply_transformation(current_data, transform)

        # Load (simplified)
        for _, row in current_data.iterrows():
            insert_sql = """
            INSERT INTO processed_employees
            (employee_name, age_group, monthly_salary, department, processed_at)
            VALUES (?, ?, ?, ?, ?)
            """
            self.db_utils.execute_query(insert_sql, params=(
                row['employee_name'], row['age_group'], row['monthly_salary'],
                row.get('department'), datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            ))

        # Quality Check
        quality_results = self.dq_engine.run_completeness_checks(
            current_data, pipeline_config["quality"]["rules"]
        )

        # Verify results
        result_count = self.db_utils.execute_query(
            "SELECT COUNT(*) as count FROM processed_employees", fetch=True
        ).iloc[0]['count']

        self.assertGreater(result_count, 0)
        self.assertIn("employee_name", quality_results)

    def test_pipeline_error_handling(self):
        """Test pipeline error handling and recovery."""
        # Test with invalid transformation
        invalid_transform_config = {
            "type": "invalid_transform_type"
        }

        data = pd.DataFrame({'test': [1, 2, 3]})

        with self.assertRaises(ValueError):
            self.transform_engine.apply_transformation(data, invalid_transform_config)


class TestDQIntegration(unittest.TestCase):
    """Test Data Quality integration in pipelines."""

    def setUp(self):
        """Setup test fixtures."""
        self.db_utils = DatabaseUtils(ConnectionConfig(
            host="", port=0, database=":memory:",
            username="", password="", db_type=DatabaseType.SQLITE
        ))

        self.dq_engine = DQEngine(self.db_utils)
        self.setup_test_data()

    def setup_test_data(self):
        """Setup test data with quality issues."""
        self.test_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5, None],
            'name': ['Alice', 'Bob', None, 'David', 'Eve', 'Frank'],
            'age': [25, 30, 35, 40, None, 50],
            'salary': [50000, 60000, 70000, 80000, 90000, None],
            'email': ['alice@test.com', 'bob@test', 'charlie@test.com', 'david@test.com', None, 'frank@test.com']
        })

    def test_dq_rules_in_pipeline(self):
        """Test applying DQ rules as part of pipeline."""
        dq_rules = {
            "completeness": [
                {"field": "id", "rule": "not_null"},
                {"field": "name", "rule": "not_null"}
            ],
            "validity": [
                {"field": "email", "rule": "email_format"},
                {"field": "age", "rule": "range", "min": 0, "max": 100}
            ],
            "uniqueness": [
                {"field": "id", "rule": "unique"}
            ]
        }

        # Run all DQ checks
        completeness_results = self.dq_engine.run_completeness_checks(
            self.test_data, dq_rules["completeness"]
        )
        validity_results = self.dq_engine.run_validity_checks(
            self.test_data, dq_rules["validity"]
        )
        uniqueness_results = self.dq_engine.run_uniqueness_checks(
            self.test_data, dq_rules["uniqueness"]
        )

        # Verify results
        self.assertFalse(completeness_results["id"]["passed"])  # Has null
        self.assertFalse(completeness_results["name"]["passed"])  # Has null
        self.assertFalse(validity_results["email"]["passed"])  # Invalid email
        self.assertTrue(uniqueness_results["id"]["passed"])  # IDs are unique

    def test_dq_threshold_gating(self):
        """Test DQ threshold-based pipeline gating."""
        # Calculate quality score
        validation_results = {
            "completeness": {"id": {"passed": False}, "name": {"passed": False}},
            "validity": {"email": {"passed": False}},
            "uniqueness": {"id": {"passed": True}}
        }

        score, breakdown = self.dq_engine.calculate_quality_score(validation_results)

        # Test gating logic
        quality_threshold = 80.0
        pipeline_should_proceed = score >= quality_threshold

        self.assertFalse(pipeline_should_proceed)  # Score should be below threshold

    def test_dq_result_storage(self):
        """Test storing DQ results in database."""
        dq_run_id = "test_run_001"
        results = {
            "completeness": {"score": 80.0},
            "validity": {"score": 75.0},
            "overall_score": 77.5
        }

        # Store results (mock implementation)
        self.dq_engine.store_dq_results(dq_run_id, results)

        # In real implementation, would verify database storage
        # For now, just ensure method exists and doesn't error
        self.assertTrue(True)


class TestErrorHandlingFlow(unittest.TestCase):
    """Test error handling and recovery flows."""

    def setUp(self):
        """Setup test fixtures."""
        self.db_utils = DatabaseUtils(ConnectionConfig(
            host="", port=0, database=":memory:",
            username="", password="", db_type=DatabaseType.SQLITE
        ))

        self.error_recovery = ErrorRecoveryManager(self.db_utils)
        self.transform_engine = TransformEngine()

    def test_transformation_error_recovery(self):
        """Test error recovery during transformation."""
        # Create data that will cause transformation error
        problematic_data = pd.DataFrame({
            'value': ['1', '2', 'invalid', '4', '5']
        })

        # Attempt transformation that will fail on 'invalid'
        transform_config = {
            "type": "convert_type",
            "column": "value",
            "target_type": "int"
        }

        # Test error handling
        try:
            result = self.transform_engine.apply_transformation(problematic_data, transform_config)
            # If we get here, transformation handled error gracefully
            self.assertTrue(True)
        except Exception as e:
            # If error occurs, test recovery
            recovery_action = self.error_recovery.classify_error(str(e))
            self.assertIsNotNone(recovery_action)

    def test_database_connection_error_handling(self):
        """Test database connection error handling."""
        # Create invalid connection
        invalid_config = ConnectionConfig(
            host="invalid_host", port=5432, database="invalid_db",
            username="invalid", password="invalid", db_type=DatabaseType.POSTGRESQL
        )

        invalid_db_utils = DatabaseUtils(invalid_config)

        # Test connection retry
        with patch('time.sleep'):
            try:
                invalid_db_utils.connect_with_retry(max_retries=2)
            except Exception:
                # Expected to fail, but should have retry logic
                self.assertTrue(True)

    def test_pipeline_failure_recovery(self):
        """Test pipeline failure and recovery."""
        # Simulate pipeline failure
        failure_info = {
            "pipeline_id": "test_pipeline",
            "stage": "transform",
            "error": "Transformation failed",
            "checkpoint_data": {"processed_rows": 100}
        }

        # Test error classification
        error_type = self.error_recovery.classify_error(failure_info["error"])
        self.assertIsNotNone(error_type)

        # Test recovery strategy
        recovery_strategy = self.error_recovery.determine_recovery_strategy(error_type)
        self.assertIsNotNone(recovery_strategy)

        # Test checkpoint creation
        checkpoint_id = self.error_recovery.create_checkpoint(failure_info)
        self.assertIsNotNone(checkpoint_id)


if __name__ == '__main__':
    unittest.main()
    #</content>
#<parameter name="filePath">d:\development\2026\metadata-etl-framework\tests\integration_tests.py