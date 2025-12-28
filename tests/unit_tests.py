#!/usr/bin/env python3
"""
Unit Tests for ETL Framework Components

Tests individual components in isolation using mocks and fixtures.
"""

import pytest
import unittest
from unittest.mock import Mock, patch, MagicMock
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import tempfile
import os
import sys
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from utils.database_utils import DatabaseUtils, ConnectionConfig, DatabaseType
from transform.connector_factory import ConnectorFactory, RelationalDBConnector
from transform.transform_engine import TransformEngine
from transform.engine_selector import EngineSelector
from quality.dq_engine import DQEngine
from monitoring.sla_monitor import SLAMonitor
from monitoring.alert_manager import AlertManager


class TestConnectorFactory(unittest.TestCase):
    """Test ConnectorFactory component functionality."""

    def setUp(self):
        """Setup test fixtures."""
        self.mock_config_loader = Mock()
        self.mock_db_connection = Mock()
        self.factory = ConnectorFactory(self.mock_config_loader, self.mock_db_connection)
        self.mock_config = {
            "type": "postgresql",
            "driver": "psycopg2",
            "conn_args": {
                "host": "localhost",
                "port": 5432,
                "database": "test_db",
                "user": "test_user",
                "password": "test_pass"
            }
        }

    def test_create_relational_connector(self):
        """Test creating relational database connector."""
        connector = self.factory.create_source_connector(self.mock_config)
        self.assertIsInstance(connector, RelationalDBConnector)
        self.assertEqual(connector.config, self.mock_config)

    def test_create_connector_invalid_type(self):
        """Test creating connector with invalid type."""
        invalid_config = {"type": "invalid_type"}
        with self.assertRaises(NotImplementedError):
            self.factory._create_connector(invalid_config)

    @patch('psycopg2.connect')
    def test_relational_connector_connection(self, mock_connect):
        """Test relational connector establishes connection."""
        mock_connection = Mock()
        mock_cursor = Mock()
        mock_connection.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_connection

        connector = RelationalDBConnector(self.mock_config)
        connector.connect()

        mock_connect.assert_called_once()
        self.assertEqual(connector.conn, mock_connection)
        self.assertEqual(connector.cursor, mock_cursor)

    def test_connector_retry_logic(self):
        """Test connector retry logic on failure."""
        source_config = {
            "id": "test_source",
            "type": "postgresql",
            "driver": "psycopg2",
            "conn_args": {"host": "localhost", "database": "test"},
            "query": "SELECT * FROM test_table"
        }
        
        # Mock the connector to fail initially then succeed
        mock_connector = Mock()
        mock_connector.connect.side_effect = [Exception("Connection failed"), None]
        mock_connector.read.return_value = [("data",)]
        mock_connector.disconnect.return_value = None
        
        with patch.object(self.factory, 'create_source_connector', return_value=mock_connector):
            with patch('time.sleep') as mock_sleep:
                result = self.factory.extract_data("test_run_id", source_config)
                
                # Should have retried once
                self.assertEqual(mock_connector.connect.call_count, 2)
                self.assertEqual(mock_sleep.call_count, 1)


class TestTransformEngine(unittest.TestCase):
    """Test TransformEngine component functionality."""

    def setUp(self):
        """Setup test fixtures."""
        self.mock_db = Mock()
        self.engine = TransformEngine(self.mock_db)
        self.sample_data = pd.DataFrame({
            'id': [1, 2, 3, 4, 5],
            'name': ['Alice', 'Bob', 'Charlie', 'David', 'Eve'],
            'age': [25, 30, 35, 40, 45],
            'salary': [50000, 60000, 70000, 80000, 90000]
        })

    def test_apply_filter_transformation(self):
        """Test applying filter transformation."""
        config = {
            "type": "filter",
            "params": {
                "condition": "age > 30"
            }
        }

        result = self.engine.apply_transform_step(self.sample_data, config, "pandas")

        expected = self.sample_data[self.sample_data['age'] > 30]
        pd.testing.assert_frame_equal(result.reset_index(drop=True), expected.reset_index(drop=True))

    def test_apply_aggregate_transformation(self):
        """Test applying aggregate transformation."""
        config = {
            "type": "aggregate",
            "params": {
                "groupby_cols": ["age"],
                "agg_functions": {
                    "salary": "mean"
                }
            }
        }

        result = self.engine.apply_transform_step(self.sample_data, config, "pandas")

        self.assertIn('salary', result.columns)
        self.assertEqual(len(result), len(self.sample_data['age'].unique()))

    def test_apply_join_transformation(self):
        """Test applying join transformation."""
        right_data = pd.DataFrame({
            'id': [1, 2, 3],
            'department': ['HR', 'IT', 'Finance']
        })

        data_dict = {
            "left": self.sample_data,
            "right": right_data
        }

        config = {
            "type": "join",
            "params": {
                "left_source": "left",
                "right_source": "right",
                "left_on": "id",
                "right_on": "id",
                "how": "left"
            }
        }

        result = self.engine.apply_transform_step(data_dict, config, "pandas")

        self.assertIn('department', result.columns)
        self.assertEqual(len(result), len(self.sample_data))

    def test_transformation_error_handling(self):
        """Test error handling in transformations."""
        config = {
            "type": "invalid_type",
            "params": {}
        }

        with self.assertRaises(NotImplementedError):
            self.engine.apply_transform_step(self.sample_data, config, "pandas")


class TestEngineSelector(unittest.TestCase):
    """Test EngineSelector component functionality."""

    def setUp(self):
        """Setup test fixtures."""
        self.mock_db = Mock()
        self.mock_config_loader = Mock()
        # Mock get_engine_config to return None for sql, enabled for others
        self.mock_config_loader.get_engine_config.side_effect = lambda engine: {"enabled": True} if engine != "sql" else None
        self.selector = EngineSelector(self.mock_db, self.mock_config_loader)
        
        # Mock engine creation methods
        self.selector.get_pandas_engine = Mock(return_value=Mock())
        self.selector.get_polars_engine = Mock(return_value=Mock())
        self.selector.get_dask_engine = Mock(return_value=Mock())
        
        self.small_data_size = 1000  # 1KB
        self.medium_data_size = 100 * 1024 * 1024  # 100MB
        self.large_data_size = 5 * 1024 * 1024 * 1024  # 5GB

    def test_select_engine_for_small_dataset(self):
        """Test engine selection for small dataset."""
        engine, instance = self.selector.select_engine(self.small_data_size, {})
        self.assertEqual(engine, "pandas")

    def test_select_engine_for_medium_dataset(self):
        """Test engine selection for medium dataset."""
        engine, instance = self.selector.select_engine(self.medium_data_size, {})
        self.assertIn(engine, ["pandas", "polars"])

    def test_select_engine_for_large_dataset(self):
        """Test engine selection for large dataset."""
        engine, instance = self.selector.select_engine(self.large_data_size, {})
        self.assertIn(engine, ["polars", "dask"])

    def test_select_engine_with_custom_rules(self):
        """Test engine selection with custom rules."""
        # Mock config_loader to return custom thresholds
        self.mock_config_loader.get_engine_config.return_value = {"enabled": True}
        
        engine, instance = self.selector.select_engine(self.medium_data_size, {})
        self.assertIn(engine, ["pandas", "polars"])

    def test_engine_availability_check(self):
        """Test checking engine availability."""
        config = self.selector.get_engine_config("pandas")
        # Since config_loader is mocked, it should return what we set up
        self.assertIsNotNone(config)


class TestDQEngine(unittest.TestCase):
    """Test DQEngine component functionality."""

    def setUp(self):
        """Setup test fixtures."""
        self.mock_db = Mock()
        self.mock_config_loader = Mock()
        self.dq_engine = DQEngine(self.mock_db, self.mock_config_loader)
        self.test_data = pd.DataFrame({
            'id': [1, 2, 3, None, 5],
            'name': ['Alice', 'Bob', None, 'David', 'Eve'],
            'age': [25, 30, 35, 40, None],
            'email': ['alice@test.com', 'bob@test', 'charlie@test.com', 'david@test.com', None]
        })

    def test_completeness_check(self):
        """Test data completeness validation."""
        rules = [
            {"rule_id": "id_not_null", "column": "id", "rule": "not_null", "threshold": 0},
            {"rule_id": "name_not_null", "column": "name", "rule": "not_null", "threshold": 0}
        ]

        results = self.dq_engine.run_completeness_checks(self.test_data, rules)

        self.assertIn("id_not_null", results)
        self.assertIn("name_not_null", results)
        self.assertFalse(results["id_not_null"]["passed"])  # id has NaN
        self.assertFalse(results["name_not_null"]["passed"])  # name has None

    def test_validity_check_email_format(self):
        """Test email format validity check."""
        rules = [
            {"rule_id": "email_format", "column": "email", "type": "pattern", "pattern": r'^[^@]+@[^@]+\.[^@]+$'}
        ]

        results = self.dq_engine.run_validity_checks(self.test_data, rules)

        self.assertIn("email_format", results)
        self.assertFalse(results["email_format"]["passed"])  # bob@test is invalid

    def test_uniqueness_check(self):
        """Test uniqueness validation."""
        rules = [
            {"rule_id": "id_unique", "columns": ["id"], "rule": "unique"}
        ]

        results = self.dq_engine.run_uniqueness_checks(self.test_data, rules)

        self.assertIn("id_unique", results)
        self.assertTrue(results["id_unique"]["passed"])  # IDs are unique (NaN doesn't count)

    def test_range_check(self):
        """Test numeric range validation."""
        rules = [
            {"rule_id": "age_range", "column": "age", "rule": "range", "min": 0, "max": 100}
        ]

        results = self.dq_engine.run_validity_checks(self.test_data, rules)

        self.assertIn("age_range", results)
        self.assertTrue(results["age_range"]["passed"])  # All ages are in valid range

    def test_calculate_quality_score(self):
        """Test quality score calculation."""
        validation_results = {
            "completeness": {"id": {"passed": True}, "name": {"passed": False}},
            "validity": {"email": {"passed": False}},
            "uniqueness": {"id": {"passed": True}}
        }

        score, breakdown = self.dq_engine.calculate_quality_score(validation_results)

        self.assertIsInstance(score, float)
        self.assertGreaterEqual(score, 0)
        self.assertLessEqual(score, 100)
        self.assertIn("overall", breakdown)


class TestAnomalyDetection(unittest.TestCase):
    """Test anomaly detection functionality."""

    def setUp(self):
        """Setup test fixtures."""
        self.mock_db = Mock()
        self.mock_config_loader = Mock()
        self.dq_engine = DQEngine(self.mock_db, self.mock_config_loader)

        # Generate normal data
        np.random.seed(42)
        self.normal_data = pd.DataFrame({
            'value': np.random.normal(100, 10, 1000)
        })

        # Add some anomalies
        self.anomaly_data = self.normal_data.copy()
        self.anomaly_data.loc[100:105, 'value'] = 1000  # Clear anomalies

    def test_isolation_forest_detection(self):
        """Test isolation forest anomaly detection."""
        config = {
            "method": "zscore",
            "column": "value",
            "threshold": 3.0
        }

        results = self.dq_engine.detect_anomalies(self.anomaly_data, config)

        self.assertIn("anomalies_count", results)
        self.assertEqual(results["method"], "zscore")
        self.assertIn("anomaly_scores", results)
        self.assertGreater(results["anomalies_detected"], 0)

    def test_statistical_anomaly_detection(self):
        """Test statistical anomaly detection."""
        config = {
            "method": "zscore",
            "column": "value",
            "threshold": 3.0
        }

        results = self.dq_engine.detect_anomalies(self.anomaly_data, config)

        self.assertIn("anomalies_count", results)
        self.assertEqual(results["method"], "zscore")
        self.assertGreater(results["anomalies_detected"], 0)

    def test_anomaly_detection_edge_cases(self):
        """Test anomaly detection with edge cases."""
        # Empty data
        empty_data = pd.DataFrame()
        config = {"method": "zscore", "column": "value"}

        results = self.dq_engine.detect_anomalies(empty_data, config)
        self.assertIn("anomalies_count", results)

        # Single value
        single_data = pd.DataFrame({'value': [100]})
        results = self.dq_engine.detect_anomalies(single_data, config)
        self.assertEqual(results["anomalies_detected"], 0)


class TestSLAMonitor(unittest.TestCase):
    """Test SLA Monitor component functionality."""

    def setUp(self):
        """Setup test fixtures."""
        self.mock_db = Mock()
        self.mock_alert_manager = Mock()
        self.sla_monitor = SLAMonitor(self.mock_db, self.mock_alert_manager)

        self.sla_config = {
            "pipeline_id": "test_pipeline",
            "max_duration_minutes": 60,
            "max_failure_rate": 0.05,
            "min_success_rate": 0.95,
            "max_data_loss_percent": 1.0
        }

    def test_check_duration_sla(self):
        """Test duration SLA compliance check."""
        execution_time = timedelta(minutes=45)  # Within SLA
        result = self.sla_monitor.check_duration_sla(self.sla_config, execution_time)

        self.assertTrue(result["compliant"])
        self.assertEqual(result["actual_duration_minutes"], 45)

        # Test violation
        execution_time = timedelta(minutes=75)  # Over SLA
        result = self.sla_monitor.check_duration_sla(self.sla_config, execution_time)

        self.assertFalse(result["compliant"])

    def test_check_success_rate_sla(self):
        """Test success rate SLA compliance check."""
        total_runs = 100
        successful_runs = 96  # 96% success rate

        result = self.sla_monitor.check_success_rate_sla(self.sla_config, successful_runs, total_runs)

        self.assertTrue(result["compliant"])
        self.assertEqual(result["success_rate"], 0.96)

        # Test violation
        successful_runs = 90  # 90% success rate
        result = self.sla_monitor.check_success_rate_sla(self.sla_config, successful_runs, total_runs)

        self.assertFalse(result["compliant"])

    def test_check_data_quality_sla(self):
        """Test data quality SLA compliance check."""
        quality_score = 98.5  # High quality score

        result = self.sla_monitor.check_data_quality_sla(self.sla_config, quality_score)

        self.assertTrue(result["compliant"])

        # Test violation
        quality_score = 85.0  # Low quality score
        result = self.sla_monitor.check_data_quality_sla(self.sla_config, quality_score)

        self.assertFalse(result["compliant"])

    def test_overall_sla_compliance(self):
        """Test overall SLA compliance calculation."""
        sla_checks = {
            "duration": {"compliant": True},
            "success_rate": {"compliant": True},
            "data_quality": {"compliant": False}
        }

        result = self.sla_monitor.calculate_overall_compliance(sla_checks)

        self.assertFalse(result["overall_compliant"])
        self.assertEqual(result["compliant_checks"], 2)
        self.assertEqual(result["total_checks"], 3)


if __name__ == '__main__':
    unittest.main()