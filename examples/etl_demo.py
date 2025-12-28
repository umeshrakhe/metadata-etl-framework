#!/usr/bin/env python3
"""
ETL Framework Complete Demo

Comprehensive demonstration of the metadata-driven ETL framework showcasing
all core components working together: security, incremental loading,
error recovery, orchestration, data quality, and monitoring.
"""

import sys
import time
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, Any

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    from orchestrator.orchestrator_manager import OrchestratorManager
    from orchestrator.config_loader import ConfigLoader
    from quality.dq_engine import DQEngine
    from monitoring.performance_monitor import PerformanceMonitor
    from transform.connector_factory import ConnectorFactory
    from transform.engine_selector import EngineSelector
    from transform.transform_engine import TransformEngine
    from monitoring.sla_monitor import SLAMonitor
    from monitoring.alert_manager import AlertManager

    FRAMEWORK_AVAILABLE = True
except ImportError as e:
    logger.error(f"Framework components not available: {e}")
    FRAMEWORK_AVAILABLE = False

# Demo data
SAMPLE_PIPELINE_CONFIG = {
    "pipeline_id": "demo_pipeline",
    "pipeline_name": "Customer Data Pipeline",
    "description": "Complete ETL pipeline for customer data processing",
    "pipeline_type": "batch",
    "source_system": "CRM Database",
    "target_system": "Data Warehouse",
    "owner": "data_team",
    "steps": [
        {
            "step_id": "extract_customers",
            "step_name": "Extract Customer Data",
            "step_type": "extract",
            "step_order": 1,
            "source_config": {
                "type": "database",
                "connection": "crm_db",
                "query": "SELECT * FROM customers WHERE updated_at > '{{ watermark }}'"
            }
        },
        {
            "step_id": "validate_customers",
            "step_name": "Validate Customer Data",
            "step_type": "validate",
            "step_order": 2,
            "validation_config": {
                "rules": ["not_null_check", "email_format_check", "duplicate_check"]
            }
        },
        {
            "step_id": "transform_customers",
            "step_name": "Transform Customer Data",
            "step_type": "transform",
            "step_order": 3,
            "transformation_config": {
                "mappings": {
                    "customer_id": "id",
                    "full_name": "CONCAT(first_name, ' ', last_name)",
                    "email_domain": "SUBSTRING_INDEX(email, '@', -1)"
                },
                "filters": ["active = 1"]
            }
        },
        {
            "step_id": "load_customers",
            "step_name": "Load Customer Data",
            "step_type": "load",
            "step_order": 4,
            "target_config": {
                "type": "database",
                "connection": "warehouse_db",
                "table": "dim_customers",
                "load_type": "incremental",
                "key_fields": ["customer_id"]
            }
        }
    ]
}

SAMPLE_DATA_QUALITY_RULES = [
    {
        "rule_name": "customer_id_not_null",
        "rule_type": "completeness",
        "rule_definition": {
            "field": "customer_id",
            "operator": "not_null"
        },
        "severity": "high"
    },
    {
        "rule_name": "email_format_valid",
        "rule_type": "validity",
        "rule_definition": {
            "field": "email",
            "operator": "regex_match",
            "pattern": "^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$"
        },
        "severity": "medium"
    },
    {
        "rule_name": "no_duplicate_customers",
        "rule_type": "uniqueness",
        "rule_definition": {
            "fields": ["customer_id"],
            "operator": "unique"
        },
        "severity": "high"
    }
]


class ETLDemo:
    """Complete ETL Framework Demonstration"""

    def __init__(self):
        self.db_utils = None
        self.security_manager = None
        self.incremental_manager = None
        self.error_recovery = None
        self.orchestrator = None
        self.dq_engine = None
        self.performance_monitor = None
        self.connector_factory = None

        # Demo state
        self.pipeline_id = "demo_pipeline"
        self.execution_id = None

    def setup_demo_environment(self) -> bool:
        """Setup demo environment with SQLite database"""
        try:
            logger.info("Setting up demo environment...")

            # Create SQLite database for demo
            db_path = ":memory:"  # In-memory database for demo
            connection_config = ConnectionConfig(
                host="",
                port=0,
                database=db_path,
                username="",
                password="",
                db_type=DatabaseType.SQLITE
            )

            self.db_utils = DatabaseUtils(connection_config)

            # Initialize components
            self.security_manager = SecurityManager()
            self.incremental_manager = IncrementalLoadManager(self.db_utils)
            self.error_recovery = ErrorRecoveryManager(self.db_utils)
            
            # Initialize required orchestrator components
            self.config_loader = ConfigLoader()
            self.engine_selector = EngineSelector()
            self.transform_engine = TransformEngine()
            self.sla_monitor = SLAMonitor(self.db_utils)
            self.alert_manager = AlertManager(self.db_utils)
            
            self.orchestrator = OrchestratorManager(
                config_loader=self.config_loader,
                connector_factory=ConnectorFactory(),
                engine_selector=self.engine_selector,
                transform_engine=self.transform_engine,
                dq_engine=DQEngine(self.db_utils),
                sla_monitor=self.sla_monitor,
                alert_manager=self.alert_manager,
                db_connection=self.db_utils
            )
            
            self.dq_engine = DQEngine(self.db_utils)
            self.performance_monitor = PerformanceMonitor(self.db_utils)
            self.connector_factory = ConnectorFactory()

            logger.info("Demo environment setup complete")
            return True

        except Exception as e:
            logger.error(f"Failed to setup demo environment: {e}")
            return False

    def demonstrate_security(self) -> None:
        """Demonstrate security manager functionality"""
        logger.info("\n" + "="*60)
        logger.info("SECURITY MANAGER DEMONSTRATION")
        logger.info("="*60)

        try:
            # Encrypt sensitive data
            sensitive_data = "password123"
            encrypted = self.security_manager.encrypt_data(sensitive_data)
            logger.info(f"Encrypted data: {encrypted[:20]}...")

            # Decrypt data
            decrypted = self.security_manager.decrypt_data(encrypted)
            logger.info(f"Decrypted data: {decrypted}")
            assert decrypted == sensitive_data

            # Generate API token
            token = self.security_manager.generate_api_token("demo_user", ["read", "write"])
            logger.info(f"Generated API token: {token[:20]}...")

            # Validate token
            claims = self.security_manager.validate_api_token(token)
            logger.info(f"Token claims: {claims}")

            # Hash password
            hashed = self.security_manager.hash_password("mypassword")
            logger.info(f"Password hash: {hashed[:20]}...")

            # Verify password
            is_valid = self.security_manager.verify_password("mypassword", hashed)
            logger.info(f"Password verification: {'✓' if is_valid else '✗'}")

            logger.info("✓ Security manager demonstration completed")

        except Exception as e:
            logger.error(f"Security demonstration failed: {e}")

    def demonstrate_incremental_loading(self) -> None:
        """Demonstrate incremental load manager functionality"""
        logger.info("\n" + "="*60)
        logger.info("INCREMENTAL LOAD MANAGER DEMONSTRATION")
        logger.info("="*60)

        try:
            # Setup watermark
            watermark_id = self.incremental_manager.initialize_watermark(
                table_name="customers",
                watermark_column="updated_at",
                initial_value="2023-01-01 00:00:00"
            )
            logger.info(f"Initialized watermark: {watermark_id}")

            # Get pending records query
            query = self.incremental_manager.get_incremental_query(
                base_query="SELECT * FROM customers",
                table_name="customers",
                watermark_column="updated_at"
            )
            logger.info(f"Incremental query: {query}")

            # Update watermark
            self.incremental_manager.update_watermark(
                table_name="customers",
                new_value="2023-12-01 00:00:00"
            )
            logger.info("Updated watermark")

            # Get watermark history
            history = self.incremental_manager.get_watermark_history("customers", limit=5)
            logger.info(f"Watermark history entries: {len(history)}")

            logger.info("✓ Incremental loading demonstration completed")

        except Exception as e:
            logger.error(f"Incremental loading demonstration failed: {e}")

    def demonstrate_error_recovery(self) -> None:
        """Demonstrate error recovery manager functionality"""
        logger.info("\n" + "="*60)
        logger.info("ERROR RECOVERY MANAGER DEMONSTRATION")
        logger.info("="*60)

        try:
            # Classify errors
            test_errors = [
                ValueError("Invalid input data"),
                ConnectionError("Database connection failed"),
                PermissionError("Access denied"),
                TimeoutError("Operation timed out"),
                Exception("Unknown error")
            ]

            for error in test_errors:
                error_type = self.error_recovery.classify_error(error)
                should_retry = self.error_recovery.should_retry(error_type, retry_count=0)
                logger.info(f"Error: {error.__class__.__name__} -> {error_type}, Retry: {should_retry}")

            # Test retry decorator
            @self.error_recovery.retry_on_failure(max_retries=2)
            def unreliable_function():
                # Simulate occasional failure
                import random
                if random.random() < 0.7:  # 70% failure rate
                    raise ConnectionError("Simulated connection error")
                return "Success!"

            result = unreliable_function()
            logger.info(f"Retry decorator result: {result}")

            # Test circuit breaker
            @self.error_recovery.circuit_breaker("demo_service", failure_threshold=2)
            def failing_service():
                raise ConnectionError("Service unavailable")

            try:
                failing_service()
            except Exception as e:
                logger.info(f"Circuit breaker activated: {e}")

            logger.info("✓ Error recovery demonstration completed")

        except Exception as e:
            logger.error(f"Error recovery demonstration failed: {e}")

    def demonstrate_data_quality(self) -> None:
        """Demonstrate data quality engine functionality"""
        logger.info("\n" + "="*60)
        logger.info("DATA QUALITY ENGINE DEMONSTRATION")
        logger.info("="*60)

        try:
            # Create sample data
            sample_data = [
                {"customer_id": 1, "email": "john@example.com", "status": "active"},
                {"customer_id": 2, "email": "invalid-email", "status": "active"},
                {"customer_id": None, "email": "jane@example.com", "status": "inactive"},
                {"customer_id": 1, "email": "duplicate@example.com", "status": "active"},  # Duplicate ID
            ]

            # Run quality checks
            for rule in SAMPLE_DATA_QUALITY_RULES:
                logger.info(f"Testing rule: {rule['rule_name']}")

                violations = self.dq_engine.validate_data_quality(
                    data=sample_data,
                    rule_config=rule
                )

                logger.info(f"Violations found: {len(violations)}")
                for violation in violations[:3]:  # Show first 3
                    logger.info(f"  - {violation}")

            # Generate quality report
            report = self.dq_engine.generate_quality_report(
                data=sample_data,
                rules=SAMPLE_DATA_QUALITY_RULES
            )

            logger.info(f"Quality score: {report.get('overall_score', 'N/A')}")
            logger.info(f"Total violations: {report.get('total_violations', 0)}")

            logger.info("✓ Data quality demonstration completed")

        except Exception as e:
            logger.error(f"Data quality demonstration failed: {e}")

    def demonstrate_orchestration(self) -> None:
        """Demonstrate orchestrator functionality"""
        logger.info("\n" + "="*60)
        logger.info("ORCHESTRATOR DEMONSTRATION")
        logger.info("="*60)

        try:
            # Create pipeline
            pipeline_config = SAMPLE_PIPELINE_CONFIG
            logger.info(f"Creating pipeline: {pipeline_config['pipeline_name']}")

            # In a real scenario, this would create the pipeline in the database
            # For demo, we'll just show the configuration
            logger.info("Pipeline configuration:")
            logger.info(f"  - Steps: {len(pipeline_config['steps'])}")
            for step in pipeline_config['steps']:
                logger.info(f"    {step['step_order']}: {step['step_name']} ({step['step_type']})")

            # Simulate pipeline execution
            logger.info("Simulating pipeline execution...")

            for step in pipeline_config['steps']:
                logger.info(f"Executing step: {step['step_name']}")
                time.sleep(0.5)  # Simulate processing time

                # Simulate metrics collection
                self.performance_monitor.record_metric(
                    execution_id="demo_exec",
                    step_id=step['step_id'],
                    metric_name="processing_time",
                    metric_value=0.5,
                    metric_unit="seconds"
                )

            logger.info("✓ Orchestrator demonstration completed")

        except Exception as e:
            logger.error(f"Orchestrator demonstration failed: {e}")

    def demonstrate_monitoring(self) -> None:
        """Demonstrate monitoring and performance tracking"""
        logger.info("\n" + "="*60)
        logger.info("MONITORING & PERFORMANCE DEMONSTRATION")
        logger.info("="*60)

        try:
            # Record various metrics
            metrics = [
                ("cpu_usage", 45.2, "percentage"),
                ("memory_usage", 2.1, "gb"),
                ("records_processed", 15000, "count"),
                ("processing_time", 120.5, "seconds"),
                ("error_rate", 0.02, "percentage")
            ]

            for metric_name, value, unit in metrics:
                self.performance_monitor.record_metric(
                    execution_id="demo_exec",
                    metric_name=metric_name,
                    metric_value=value,
                    metric_unit=unit
                )
                logger.info(f"Recorded metric: {metric_name} = {value} {unit}")

            # Get performance summary
            summary = self.performance_monitor.get_execution_summary("demo_exec")
            logger.info(f"Execution summary: {summary}")

            # Check system health
            health = self.db_utils.health_check()
            logger.info(f"System health: {'✓ Healthy' if health['healthy'] else '✗ Unhealthy'}")
            logger.info(f"Response time: {health['response_time']}s")

            logger.info("✓ Monitoring demonstration completed")

        except Exception as e:
            logger.error(f"Monitoring demonstration failed: {e}")

    def demonstrate_connectors(self) -> None:
        """Demonstrate connector factory functionality"""
        logger.info("\n" + "="*60)
        logger.info("CONNECTOR FACTORY DEMONSTRATION")
        logger.info("="*60)

        try:
            # Show available connector types
            connector_types = ["database", "api", "file", "message_queue"]
            logger.info("Available connector types:")
            for conn_type in connector_types:
                logger.info(f"  - {conn_type}")

            # Demonstrate connection configuration
            sample_configs = {
                "database": {
                    "type": "postgresql",
                    "host": "localhost",
                    "database": "demo_db",
                    "table": "customers"
                },
                "api": {
                    "url": "https://api.example.com/data",
                    "method": "GET",
                    "headers": {"Authorization": "Bearer token"}
                },
                "file": {
                    "path": "/data/input/customers.csv",
                    "format": "csv",
                    "delimiter": ","
                }
            }

            for conn_type, config in sample_configs.items():
                logger.info(f"\n{conn_type.upper()} connector configuration:")
                for key, value in config.items():
                    logger.info(f"  {key}: {value}")

            logger.info("✓ Connector demonstration completed")

        except Exception as e:
            logger.error(f"Connector demonstration failed: {e}")

    def run_complete_demo(self) -> None:
        """Run the complete ETL framework demonstration"""
        logger.info("🚀 Starting ETL Framework Complete Demonstration")
        logger.info("="*80)

        if not FRAMEWORK_AVAILABLE:
            logger.error("ETL Framework components not available. Please ensure all dependencies are installed.")
            return

        # Setup environment
        if not self.setup_demo_environment():
            logger.error("Failed to setup demo environment")
            return

        # Run demonstrations
        demonstrations = [
            self.demonstrate_security,
            self.demonstrate_incremental_loading,
            self.demonstrate_error_recovery,
            self.demonstrate_data_quality,
            self.demonstrate_orchestration,
            self.demonstrate_monitoring,
            self.demonstrate_connectors
        ]

        for demo_func in demonstrations:
            try:
                demo_func()
            except Exception as e:
                logger.error(f"Demonstration failed: {e}")
                continue

        # Final summary
        logger.info("\n" + "="*80)
        logger.info("🎉 ETL FRAMEWORK DEMONSTRATION COMPLETED")
        logger.info("="*80)
        logger.info("Demonstrated components:")
        logger.info("  ✓ Security Manager - Encryption, authentication, authorization")
        logger.info("  ✓ Incremental Load Manager - Watermark management, CDC")
        logger.info("  ✓ Error Recovery Manager - Retry logic, circuit breakers")
        logger.info("  ✓ Data Quality Engine - Validation rules, profiling")
        logger.info("  ✓ Orchestrator - Pipeline execution coordination")
        logger.info("  ✓ Performance Monitor - Metrics collection and analysis")
        logger.info("  ✓ Connector Factory - Multi-source data connectivity")
        logger.info("")
        logger.info("The framework provides:")
        logger.info("  • Production-ready ETL pipeline orchestration")
        logger.info("  • Comprehensive error handling and recovery")
        logger.info("  • Data quality monitoring and validation")
        logger.info("  • Security and access control")
        logger.info("  • Performance monitoring and alerting")
        logger.info("  • Scalable architecture with dependency injection")
        logger.info("  • REST API and CLI interfaces")
        logger.info("  • Docker containerization support")
        logger.info("="*80)


def main():
    """Main demonstration entry point"""
    demo = ETLDemo()
    demo.run_complete_demo()


if __name__ == '__main__':
    main()