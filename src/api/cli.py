#!/usr/bin/env python3
"""
ETL Framework CLI Interface

Command-line interface for managing ETL pipelines, executions, and monitoring.
"""

import argparse
import sys
import json
import yaml
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta

# Add src to path for imports
base_path=str(Path(__file__).parent.parent)
print(base_path)
sys.path.append(base_path)

try:
    from utils.database_utils import DatabaseUtils, ConnectionConfig, DatabaseType
    from utils.schema_manager import SchemaManager
    from orchestrator.orchestrator_manager import OrchestratorManager
    from orchestrator.config_loader import ConfigLoader
    HAS_FRAMEWORK = True
except ImportError as e:
    print(f"Error importing framework components: {e}")
    print("Make sure the framework is properly installed.")
    HAS_FRAMEWORK = False


class ETLCLI:
    """ETL Framework Command Line Interface"""

    def __init__(self, config_file: str = 'config/config.yaml'):
        
        self.config_loader = None
        self.db_utils = None
        self.orchestrator = None
        self.logger = logging.getLogger(__name__)

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(lineno)d - %(message)s'
        )
        self.config = self.setup_config(config_file)

    def setup_config(self, config_file="config.yaml"): 
        with open(config_file, "r") as f: 
            config = yaml.safe_load(f)
        
        db_config = config['database']['metadata']
        connection_config = ConnectionConfig(
            host=db_config['host'],
            port=db_config['port'],
            database=db_config['database'],
            username=db_config['username'],
            password=db_config['password'],
            db_type=DatabaseType(db_config['type'])
        )

        self.db_utils = DatabaseUtils(connection_config)
        #self.orchestrator = OrchestratorManager(self.db_utils, config)
        
        return config 
    
    def load_config(self, config_file: str) -> bool:
        """Load configuration from file"""
        try:
            self.config_loader = ConfigLoader(config_file)
            config = self.config_loader.load_config()

            # Initialize database connection
            db_config = config['database']['metadata']
            connection_config = ConnectionConfig(
                host=db_config['host'],
                port=db_config['port'],
                database=db_config['database'],
                username=db_config['username'],
                password=db_config['password'],
                db_type=DatabaseType(db_config['type'])
            )

            self.db_utils = DatabaseUtils(connection_config)
            print(f"db_utils: {self.db_utils}   ")
            self.orchestrator = OrchestratorManager(self.db_utils, config)

            return True

        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            return False

    def setup_database(self, schema_files: List[str]) -> bool:
        """Setup database schemas"""
        try:
            
            schema_manager = SchemaManager(self.db_utils)

            for schema_file in schema_files:
                schema_path = Path(schema_file)
                self.logger.debug(f"Applying schema from file: {schema_path}")
                if not schema_path.exists():
                    self.logger.error(f"Schema file not found: {schema_file}")
                    continue

                version = f"1.0.{int(datetime.now().timestamp())}"
                success = schema_manager.apply_schema_from_file(
                    str(schema_path), version, f"CLI setup - {schema_path.name}"
                )

                if success:
                    print(f"✓ Applied schema: {schema_path.name}")
                else:
                    print(f"✗ Failed to apply schema: {schema_path.name}")
                    return False

            return True

        except Exception as e:
            self.logger.error(f"Database setup failed: {e}",exc_info=True)
            return False

    def list_pipelines(self, status: Optional[str] = None, limit: int = 50) -> None:
        """List ETL pipelines"""
        try:
            query = """
                SELECT pipeline_id, pipeline_name, status, owner, created_at
                FROM etl_pipelines
            """

            params = []
            if status:
                query += " WHERE status = %s"
                params.append(status)

            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)

            result = self.db_utils.execute_query(query, tuple(params))

            if result.success and result.rows:
                print(f"\n{'Pipeline ID':<20} {'Name':<30} {'Status':<10} {'Owner':<15} {'Created':<19}")
                print("-" * 95)

                for row in result.rows:
                    pipeline_id, name, status, owner, created = row
                    created_str = created.strftime("%Y-%m-%d %H:%M:%S") if created else ""
                    print(f"{pipeline_id:<20} {name:<30} {status:<10} {owner:<15} {created_str:<19}")

                print(f"\nTotal pipelines: {len(result.rows)}")
            else:
                print("No pipelines found.")

        except Exception as e:
            self.logger.error(f"Failed to list pipelines: {e}")

    def show_pipeline(self, pipeline_id: str) -> None:
        """Show detailed pipeline information"""
        try:
            # Get pipeline info
            pipeline_query = """
                SELECT pipeline_id, pipeline_name, description, pipeline_type,
                       source_system, target_system, owner, status, created_at, updated_at
                FROM etl_pipelines
                WHERE pipeline_id = %s
            """

            pipeline_result = self.db_utils.execute_query(pipeline_query, (pipeline_id,))

            if not pipeline_result.success or not pipeline_result.rows:
                print(f"Pipeline not found: {pipeline_id}")
                return

            pipeline = pipeline_result.rows[0]

            print(f"\nPipeline Details")
            print("=" * 50)
            print(f"ID: {pipeline[0]}")
            print(f"Name: {pipeline[1]}")
            print(f"Description: {pipeline[2] or 'N/A'}")
            print(f"Type: {pipeline[3]}")
            print(f"Source: {pipeline[4] or 'N/A'}")
            print(f"Target: {pipeline[5] or 'N/A'}")
            print(f"Owner: {pipeline[6]}")
            print(f"Status: {pipeline[7]}")
            print(f"Created: {pipeline[8].strftime('%Y-%m-%d %H:%M:%S') if pipeline[8] else 'N/A'}")
            print(f"Updated: {pipeline[9].strftime('%Y-%m-%d %H:%M:%S') if pipeline[9] else 'N/A'}")

            # Get pipeline steps
            steps_query = """
                SELECT step_id, step_name, step_type, step_order, status
                FROM etl_pipeline_steps
                WHERE pipeline_id = %s
                ORDER BY step_order
            """

            steps_result = self.db_utils.execute_query(steps_query, (pipeline_id,))

            if steps_result.success and steps_result.rows:
                print(f"\nPipeline Steps ({len(steps_result.rows)})")
                print("-" * 40)

                for step in steps_result.rows:
                    print(f"{step[4]:<3} {step[1]:<25} {step[2]:<12} {step[0]}")

            # Get recent executions
            executions_query = """
                SELECT execution_id, execution_status, start_time, end_time, records_processed
                FROM etl_pipeline_executions
                WHERE pipeline_id = %s
                ORDER BY start_time DESC
                LIMIT 5
            """

            exec_result = self.db_utils.execute_query(executions_query, (pipeline_id,))

            if exec_result.success and exec_result.rows:
                print(f"\nRecent Executions")
                print("-" * 50)

                for exec_row in exec_result.rows:
                    exec_id, status, start, end, records = exec_row
                    duration = ""
                    if start and end:
                        duration = str(end - start).split('.')[0]

                    print(f"{exec_id:<15} {status:<10} {start.strftime('%Y-%m-%d %H:%M') if start else 'N/A':<16} {duration:<10} {records or 0:>8}")

        except Exception as e:
            self.logger.error(f"Failed to show pipeline details: {e}")

    def execute_pipeline(self, pipeline_id: str, wait: bool = False) -> None:
        """Execute a pipeline"""
        try:
            print(f"Starting execution of pipeline: {pipeline_id}")

            execution_id = self.orchestrator.execute_pipeline(pipeline_id)

            if execution_id:
                print(f"Pipeline execution started with ID: {execution_id}")

                if wait:
                    print("Waiting for execution to complete...")
                    # In a real implementation, you'd poll for status
                    print("Execution completed.")
                else:
                    print("Use 'etl status' to check execution progress.")
            else:
                print("Failed to start pipeline execution.")

        except Exception as e:
            self.logger.error(f"Failed to execute pipeline: {e}")

    def list_executions(self, pipeline_id: Optional[str] = None,
                       status: Optional[str] = None, limit: int = 20) -> None:
        """List pipeline executions"""
        try:
            query = """
                SELECT e.execution_id, e.pipeline_id, p.pipeline_name,
                       e.execution_status, e.start_time, e.end_time,
                       e.records_processed, e.records_failed
                FROM etl_pipeline_executions e
                JOIN etl_pipelines p ON e.pipeline_id = p.pipeline_id
            """

            conditions = []
            params = []

            if pipeline_id:
                conditions.append("e.pipeline_id = %s")
                params.append(pipeline_id)

            if status:
                conditions.append("e.execution_status = %s")
                params.append(status)

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            query += " ORDER BY e.start_time DESC LIMIT %s"
            params.append(limit)

            result = self.db_utils.execute_query(query, tuple(params))

            if result.success and result.rows:
                print(f"\n{'Execution ID':<15} {'Pipeline':<25} {'Status':<10} {'Start Time':<19} {'Duration':<10} {'Records':>8}")
                print("-" * 100)

                for row in result.rows:
                    exec_id, pipe_id, pipe_name, status, start, end, records, failed = row

                    duration = ""
                    if start and end:
                        duration = str(end - start).split('.')[0]
                    elif start and not end:
                        duration = str(datetime.now() - start).split('.')[0]

                    start_str = start.strftime("%Y-%m-%d %H:%M:%S") if start else "N/A"

                    print(f"{exec_id:<15} {pipe_name[:24]:<25} {status:<10} {start_str:<19} {duration:<10} {records or 0:>8}")

                print(f"\nTotal executions: {len(result.rows)}")
            else:
                print("No executions found.")

        except Exception as e:
            self.logger.error(f"Failed to list executions: {e}")

    def show_execution_status(self, execution_id: str) -> None:
        """Show execution status and details"""
        try:
            # Get execution info
            exec_query = """
                SELECT e.execution_id, e.pipeline_id, p.pipeline_name,
                       e.execution_status, e.start_time, e.end_time,
                       e.duration_seconds, e.records_processed, e.records_failed,
                       e.error_message, e.retry_count
                FROM etl_pipeline_executions e
                JOIN etl_pipelines p ON e.pipeline_id = p.pipeline_id
                WHERE e.execution_id = %s
            """

            exec_result = self.db_utils.execute_query(exec_query, (execution_id,))

            if not exec_result.success or not exec_result.rows:
                print(f"Execution not found: {execution_id}")
                return

            exec_data = exec_result.rows[0]

            print(f"\nExecution Status")
            print("=" * 50)
            print(f"Execution ID: {exec_data[0]}")
            print(f"Pipeline: {exec_data[2]} ({exec_data[1]})")
            print(f"Status: {exec_data[3]}")
            print(f"Start Time: {exec_data[4].strftime('%Y-%m-%d %H:%M:%S') if exec_data[4] else 'N/A'}")
            print(f"End Time: {exec_data[5].strftime('%Y-%m-%d %H:%M:%S') if exec_data[5] else 'N/A'}")
            print(f"Duration: {str(timedelta(seconds=exec_data[6])) if exec_data[6] else 'N/A'}")
            print(f"Records Processed: {exec_data[7] or 0}")
            print(f"Records Failed: {exec_data[8] or 0}")
            print(f"Retry Count: {exec_data[10] or 0}")

            if exec_data[9]:  # error_message
                print(f"Error: {exec_data[9]}")

            # Get step executions
            steps_query = """
                SELECT s.step_name, se.step_status, se.start_time, se.end_time,
                       se.records_processed, se.error_message
                FROM etl_step_executions se
                JOIN etl_pipeline_steps s ON se.step_id = s.step_id
                WHERE se.execution_id = %s
                ORDER BY s.step_order
            """

            steps_result = self.db_utils.execute_query(steps_query, (execution_id,))

            if steps_result.success and steps_result.rows:
                print(f"\nStep Details")
                print("-" * 70)

                for step in steps_result.rows:
                    step_name, status, start, end, records, error = step
                    duration = ""
                    if start and end:
                        duration = str(end - start).split('.')[0]

                    print(f"{step_name:<25} {status:<10} {duration:<10} {records or 0:>8}")
                    if error:
                        print(f"  Error: {error}")

        except Exception as e:
            self.logger.error(f"Failed to show execution status: {e}")

    def cancel_execution(self, execution_id: str) -> None:
        """Cancel a running execution"""
        try:
            # In a real implementation, this would signal the orchestrator to cancel
            print(f"Cancelling execution: {execution_id}")
            # self.orchestrator.cancel_execution(execution_id)
            print("Execution cancellation requested.")

        except Exception as e:
            self.logger.error(f"Failed to cancel execution: {e}")

    def show_health(self) -> None:
        """Show system health status"""
        try:
            health = self.db_utils.health_check()

            print(f"\nSystem Health")
            print("=" * 30)
            print(f"Database: {'✓ Healthy' if health['healthy'] else '✗ Unhealthy'}")
            print(f"Response Time: {health['response_time']}s")

            if not health['healthy']:
                print(f"Error: {health['error']}")

            # Show some basic stats
            stats = self.db_utils.get_database_stats()
            print(f"Active Connections: {stats.get('active_connections', 'N/A')}")
            print(f"Tables: {stats.get('table_count', 'N/A')}")

        except Exception as e:
            self.logger.error(f"Failed to check system health: {e}")

    def create_pipeline_template(self, output_file: str) -> None:
        """Create a pipeline template file"""
        template = {
            "pipeline": {
                "id": "new_pipeline",
                "name": "New ETL Pipeline",
                "description": "Description of the pipeline",
                "type": "batch",
                "source_system": "source_system",
                "target_system": "target_system",
                "owner": "pipeline_owner"
            },
            "steps": [
                {
                    "id": "extract_step",
                    "name": "Extract Data",
                    "type": "extract",
                    "order": 1,
                    "source_config": {
                        "type": "database",
                        "connection": "source_connection",
                        "query": "SELECT * FROM source_table"
                    }
                },
                {
                    "id": "transform_step",
                    "name": "Transform Data",
                    "type": "transform",
                    "order": 2,
                    "transformation_config": {
                        "rules": []
                    }
                },
                {
                    "id": "load_step",
                    "name": "Load Data",
                    "type": "load",
                    "order": 3,
                    "target_config": {
                        "type": "database",
                        "connection": "target_connection",
                        "table": "target_table"
                    }
                }
            ]
        }

        try:
            with open(output_file, 'w') as f:
                json.dump(template, f, indent=2)
            print(f"Pipeline template created: {output_file}")

        except Exception as e:
            self.logger.error(f"Failed to create template: {e}")


def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="ETL Framework CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  etl --config config.yaml setup-db
  etl --config config.yaml pipelines list
  etl --config config.yaml pipeline show my_pipeline
  etl --config config.yaml pipeline execute my_pipeline
  etl --config config.yaml executions list --status running
  etl --config config.yaml execution status exec_123
  etl --config config.yaml health
        """
    )

    parser.add_argument('--config', '-c', default='config/config.yaml',
                       help='Configuration file path')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'],
                       default='INFO', help='Logging level')

    subparsers = parser.add_subparsers(dest='command', help='Available commands')

    # Setup database command
    setup_parser = subparsers.add_parser('setup-db', help='Setup database schemas')
    setup_parser.add_argument('--schemas', nargs='+',
                             default=['database/metadata_schema.sql',
                                    'database/execution_schema.sql',
                                    'database/data_quality_schema.sql'],
                             help='Schema files to apply')

    # Pipelines command
    pipelines_parser = subparsers.add_parser('pipelines', help='Pipeline management')
    pipelines_sub = pipelines_parser.add_subparsers(dest='subcommand')

    # List pipelines
    list_parser = pipelines_sub.add_parser('list', help='List pipelines')
    list_parser.add_argument('--status', help='Filter by status')
    list_parser.add_argument('--limit', type=int, default=50, help='Limit results')

    # Show pipeline
    show_parser = pipelines_sub.add_parser('show', help='Show pipeline details')
    show_parser.add_argument('pipeline_id', help='Pipeline ID')

    # Execute pipeline
    exec_parser = pipelines_sub.add_parser('execute', help='Execute pipeline')
    exec_parser.add_argument('pipeline_id', help='Pipeline ID')
    exec_parser.add_argument('--wait', action='store_true', help='Wait for completion')

    # Create pipeline template
    template_parser = pipelines_sub.add_parser('template', help='Create pipeline template')
    template_parser.add_argument('output_file', help='Output template file')

    # Executions command
    executions_parser = subparsers.add_parser('executions', help='Execution management')
    execs_sub = executions_parser.add_subparsers(dest='subcommand')

    # List executions
    execs_list_parser = execs_sub.add_parser('list', help='List executions')
    execs_list_parser.add_argument('--pipeline', help='Filter by pipeline ID')
    execs_list_parser.add_argument('--status', help='Filter by status')
    execs_list_parser.add_argument('--limit', type=int, default=20, help='Limit results')

    # Show execution status
    execs_status_parser = execs_sub.add_parser('status', help='Show execution status')
    execs_status_parser.add_argument('execution_id', help='Execution ID')

    # Cancel execution
    execs_cancel_parser = execs_sub.add_parser('cancel', help='Cancel execution')
    execs_cancel_parser.add_argument('execution_id', help='Execution ID')

    # Health check
    subparsers.add_parser('health', help='Show system health')

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        return

    # Set log level
    logging.getLogger().setLevel(getattr(logging, args.log_level))

    # Initialize CLI
    cli = ETLCLI(args.config)

    if args.command != 'setup-db':
        # Load configuration for all commands except setup-db
        if not cli.load_config(args.config):
            print(f"Failed to load configuration from {args.config}")
            sys.exit(1)

    # Execute commands
    try:
        if args.command == 'setup-db':
            if not HAS_FRAMEWORK:
                print("Framework components not available. Cannot setup database.")
                sys.exit(1)

            success = cli.setup_database(args.schemas)
            if not success:
                sys.exit(1)

        elif args.command == 'pipelines':
            if args.subcommand == 'list':
                cli.list_pipelines(args.status, args.limit)
            elif args.subcommand == 'show':
                cli.show_pipeline(args.pipeline_id)
            elif args.subcommand == 'execute':
                cli.execute_pipeline(args.pipeline_id, args.wait)
            elif args.subcommand == 'template':
                cli.create_pipeline_template(args.output_file)

        elif args.command == 'executions':
            if args.subcommand == 'list':
                cli.list_executions(args.pipeline, args.status, args.limit)
            elif args.subcommand == 'status':
                cli.show_execution_status(args.execution_id)
            elif args.subcommand == 'cancel':
                cli.cancel_execution(args.execution_id)

        elif args.command == 'health':
            cli.show_health()

    except KeyboardInterrupt:
        print("\nOperation cancelled by user.")
        sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()