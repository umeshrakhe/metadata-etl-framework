"""
ETL Framework REST API

RESTful API for programmatic access to ETL framework functionality.
Provides endpoints for pipeline management, execution monitoring, and system control.
"""

import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from pathlib import Path
import sys

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent / "src"))

try:
    from flask import Flask, request, jsonify, Response
    from flask_cors import CORS
    HAS_FLASK = True
except ImportError:
    HAS_FLASK = False
    Flask = None
    CORS = None

try:
    from fastapi import FastAPI, HTTPException, Query, BackgroundTasks
    from fastapi.middleware.cors import CORSMiddleware
    from pydantic import BaseModel, Field
    HAS_FASTAPI = True
except ImportError:
    HAS_FASTAPI = False
    FastAPI = None

try:
    from utils.database_utils import DatabaseUtils, ConnectionConfig, DatabaseType
    from utils.schema_manager import SchemaManager
    from orchestrator.orchestrator_manager import OrchestratorManager
    from orchestrator.config_loader import ConfigLoader
    HAS_FRAMEWORK = True
except ImportError:
    HAS_FRAMEWORK = False


# Pydantic models for FastAPI
if HAS_FASTAPI:
    class PipelineCreate(BaseModel):
        pipeline_id: str = Field(..., description="Unique pipeline identifier")
        pipeline_name: str = Field(..., description="Human-readable pipeline name")
        description: Optional[str] = None
        pipeline_type: str = Field("batch", description="Pipeline type: batch, streaming, incremental")
        source_system: Optional[str] = None
        target_system: Optional[str] = None
        owner: str = Field(..., description="Pipeline owner")
        priority: int = Field(1, description="Execution priority (1-5)")

    class PipelineUpdate(BaseModel):
        pipeline_name: Optional[str] = None
        description: Optional[str] = None
        status: Optional[str] = None
        priority: Optional[int] = None

    class ExecutionRequest(BaseModel):
        pipeline_id: str = Field(..., description="Pipeline to execute")
        parameters: Optional[Dict[str, Any]] = Field(default_factory=dict, description="Execution parameters")
        priority: int = Field(1, description="Execution priority")


class ETLAPI:
    """ETL Framework REST API"""

    def __init__(self, config_file: str = "config/config.yaml"):
        self.config_file = config_file
        self.config_loader = None
        self.db_utils = None
        self.orchestrator = None
        self.logger = logging.getLogger(__name__)

        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        self.load_config()

    def load_config(self) -> bool:
        """Load configuration and initialize components"""
        try:
            self.config_loader = ConfigLoader(self.config_file)
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
            self.orchestrator = OrchestratorManager(self.db_utils, config)

            return True

        except Exception as e:
            self.logger.error(f"Failed to load configuration: {e}")
            return False

    def get_pipelines(self, status: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
        """Get list of pipelines"""
        try:
            query = """
                SELECT pipeline_id, pipeline_name, description, pipeline_type,
                       source_system, target_system, owner, status, priority,
                       created_at, updated_at
                FROM etl_pipelines
            """

            params = []
            if status:
                query += " WHERE status = %s"
                params.append(status)

            query += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)

            result = self.db_utils.execute_query(query, tuple(params))

            if result.success:
                pipelines = []
                for row in result.rows:
                    pipeline = {
                        'pipeline_id': row[0],
                        'pipeline_name': row[1],
                        'description': row[2],
                        'pipeline_type': row[3],
                        'source_system': row[4],
                        'target_system': row[5],
                        'owner': row[6],
                        'status': row[7],
                        'priority': row[8],
                        'created_at': row[9].isoformat() if row[9] else None,
                        'updated_at': row[10].isoformat() if row[10] else None
                    }
                    pipelines.append(pipeline)

                return {'pipelines': pipelines, 'count': len(pipelines)}

            return {'error': 'Failed to retrieve pipelines'}

        except Exception as e:
            self.logger.error(f"Failed to get pipelines: {e}")
            return {'error': str(e)}

    def get_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Get detailed pipeline information"""
        try:
            # Get pipeline info
            pipeline_query = """
                SELECT pipeline_id, pipeline_name, description, pipeline_type,
                       source_system, target_system, owner, status, priority,
                       schedule_config, retry_config, notification_config,
                       created_at, updated_at
                FROM etl_pipelines
                WHERE pipeline_id = %s
            """

            pipeline_result = self.db_utils.execute_query(pipeline_query, (pipeline_id,))

            if not pipeline_result.success or not pipeline_result.rows:
                return {'error': 'Pipeline not found'}

            row = pipeline_result.rows[0]
            pipeline = {
                'pipeline_id': row[0],
                'pipeline_name': row[1],
                'description': row[2],
                'pipeline_type': row[3],
                'source_system': row[4],
                'target_system': row[5],
                'owner': row[6],
                'status': row[7],
                'priority': row[8],
                'schedule_config': row[9],
                'retry_config': row[10],
                'notification_config': row[11],
                'created_at': row[12].isoformat() if row[12] else None,
                'updated_at': row[13].isoformat() if row[13] else None
            }

            # Get pipeline steps
            steps_query = """
                SELECT step_id, step_name, step_type, step_order,
                       source_config, target_config, transformation_config,
                       validation_config, timeout_seconds, status
                FROM etl_pipeline_steps
                WHERE pipeline_id = %s
                ORDER BY step_order
            """

            steps_result = self.db_utils.execute_query(steps_query, (pipeline_id,))

            if steps_result.success:
                steps = []
                for step_row in steps_result.rows:
                    step = {
                        'step_id': step_row[0],
                        'step_name': step_row[1],
                        'step_type': step_row[2],
                        'step_order': step_row[3],
                        'source_config': step_row[4],
                        'target_config': step_row[5],
                        'transformation_config': step_row[6],
                        'validation_config': step_row[7],
                        'timeout_seconds': step_row[8],
                        'status': step_row[9]
                    }
                    steps.append(step)

                pipeline['steps'] = steps

            return pipeline

        except Exception as e:
            self.logger.error(f"Failed to get pipeline: {e}")
            return {'error': str(e)}

    def create_pipeline(self, pipeline_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a new pipeline"""
        try:
            # Insert pipeline
            insert_query = """
                INSERT INTO etl_pipelines
                (pipeline_id, pipeline_name, description, pipeline_type,
                 source_system, target_system, owner, priority)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """

            self.db_utils.execute_query(insert_query, (
                pipeline_data['pipeline_id'],
                pipeline_data['pipeline_name'],
                pipeline_data.get('description'),
                pipeline_data.get('pipeline_type', 'batch'),
                pipeline_data.get('source_system'),
                pipeline_data.get('target_system'),
                pipeline_data['owner'],
                pipeline_data.get('priority', 1)
            ))

            return {'message': 'Pipeline created successfully', 'pipeline_id': pipeline_data['pipeline_id']}

        except Exception as e:
            self.logger.error(f"Failed to create pipeline: {e}")
            return {'error': str(e)}

    def update_pipeline(self, pipeline_id: str, update_data: Dict[str, Any]) -> Dict[str, Any]:
        """Update pipeline information"""
        try:
            # Build dynamic update query
            update_fields = []
            params = []

            for field in ['pipeline_name', 'description', 'status', 'priority']:
                if field in update_data:
                    update_fields.append(f"{field} = %s")
                    params.append(update_data[field])

            if not update_fields:
                return {'error': 'No valid fields to update'}

            update_query = f"""
                UPDATE etl_pipelines
                SET {', '.join(update_fields)}, updated_at = CURRENT_TIMESTAMP
                WHERE pipeline_id = %s
            """
            params.append(pipeline_id)

            result = self.db_utils.execute_query(update_query, tuple(params))

            if result.success:
                return {'message': 'Pipeline updated successfully'}
            else:
                return {'error': 'Failed to update pipeline'}

        except Exception as e:
            self.logger.error(f"Failed to update pipeline: {e}")
            return {'error': str(e)}

    def delete_pipeline(self, pipeline_id: str) -> Dict[str, Any]:
        """Delete a pipeline"""
        try:
            delete_query = "DELETE FROM etl_pipelines WHERE pipeline_id = %s"
            result = self.db_utils.execute_query(delete_query, (pipeline_id,))

            if result.success and result.row_count > 0:
                return {'message': 'Pipeline deleted successfully'}
            else:
                return {'error': 'Pipeline not found or could not be deleted'}

        except Exception as e:
            self.logger.error(f"Failed to delete pipeline: {e}")
            return {'error': str(e)}

    def execute_pipeline(self, execution_request: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a pipeline"""
        try:
            pipeline_id = execution_request['pipeline_id']
            execution_id = self.orchestrator.execute_pipeline(pipeline_id)

            if execution_id:
                return {
                    'message': 'Pipeline execution started',
                    'execution_id': execution_id,
                    'pipeline_id': pipeline_id
                }
            else:
                return {'error': 'Failed to start pipeline execution'}

        except Exception as e:
            self.logger.error(f"Failed to execute pipeline: {e}")
            return {'error': str(e)}

    def get_executions(self, pipeline_id: Optional[str] = None,
                      status: Optional[str] = None, limit: int = 20) -> Dict[str, Any]:
        """Get list of executions"""
        try:
            query = """
                SELECT e.execution_id, e.pipeline_id, p.pipeline_name,
                       e.execution_status, e.start_time, e.end_time,
                       e.duration_seconds, e.records_processed, e.records_failed
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

            if result.success:
                executions = []
                for row in result.rows:
                    execution = {
                        'execution_id': row[0],
                        'pipeline_id': row[1],
                        'pipeline_name': row[2],
                        'status': row[3],
                        'start_time': row[4].isoformat() if row[4] else None,
                        'end_time': row[5].isoformat() if row[5] else None,
                        'duration_seconds': row[6],
                        'records_processed': row[7],
                        'records_failed': row[8]
                    }
                    executions.append(execution)

                return {'executions': executions, 'count': len(executions)}

            return {'error': 'Failed to retrieve executions'}

        except Exception as e:
            self.logger.error(f"Failed to get executions: {e}")
            return {'error': str(e)}

    def get_execution_status(self, execution_id: str) -> Dict[str, Any]:
        """Get detailed execution status"""
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
                return {'error': 'Execution not found'}

            row = exec_result.rows[0]
            execution = {
                'execution_id': row[0],
                'pipeline_id': row[1],
                'pipeline_name': row[2],
                'status': row[3],
                'start_time': row[4].isoformat() if row[4] else None,
                'end_time': row[5].isoformat() if row[5] else None,
                'duration_seconds': row[6],
                'records_processed': row[7],
                'records_failed': row[8],
                'error_message': row[9],
                'retry_count': row[10]
            }

            # Get step executions
            steps_query = """
                SELECT s.step_id, s.step_name, se.step_status,
                       se.start_time, se.end_time, se.duration_seconds,
                       se.records_processed, se.records_failed, se.error_message
                FROM etl_step_executions se
                JOIN etl_pipeline_steps s ON se.step_id = s.step_id
                WHERE se.execution_id = %s
                ORDER BY s.step_order
            """

            steps_result = self.db_utils.execute_query(steps_query, (execution_id,))

            if steps_result.success:
                steps = []
                for step_row in steps_result.rows:
                    step = {
                        'step_id': step_row[0],
                        'step_name': step_row[1],
                        'status': step_row[2],
                        'start_time': step_row[3].isoformat() if step_row[3] else None,
                        'end_time': step_row[4].isoformat() if step_row[4] else None,
                        'duration_seconds': step_row[5],
                        'records_processed': step_row[6],
                        'records_failed': step_row[7],
                        'error_message': step_row[8]
                    }
                    steps.append(step)

                execution['steps'] = steps

            return execution

        except Exception as e:
            self.logger.error(f"Failed to get execution status: {e}")
            return {'error': str(e)}

    def cancel_execution(self, execution_id: str) -> Dict[str, Any]:
        """Cancel a running execution"""
        try:
            # In a real implementation, this would signal the orchestrator
            return {'message': f'Execution {execution_id} cancellation requested'}

        except Exception as e:
            self.logger.error(f"Failed to cancel execution: {e}")
            return {'error': str(e)}

    def get_system_health(self) -> Dict[str, Any]:
        """Get system health status"""
        try:
            health = self.db_utils.health_check()
            stats = self.db_utils.get_database_stats()

            return {
                'status': 'healthy' if health['healthy'] else 'unhealthy',
                'database': {
                    'healthy': health['healthy'],
                    'response_time': health['response_time'],
                    'error': health.get('error')
                },
                'stats': stats,
                'timestamp': datetime.now().isoformat()
            }

        except Exception as e:
            self.logger.error(f"Failed to get system health: {e}")
            return {'error': str(e)}

    def get_metrics(self, metric_type: Optional[str] = None,
                   hours: int = 24) -> Dict[str, Any]:
        """Get system metrics"""
        try:
            # Get recent metrics
            query = """
                SELECT metric_type, metric_name, metric_value, metric_unit,
                       collection_time, execution_id
                FROM etl_execution_metrics
                WHERE collection_time >= %s
            """

            params = [datetime.now() - timedelta(hours=hours)]

            if metric_type:
                query += " AND metric_type = %s"
                params.append(metric_type)

            query += " ORDER BY collection_time DESC LIMIT 1000"

            result = self.db_utils.execute_query(query, tuple(params))

            if result.success:
                metrics = []
                for row in result.rows:
                    metric = {
                        'type': row[0],
                        'name': row[1],
                        'value': float(row[2]) if row[2] else None,
                        'unit': row[3],
                        'timestamp': row[4].isoformat() if row[4] else None,
                        'execution_id': row[5]
                    }
                    metrics.append(metric)

                return {'metrics': metrics, 'count': len(metrics)}

            return {'error': 'Failed to retrieve metrics'}

        except Exception as e:
            self.logger.error(f"Failed to get metrics: {e}")
            return {'error': str(e)}


# Flask API Implementation
def create_flask_app(api: ETLAPI) -> Flask:
    """Create Flask application"""
    app = Flask(__name__)

    # Enable CORS
    if CORS:
        CORS(app)

    @app.route('/health', methods=['GET'])
    def health():
        return jsonify(api.get_system_health())

    @app.route('/pipelines', methods=['GET'])
    def get_pipelines():
        status = request.args.get('status')
        limit = int(request.args.get('limit', 50))
        return jsonify(api.get_pipelines(status, limit))

    @app.route('/pipelines/<pipeline_id>', methods=['GET'])
    def get_pipeline(pipeline_id):
        return jsonify(api.get_pipeline(pipeline_id))

    @app.route('/pipelines', methods=['POST'])
    def create_pipeline():
        data = request.get_json()
        return jsonify(api.create_pipeline(data))

    @app.route('/pipelines/<pipeline_id>', methods=['PUT'])
    def update_pipeline(pipeline_id):
        data = request.get_json()
        return jsonify(api.update_pipeline(pipeline_id, data))

    @app.route('/pipelines/<pipeline_id>', methods=['DELETE'])
    def delete_pipeline(pipeline_id):
        return jsonify(api.delete_pipeline(pipeline_id))

    @app.route('/executions', methods=['POST'])
    def execute_pipeline():
        data = request.get_json()
        return jsonify(api.execute_pipeline(data))

    @app.route('/executions', methods=['GET'])
    def get_executions():
        pipeline_id = request.args.get('pipeline_id')
        status = request.args.get('status')
        limit = int(request.args.get('limit', 20))
        return jsonify(api.get_executions(pipeline_id, status, limit))

    @app.route('/executions/<execution_id>', methods=['GET'])
    def get_execution_status(execution_id):
        return jsonify(api.get_execution_status(execution_id))

    @app.route('/executions/<execution_id>/cancel', methods=['POST'])
    def cancel_execution(execution_id):
        return jsonify(api.cancel_execution(execution_id))

    @app.route('/metrics', methods=['GET'])
    def get_metrics():
        metric_type = request.args.get('type')
        hours = int(request.args.get('hours', 24))
        return jsonify(api.get_metrics(metric_type, hours))

    return app


# FastAPI Implementation
def create_fastapi_app(api: ETLAPI) -> FastAPI:
    """Create FastAPI application"""
    app = FastAPI(
        title="ETL Framework API",
        description="REST API for ETL Framework management",
        version="1.0.0"
    )

    # Enable CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/health")
    async def health():
        return api.get_system_health()

    @app.get("/pipelines")
    async def get_pipelines(status: Optional[str] = None, limit: int = Query(50, ge=1, le=1000)):
        return api.get_pipelines(status, limit)

    @app.get("/pipelines/{pipeline_id}")
    async def get_pipeline(pipeline_id: str):
        result = api.get_pipeline(pipeline_id)
        if 'error' in result:
            raise HTTPException(status_code=404, detail=result['error'])
        return result

    @app.post("/pipelines")
    async def create_pipeline(pipeline: PipelineCreate):
        result = api.create_pipeline(pipeline.dict())
        if 'error' in result:
            raise HTTPException(status_code=400, detail=result['error'])
        return result

    @app.put("/pipelines/{pipeline_id}")
    async def update_pipeline(pipeline_id: str, update: PipelineUpdate):
        result = api.update_pipeline(pipeline_id, update.dict(exclude_unset=True))
        if 'error' in result:
            raise HTTPException(status_code=400, detail=result['error'])
        return result

    @app.delete("/pipelines/{pipeline_id}")
    async def delete_pipeline(pipeline_id: str):
        result = api.delete_pipeline(pipeline_id)
        if 'error' in result:
            raise HTTPException(status_code=404, detail=result['error'])
        return result

    @app.post("/executions")
    async def execute_pipeline(request: ExecutionRequest):
        result = api.execute_pipeline(request.dict())
        if 'error' in result:
            raise HTTPException(status_code=400, detail=result['error'])
        return result

    @app.get("/executions")
    async def get_executions(pipeline_id: Optional[str] = None,
                           status: Optional[str] = None,
                           limit: int = Query(20, ge=1, le=1000)):
        return api.get_executions(pipeline_id, status, limit)

    @app.get("/executions/{execution_id}")
    async def get_execution_status(execution_id: str):
        result = api.get_execution_status(execution_id)
        if 'error' in result:
            raise HTTPException(status_code=404, detail=result['error'])
        return result

    @app.post("/executions/{execution_id}/cancel")
    async def cancel_execution(execution_id: str):
        return api.cancel_execution(execution_id)

    @app.get("/metrics")
    async def get_metrics(metric_type: Optional[str] = None,
                         hours: int = Query(24, ge=1, le=168)):
        return api.get_metrics(metric_type, hours)

    return app


def main():
    """Main API entry point"""
    import argparse

    parser = argparse.ArgumentParser(description="ETL Framework REST API")
    parser.add_argument('--config', '-c', default='config/config.yaml',
                       help='Configuration file path')
    parser.add_argument('--framework', choices=['flask', 'fastapi'],
                       default='flask', help='Web framework to use')
    parser.add_argument('--host', default='0.0.0.0', help='Host to bind to')
    parser.add_argument('--port', type=int, default=8000, help='Port to bind to')
    parser.add_argument('--workers', type=int, default=1, help='Number of workers (FastAPI)')

    args = parser.parse_args()

    if not HAS_FRAMEWORK:
        print("ETL Framework components not available.")
        return

    # Initialize API
    api = ETLAPI(args.config)

    if args.framework == 'flask':
        if not HAS_FLASK:
            print("Flask not available. Install with: pip install flask flask-cors")
            return

        app = create_flask_app(api)
        print(f"Starting Flask API on {args.host}:{args.port}")
        app.run(host=args.host, port=args.port, debug=True)

    elif args.framework == 'fastapi':
        if not HAS_FASTAPI:
            print("FastAPI not available. Install with: pip install fastapi uvicorn pydantic")
            return

        import uvicorn
        app = create_fastapi_app(api)
        print(f"Starting FastAPI on {args.host}:{args.port}")
        uvicorn.run(app, host=args.host, port=args.port, workers=args.workers)


if __name__ == '__main__':
    main()