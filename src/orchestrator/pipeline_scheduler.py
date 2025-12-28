import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, asdict
import uuid
import re
import threading
import time

# Optional imports
try:
    from croniter import croniter
    CRONITER_AVAILABLE = True
except ImportError:
    croniter = None
    CRONITER_AVAILABLE = False

logger = logging.getLogger("PipelineScheduler")


@dataclass
class ScheduleConfig:
    """Configuration for pipeline scheduling."""
    pipeline_id: str
    cron_expression: Optional[str] = None
    schedule_type: str = "time_based"  # time_based, event_based, dependency_based, conditional
    is_active: bool = True
    concurrent_policy: str = "allow"  # allow, skip, queue, fail
    max_concurrent_runs: int = 1
    retry_count: int = 3
    retry_delay_minutes: int = 5
    execution_timeout_hours: int = 24
    execution_window_start: Optional[str] = None  # HH:MM format
    execution_window_end: Optional[str] = None    # HH:MM format
    dependencies: List[str] = None  # upstream pipeline IDs
    event_triggers: List[str] = None  # event types to listen for
    conditions: Dict[str, Any] = None  # conditional execution criteria
    created_at: datetime = None
    updated_at: datetime = None

    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.event_triggers is None:
            self.event_triggers = []
        if self.conditions is None:
            self.conditions = {}
        if self.created_at is None:
            self.created_at = datetime.utcnow()
        if self.updated_at is None:
            self.updated_at = datetime.utcnow()


@dataclass
class ExecutionTrigger:
    """Information about what triggered a pipeline execution."""
    trigger_type: str  # scheduled, manual, event, dependency, conditional
    trigger_id: str
    triggered_by: Optional[str] = None  # user_id for manual triggers
    event_data: Optional[Dict[str, Any]] = None
    upstream_run_id: Optional[str] = None
    condition_results: Optional[Dict[str, Any]] = None
    triggered_at: datetime = None

    def __post_init__(self):
        if self.event_data is None:
            self.event_data = {}
        if self.condition_results is None:
            self.condition_results = {}
        if self.triggered_at is None:
            self.triggered_at = datetime.utcnow()


@dataclass
class ScheduledExecution:
    """Scheduled execution record."""
    execution_id: str
    pipeline_id: str
    scheduled_time: datetime
    actual_start_time: Optional[datetime] = None
    status: str = "scheduled"  # scheduled, running, completed, failed, skipped, cancelled
    trigger_info: ExecutionTrigger = None
    retry_count: int = 0
    error_message: Optional[str] = None
    created_at: datetime = None

    def __post_init__(self):
        if self.created_at is None:
            self.created_at = datetime.utcnow()


class PipelineScheduler:
    """
    Comprehensive pipeline scheduling system supporting multiple scheduling patterns
    and integration with external schedulers.
    """

    def __init__(self, db_connection: Any = None, orchestrator_manager: Any = None):
        self.db = db_connection
        self.orchestrator = orchestrator_manager
        self.logger = logging.getLogger(self.__class__.__name__)
        self.active_schedules: Dict[str, ScheduleConfig] = {}
        self.scheduler_thread: Optional[threading.Thread] = None
        self.running = False
        self.execution_queue: List[ScheduledExecution] = []
        self.event_listeners: Dict[str, List[str]] = {}  # event_type -> [pipeline_ids]

        if not CRONITER_AVAILABLE:
            self.logger.warning("croniter not available - cron scheduling will be limited")

    def start_scheduler(self) -> None:
        """Start the scheduler daemon thread."""
        if self.scheduler_thread and self.scheduler_thread.is_alive():
            self.logger.warning("Scheduler already running")
            return

        self.running = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop, daemon=True)
        self.scheduler_thread.start()
        self.logger.info("Pipeline scheduler started")

    def stop_scheduler(self) -> None:
        """Stop the scheduler daemon thread."""
        self.running = False
        if self.scheduler_thread:
            self.scheduler_thread.join(timeout=5.0)
        self.logger.info("Pipeline scheduler stopped")

    def schedule_pipeline(self, pipeline_id: str, cron_expression: str,
                         schedule_type: str = "time_based", **kwargs) -> str:
        """Create a schedule for a pipeline."""
        if not self.validate_cron_expression(cron_expression):
            raise ValueError(f"Invalid cron expression: {cron_expression}")

        schedule_config = ScheduleConfig(
            pipeline_id=pipeline_id,
            cron_expression=cron_expression,
            schedule_type=schedule_type,
            **kwargs
        )

        # Store in database
        self._store_schedule_config(schedule_config)

        # Add to active schedules
        self.active_schedules[pipeline_id] = schedule_config

        # Register event listeners if event-based
        if schedule_type == "event_based" and schedule_config.event_triggers:
            for event_type in schedule_config.event_triggers:
                if event_type not in self.event_listeners:
                    self.event_listeners[event_type] = []
                if pipeline_id not in self.event_listeners[event_type]:
                    self.event_listeners[event_type].append(pipeline_id)

        self.logger.info(f"Scheduled pipeline {pipeline_id} with cron: {cron_expression}")
        return schedule_config.pipeline_id

    def enable_schedule(self, pipeline_id: str) -> None:
        """Activate a pipeline schedule."""
        if pipeline_id not in self.active_schedules:
            raise ValueError(f"No schedule found for pipeline {pipeline_id}")

        self.active_schedules[pipeline_id].is_active = True
        self.active_schedules[pipeline_id].updated_at = datetime.utcnow()
        self._update_schedule_config(self.active_schedules[pipeline_id])

        self.logger.info(f"Enabled schedule for pipeline {pipeline_id}")

    def disable_schedule(self, pipeline_id: str) -> None:
        """Deactivate a pipeline schedule."""
        if pipeline_id not in self.active_schedules:
            raise ValueError(f"No schedule found for pipeline {pipeline_id}")

        self.active_schedules[pipeline_id].is_active = False
        self.active_schedules[pipeline_id].updated_at = datetime.utcnow()
        self._update_schedule_config(self.active_schedules[pipeline_id])

        self.logger.info(f"Disabled schedule for pipeline {pipeline_id}")

    def pause_schedule(self, pipeline_id: str, duration_minutes: int) -> None:
        """Temporarily pause a pipeline schedule."""
        if pipeline_id not in self.active_schedules:
            raise ValueError(f"No schedule found for pipeline {pipeline_id}")

        # Set a pause flag and schedule reactivation
        schedule = self.active_schedules[pipeline_id]
        schedule.is_active = False
        schedule.updated_at = datetime.utcnow()

        # Schedule reactivation after duration
        def reactivate():
            time.sleep(duration_minutes * 60)
            if pipeline_id in self.active_schedules:
                self.enable_schedule(pipeline_id)

        reactivation_thread = threading.Thread(target=reactivate, daemon=True)
        reactivation_thread.start()

        self._update_schedule_config(schedule)
        self.logger.info(f"Paused schedule for pipeline {pipeline_id} for {duration_minutes} minutes")

    def trigger_manual_execution(self, pipeline_id: str, user_id: str) -> str:
        """Trigger manual pipeline execution."""
        trigger = ExecutionTrigger(
            trigger_type="manual",
            trigger_id=str(uuid.uuid4()),
            triggered_by=user_id
        )

        execution_id = self._create_scheduled_execution(pipeline_id, trigger)
        self._execute_pipeline(pipeline_id, execution_id, trigger)

        self.logger.info(f"Manual execution triggered for pipeline {pipeline_id} by user {user_id}")
        return execution_id

    def trigger_on_event(self, pipeline_id: str, event_type: str, event_data: Dict[str, Any]) -> Optional[str]:
        """Trigger pipeline execution based on event."""
        if pipeline_id not in self.active_schedules:
            return None

        schedule = self.active_schedules[pipeline_id]
        if not schedule.is_active or schedule.schedule_type != "event_based":
            return None

        if event_type not in schedule.event_triggers:
            return None

        trigger = ExecutionTrigger(
            trigger_type="event",
            trigger_id=str(uuid.uuid4()),
            event_data=event_data
        )

        execution_id = self._create_scheduled_execution(pipeline_id, trigger)
        self._execute_pipeline(pipeline_id, execution_id, trigger)

        self.logger.info(f"Event-triggered execution for pipeline {pipeline_id}: {event_type}")
        return execution_id

    def trigger_on_dependency(self, pipeline_id: str, upstream_run_id: str) -> Optional[str]:
        """Trigger pipeline execution after upstream completion."""
        if pipeline_id not in self.active_schedules:
            return None

        schedule = self.active_schedules[pipeline_id]
        if not schedule.is_active or schedule.schedule_type != "dependency_based":
            return None

        # Check if upstream pipeline is in dependencies
        if not self._check_dependency_completion(pipeline_id, upstream_run_id):
            return None

        trigger = ExecutionTrigger(
            trigger_type="dependency",
            trigger_id=str(uuid.uuid4()),
            upstream_run_id=upstream_run_id
        )

        execution_id = self._create_scheduled_execution(pipeline_id, trigger)
        self._execute_pipeline(pipeline_id, execution_id, trigger)

        self.logger.info(f"Dependency-triggered execution for pipeline {pipeline_id} after {upstream_run_id}")
        return execution_id

    def get_next_execution_time(self, pipeline_id: str) -> Optional[datetime]:
        """Calculate next execution time for a pipeline."""
        if pipeline_id not in self.active_schedules:
            return None

        schedule = self.active_schedules[pipeline_id]
        if not schedule.is_active or not schedule.cron_expression:
            return None

        if not CRONITER_AVAILABLE:
            self.logger.warning("croniter not available - cannot calculate next execution time")
            return None

        try:
            cron = croniter(schedule.cron_expression, datetime.utcnow())
            next_time = cron.get_next(datetime)

            # Check execution window
            if schedule.execution_window_start and schedule.execution_window_end:
                next_time = self._adjust_for_execution_window(next_time, schedule)

            return next_time

        except Exception as e:
            self.logger.exception(f"Failed to calculate next execution time: {e}")
            return None

    def check_concurrent_policy(self, pipeline_id: str) -> bool:
        """Check if pipeline can execute based on concurrent policy."""
        if pipeline_id not in self.active_schedules:
            return True

        schedule = self.active_schedules[pipeline_id]

        # Get currently running executions
        running_count = self._get_running_execution_count(pipeline_id)

        if schedule.concurrent_policy == "allow":
            return running_count < schedule.max_concurrent_runs
        elif schedule.concurrent_policy == "skip":
            return running_count == 0
        elif schedule.concurrent_policy == "queue":
            return True  # Will be queued
        elif schedule.concurrent_policy == "fail":
            return running_count == 0
        else:
            return True

    def get_execution_history(self, pipeline_id: str, days: int = 30) -> List[Dict[str, Any]]:
        """Get execution history for a pipeline."""
        if not self.db:
            return []

        try:
            cutoff_date = datetime.utcnow() - timedelta(days=days)

            sql = """
                SELECT se.execution_id, se.scheduled_time, se.actual_start_time,
                       se.status, se.retry_count, se.error_message, se.created_at,
                       se.trigger_info
                FROM SCHEDULED_EXECUTIONS se
                WHERE se.pipeline_id = %s AND se.created_at >= %s
                ORDER BY se.created_at DESC
            """

            self.db.execute(sql, (pipeline_id, cutoff_date))
            rows = self.db.fetchall()

            history = []
            for row in rows:
                history.append({
                    "execution_id": row[0],
                    "scheduled_time": row[1],
                    "actual_start_time": row[2],
                    "status": row[3],
                    "retry_count": row[4],
                    "error_message": row[5],
                    "created_at": row[6],
                    "trigger_info": row[7] if row[7] else None
                })

            return history

        except Exception as e:
            self.logger.exception(f"Failed to get execution history: {e}")
            return []

    def get_scheduled_pipelines(self) -> List[Dict[str, Any]]:
        """Get all active scheduled pipelines."""
        scheduled = []
        for pipeline_id, schedule in self.active_schedules.items():
            if schedule.is_active:
                next_run = self.get_next_execution_time(pipeline_id)
                scheduled.append({
                    "pipeline_id": pipeline_id,
                    "cron_expression": schedule.cron_expression,
                    "schedule_type": schedule.schedule_type,
                    "next_execution": next_run.isoformat() if next_run else None,
                    "concurrent_policy": schedule.concurrent_policy,
                    "last_updated": schedule.updated_at.isoformat()
                })

        return scheduled

    def update_schedule(self, pipeline_id: str, new_cron: str, **kwargs) -> None:
        """Update pipeline schedule configuration."""
        if pipeline_id not in self.active_schedules:
            raise ValueError(f"No schedule found for pipeline {pipeline_id}")

        if not self.validate_cron_expression(new_cron):
            raise ValueError(f"Invalid cron expression: {new_cron}")

        schedule = self.active_schedules[pipeline_id]
        schedule.cron_expression = new_cron
        schedule.updated_at = datetime.utcnow()

        # Update other fields if provided
        for key, value in kwargs.items():
            if hasattr(schedule, key):
                setattr(schedule, key, value)

        self._update_schedule_config(schedule)
        self.logger.info(f"Updated schedule for pipeline {pipeline_id}")

    def validate_cron_expression(self, cron_string: str) -> bool:
        """Validate cron expression syntax."""
        if not CRONITER_AVAILABLE:
            # Basic validation without croniter
            parts = cron_string.split()
            if len(parts) != 5:
                return False
            return True

        try:
            croniter(cron_string)
            return True
        except Exception:
            return False

    def integrate_with_airflow(self, pipeline_id: str, dag_config: Dict[str, Any]) -> str:
        """Generate Airflow DAG configuration."""
        if pipeline_id not in self.active_schedules:
            raise ValueError(f"No schedule found for pipeline {pipeline_id}")

        schedule = self.active_schedules[pipeline_id]

        dag_code = f"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {{
    'owner': '{dag_config.get("owner", "etl_framework")}',
    'depends_on_past': {dag_config.get("depends_on_past", False)},
    'start_date': datetime({dag_config.get("start_year", 2024)}, {dag_config.get("start_month", 1)}, {dag_config.get("start_day", 1)}),
    'email_on_failure': {dag_config.get("email_on_failure", False)},
    'email_on_retry': {dag_config.get("email_on_retry", False)},
    'retries': {schedule.retry_count},
    'retry_delay': timedelta(minutes={schedule.retry_delay_minutes})
}}

dag = DAG(
    '{pipeline_id}',
    default_args=default_args,
    description='{dag_config.get("description", f"ETL Pipeline {pipeline_id}")}',
    schedule_interval='{schedule.cron_expression}',
    catchup={dag_config.get("catchup", False)},
    max_active_runs={schedule.max_concurrent_runs}
)

def execute_pipeline():
    # Integration code to trigger pipeline execution
    pass

pipeline_task = PythonOperator(
    task_id='execute_pipeline',
    python_callable=execute_pipeline,
    dag=dag,
)
"""

        self.logger.info(f"Generated Airflow DAG for pipeline {pipeline_id}")
        return dag_code

    def integrate_with_adf(self, pipeline_id: str, trigger_config: Dict[str, Any]) -> Dict[str, Any]:
        """Generate Azure Data Factory trigger configuration."""
        if pipeline_id not in self.active_schedules:
            raise ValueError(f"No schedule found for pipeline {pipeline_id}")

        schedule = self.active_schedules[pipeline_id]

        adf_trigger = {
            "name": f"{pipeline_id}_trigger",
            "type": "Microsoft.DataFactory/factories/triggers",
            "apiVersion": "2018-06-01",
            "properties": {
                "type": "ScheduleTrigger",
                "typeProperties": {
                    "recurrence": {
                        "frequency": trigger_config.get("frequency", "Day"),
                        "interval": trigger_config.get("interval", 1),
                        "startTime": trigger_config.get("start_time", datetime.utcnow().isoformat()),
                        "timeZone": trigger_config.get("timezone", "UTC")
                    }
                },
                "pipelines": [
                    {
                        "pipelineReference": {
                            "referenceName": pipeline_id,
                            "type": "PipelineReference"
                        }
                    }
                ]
            }
        }

        self.logger.info(f"Generated ADF trigger for pipeline {pipeline_id}")
        return adf_trigger

    def _scheduler_loop(self) -> None:
        """Main scheduler loop that checks for due executions."""
        while self.running:
            try:
                current_time = datetime.utcnow()

                for pipeline_id, schedule in list(self.active_schedules.items()):
                    if not schedule.is_active or schedule.schedule_type != "time_based":
                        continue

                    if not schedule.cron_expression:
                        continue

                    next_run = self.get_next_execution_time(pipeline_id)
                    if next_run and next_run <= current_time:
                        # Check concurrent policy
                        if self.check_concurrent_policy(pipeline_id):
                            trigger = ExecutionTrigger(
                                trigger_type="scheduled",
                                trigger_id=str(uuid.uuid4())
                            )

                            execution_id = self._create_scheduled_execution(pipeline_id, trigger)
                            self._execute_pipeline(pipeline_id, execution_id, trigger)
                        else:
                            # Handle concurrent policy
                            if schedule.concurrent_policy == "queue":
                                # Queue the execution
                                trigger = ExecutionTrigger(
                                    trigger_type="scheduled",
                                    trigger_id=str(uuid.uuid4())
                                )
                                execution = ScheduledExecution(
                                    execution_id=str(uuid.uuid4()),
                                    pipeline_id=pipeline_id,
                                    scheduled_time=next_run,
                                    trigger_info=trigger
                                )
                                self.execution_queue.append(execution)

                # Process execution queue
                self._process_execution_queue()

                time.sleep(30)  # Check every 30 seconds

            except Exception as e:
                self.logger.exception(f"Scheduler loop error: {e}")
                time.sleep(60)  # Wait longer on error

    def _execute_pipeline(self, pipeline_id: str, execution_id: str, trigger: ExecutionTrigger) -> None:
        """Execute a pipeline."""
        try:
            # Update execution status
            self._update_execution_status(execution_id, "running", actual_start_time=datetime.utcnow())

            # Execute via orchestrator
            if self.orchestrator:
                run_id = self.orchestrator.execute_pipeline(pipeline_id)
                self._update_execution_status(execution_id, "completed")
                self.logger.info(f"Pipeline {pipeline_id} executed successfully with run_id {run_id}")
            else:
                # Simulate execution
                time.sleep(1)
                self._update_execution_status(execution_id, "completed")
                self.logger.info(f"Pipeline {pipeline_id} executed successfully (simulated)")

        except Exception as e:
            error_msg = str(e)
            self._update_execution_status(execution_id, "failed", error_message=error_msg)
            self.logger.exception(f"Pipeline {pipeline_id} execution failed: {e}")

            # Handle retries
            self._handle_retry(pipeline_id, execution_id, trigger)

    def _handle_retry(self, pipeline_id: str, execution_id: str, trigger: ExecutionTrigger) -> None:
        """Handle retry logic for failed executions."""
        if pipeline_id not in self.active_schedules:
            return

        schedule = self.active_schedules[pipeline_id]
        execution = self._get_execution(execution_id)

        if execution and execution.retry_count < schedule.retry_count:
            # Schedule retry
            retry_time = datetime.utcnow() + timedelta(minutes=schedule.retry_delay_minutes)
            retry_execution = ScheduledExecution(
                execution_id=str(uuid.uuid4()),
                pipeline_id=pipeline_id,
                scheduled_time=retry_time,
                trigger_info=trigger,
                retry_count=execution.retry_count + 1
            )

            self.execution_queue.append(retry_execution)
            self.logger.info(f"Scheduled retry {execution.retry_count + 1} for pipeline {pipeline_id}")

    def _process_execution_queue(self) -> None:
        """Process queued executions."""
        current_time = datetime.utcnow()
        to_remove = []

        for execution in self.execution_queue:
            if execution.scheduled_time <= current_time:
                if self.check_concurrent_policy(execution.pipeline_id):
                    self._execute_pipeline(execution.pipeline_id, execution.execution_id, execution.trigger_info)
                    to_remove.append(execution)
                elif self.active_schedules[execution.pipeline_id].concurrent_policy == "skip":
                    self._update_execution_status(execution.execution_id, "skipped")
                    to_remove.append(execution)

        # Remove processed executions
        for execution in to_remove:
            self.execution_queue.remove(execution)

    def _adjust_for_execution_window(self, scheduled_time: datetime, schedule: ScheduleConfig) -> datetime:
        """Adjust execution time to fit within execution window."""
        if not schedule.execution_window_start or not schedule.execution_window_end:
            return scheduled_time

        # Parse window times
        start_hour, start_min = map(int, schedule.execution_window_start.split(':'))
        end_hour, end_min = map(int, schedule.execution_window_end.split(':'))

        scheduled_hour = scheduled_time.hour
        scheduled_min = scheduled_time.minute

        # Check if within window
        scheduled_minutes = scheduled_hour * 60 + scheduled_min
        start_minutes = start_hour * 60 + start_min
        end_minutes = end_hour * 60 + end_min

        if start_minutes <= scheduled_minutes <= end_minutes:
            return scheduled_time

        # Adjust to next valid time
        if scheduled_minutes < start_minutes:
            # Schedule for start of window
            return scheduled_time.replace(hour=start_hour, minute=start_min)
        else:
            # Schedule for next day at start of window
            next_day = scheduled_time + timedelta(days=1)
            return next_day.replace(hour=start_hour, minute=start_min)

    def _check_dependency_completion(self, pipeline_id: str, upstream_run_id: str) -> bool:
        """Check if upstream dependencies are completed."""
        if pipeline_id not in self.active_schedules:
            return False

        schedule = self.active_schedules[pipeline_id]
        if not schedule.dependencies:
            return True

        # Check if upstream_run_id belongs to a dependency
        # This would need integration with execution tracking
        return True  # Simplified for now

    def _get_running_execution_count(self, pipeline_id: str) -> int:
        """Get count of currently running executions for a pipeline."""
        if not self.db:
            return 0

        try:
            sql = """
                SELECT COUNT(*) FROM SCHEDULED_EXECUTIONS
                WHERE pipeline_id = %s AND status = 'running'
            """

            self.db.execute(sql, (pipeline_id,))
            count = self.db.fetchone()[0]
            return count

        except Exception:
            return 0

    def _create_scheduled_execution(self, pipeline_id: str, trigger: ExecutionTrigger) -> str:
        """Create a scheduled execution record."""
        execution_id = str(uuid.uuid4())
        scheduled_time = datetime.utcnow()

        execution = ScheduledExecution(
            execution_id=execution_id,
            pipeline_id=pipeline_id,
            scheduled_time=scheduled_time,
            trigger_info=trigger
        )

        # Store in database
        if self.db:
            try:
                sql = """
                    INSERT INTO SCHEDULED_EXECUTIONS (
                        execution_id, pipeline_id, scheduled_time, status, trigger_info, created_at
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                """

                import json
                params = (
                    execution_id,
                    pipeline_id,
                    scheduled_time,
                    execution.status,
                    json.dumps(asdict(trigger)),
                    execution.created_at
                )

                self.db.begin()
                self.db.execute(sql, params)
                self.db.commit()

            except Exception as e:
                self.db.rollback()
                self.logger.exception(f"Failed to store scheduled execution: {e}")

        return execution_id

    def _update_execution_status(self, execution_id: str, status: str,
                               actual_start_time: Optional[datetime] = None,
                               error_message: Optional[str] = None) -> None:
        """Update execution status."""
        if not self.db:
            return

        try:
            sql = """
                UPDATE SCHEDULED_EXECUTIONS
                SET status = %s, actual_start_time = %s, error_message = %s
                WHERE execution_id = %s
            """

            params = (status, actual_start_time, error_message, execution_id)

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to update execution status: {e}")

    def _get_execution(self, execution_id: str) -> Optional[ScheduledExecution]:
        """Get execution record."""
        if not self.db:
            return None

        try:
            sql = "SELECT * FROM SCHEDULED_EXECUTIONS WHERE execution_id = %s"

            self.db.execute(sql, (execution_id,))
            row = self.db.fetchone()

            if row:
                import json
                return ScheduledExecution(
                    execution_id=row[0],
                    pipeline_id=row[1],
                    scheduled_time=row[2],
                    actual_start_time=row[3],
                    status=row[4],
                    retry_count=row[5] or 0,
                    error_message=row[6],
                    created_at=row[7]
                )

        except Exception as e:
            self.logger.exception(f"Failed to get execution: {e}")

        return None

    def _store_schedule_config(self, schedule: ScheduleConfig) -> None:
        """Store schedule configuration in database."""
        if not self.db:
            return

        try:
            import json
            sql = """
                INSERT INTO PIPELINE_SCHEDULES (
                    pipeline_id, cron_expression, schedule_type, is_active,
                    concurrent_policy, max_concurrent_runs, retry_count,
                    retry_delay_minutes, execution_timeout_hours,
                    execution_window_start, execution_window_end,
                    dependencies, event_triggers, conditions, created_at, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """

            params = (
                schedule.pipeline_id,
                schedule.cron_expression,
                schedule.schedule_type,
                schedule.is_active,
                schedule.concurrent_policy,
                schedule.max_concurrent_runs,
                schedule.retry_count,
                schedule.retry_delay_minutes,
                schedule.execution_timeout_hours,
                schedule.execution_window_start,
                schedule.execution_window_end,
                json.dumps(schedule.dependencies),
                json.dumps(schedule.event_triggers),
                json.dumps(schedule.conditions),
                schedule.created_at,
                schedule.updated_at
            )

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to store schedule config: {e}")

    def _update_schedule_config(self, schedule: ScheduleConfig) -> None:
        """Update schedule configuration in database."""
        if not self.db:
            return

        try:
            import json
            sql = """
                UPDATE PIPELINE_SCHEDULES
                SET cron_expression = %s, schedule_type = %s, is_active = %s,
                    concurrent_policy = %s, max_concurrent_runs = %s, retry_count = %s,
                    retry_delay_minutes = %s, execution_timeout_hours = %s,
                    execution_window_start = %s, execution_window_end = %s,
                    dependencies = %s, event_triggers = %s, conditions = %s, updated_at = %s
                WHERE pipeline_id = %s
            """

            params = (
                schedule.cron_expression,
                schedule.schedule_type,
                schedule.is_active,
                schedule.concurrent_policy,
                schedule.max_concurrent_runs,
                schedule.retry_count,
                schedule.retry_delay_minutes,
                schedule.execution_timeout_hours,
                schedule.execution_window_start,
                schedule.execution_window_end,
                json.dumps(schedule.dependencies),
                json.dumps(schedule.event_triggers),
                json.dumps(schedule.conditions),
                schedule.updated_at,
                schedule.pipeline_id
            )

            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Failed to update schedule config: {e}")