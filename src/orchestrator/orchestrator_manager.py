import logging
import time
import uuid
from datetime import datetime
from typing import Any, Callable, Dict, Optional, Tuple

# ...existing code...
class OrchestratorManager:
    """
    OrchestratorManager coordinates pipeline runs:
    - loads and validates metadata
    - creates execution records
    - orchestrates extract -> transform -> load -> validate -> monitor
    - handles retries, errors, alerts and real-time status updates
    """

    def __init__(
        self,
        config_loader,
        connector_factory,
        engine_selector,
        transform_engine,
        dq_engine,
        sla_monitor,
        alert_manager,
        db_connection,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        self.config_loader = config_loader
        self.connector_factory = connector_factory
        self.engine_selector = engine_selector
        self.transform_engine = transform_engine
        self.dq_engine = dq_engine
        self.sla_monitor = sla_monitor
        self.alert_manager = alert_manager
        self.db = db_connection
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.setLevel(logging.INFO)

    # 1. execute_pipeline
    def execute_pipeline(self, pipeline_id: str, trigger_type: str, triggered_by: str) -> Dict[str, Any]:
        run_id = None
        start_time = datetime.utcnow()
        try:
            self.logger.info("Starting pipeline execution: %s", pipeline_id)
            config = self.load_configuration(pipeline_id)
            self.validate_metadata(config)

            run_id = self.create_execution_record(pipeline_id, trigger_type, triggered_by)
            self.update_execution_status(run_id, "running", {"start_time": start_time.isoformat()})

            # extraction
            extracted_data = self.retry_logic(
                lambda: self.orchestrate_extraction(config),
                max_retries=self.max_retries,
                backoff_factor=self.backoff_factor,
            )

            # transform
            transformed_data = self.retry_logic(
                lambda: self.orchestrate_transformation(extracted_data, config),
                max_retries=self.max_retries,
                backoff_factor=self.backoff_factor,
            )

            # load
            load_result = self.retry_logic(
                lambda: self.orchestrate_loading(transformed_data, config),
                max_retries=self.max_retries,
                backoff_factor=self.backoff_factor,
            )

            # data quality
            dq_passed, dq_report = self.orchestrate_dq_validation(transformed_data, config)

            # SLA / monitoring
            self.sla_monitor.record_run(run_id, pipeline_id, start_time, datetime.utcnow(), dq_passed)

            # finalize
            self.finalize_execution(run_id, success=(dq_passed and load_result))
            return {"run_id": run_id, "status": "success" if dq_passed and load_result else "partial_failure", "dq_report": dq_report}
        except Exception as e:
            context = {"pipeline_id": pipeline_id, "run_id": run_id}
            self.handle_failure(e, context)
            if run_id:
                self.finalize_execution(run_id, success=False)
            return {"run_id": run_id, "status": "failed", "error": str(e)}

    # 2. load_configuration
    def load_configuration(self, pipeline_id: str) -> Dict[str, Any]:
        """
        Reads metadata from relevant tables and returns consolidated config dict.
        Expects config_loader and db to be provided.
        """
        self.logger.debug("Loading configuration for pipeline: %s", pipeline_id)
        config = self.config_loader.load_pipeline_metadata(pipeline_id)
        if not config:
            raise ValueError(f"No metadata found for pipeline {pipeline_id}")
        return config

    # 3. validate_metadata
    def validate_metadata(self, config: Dict[str, Any]) -> bool:
        self.logger.debug("Validating pipeline metadata")
        required_sections = ["pipeline", "connections", "sources", "targets", "transformations"]
        missing = [s for s in required_sections if s not in config]
        if missing:
            raise ValueError(f"Metadata validation failed. Missing sections: {missing}")

        # simple checks for required fields
        pipeline = config.get("pipeline", {})
        if not pipeline.get("id") or not pipeline.get("name"):
            raise ValueError("Pipeline metadata must include id and name")

        # validate references between transformations and sources/targets
        trans = config.get("transformations", [])
        conn_ids = {c["id"] for c in config.get("connections", [])}
        for t in trans:
            for ref in ("source_connection_id", "target_connection_id"):
                if ref in t and t[ref] not in conn_ids:
                    raise ValueError(f"Transformation {t.get('id')} references missing connection {t[ref]}")

        self.logger.info("Metadata validated for pipeline %s", pipeline.get("id"))
        return True

    # 4. create_execution_record
    def create_execution_record(self, pipeline_id: str, trigger_type: str, triggered_by: str) -> str:
        self.logger.debug("Creating execution record for pipeline: %s", pipeline_id)
        run_id = str(uuid.uuid4())
        start_time = datetime.utcnow()
        sql = """
            INSERT INTO PIPELINE_RUNS (run_id, pipeline_id, status, trigger_type, triggered_by, start_time)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        params = (run_id, pipeline_id, "running", trigger_type, triggered_by, start_time)
        try:
            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()
            self.logger.info("Created execution run %s for pipeline %s", run_id, pipeline_id)
            return run_id
        except Exception:
            self.db.rollback()
            self.logger.exception("Failed to create execution record")
            raise

    # 5. orchestrate_extraction
    def orchestrate_extraction(self, config: Dict[str, Any]) -> Any:
        self.logger.info("Starting extraction phase")
        sources = config.get("sources", [])
        extracted = {}
        for src in sources:
            connector = self.connector_factory.get_connector(src["connection_type"], src["connection_config"])
            self.logger.debug("Extracting from source %s using %s", src.get("id"), type(connector).__name__)
            data = connector.extract(src.get("query") or src.get("path"), src.get("options", {}))
            extracted[src.get("id")] = data
            self.update_execution_status(None, "extract.progress", {"source": src.get("id")})
        self.logger.info("Extraction phase completed")
        return extracted

    # 6. orchestrate_transformation
    def orchestrate_transformation(self, data: Any, config: Dict[str, Any]) -> Any:
        self.logger.info("Starting transformation phase")
        engine_type = self.engine_selector.select(config.get("transformations", []))
        engine = self.transform_engine.get_engine(engine_type)
        transformed = engine.transform(data, config.get("transformations", []))
        self.update_execution_status(None, "transform.completed", {"rows": self._approx_row_count(transformed)})
        self.logger.info("Transformation phase completed")
        return transformed

    # 7. orchestrate_loading
    def orchestrate_loading(self, data: Any, config: Dict[str, Any]) -> bool:
        self.logger.info("Starting loading phase")
        targets = config.get("targets", [])
        all_success = True
        for tgt in targets:
            connector = self.connector_factory.get_connector(tgt["connection_type"], tgt["connection_config"])
            self.logger.debug("Loading to target %s using %s", tgt.get("id"), type(connector).__name__)
            result = connector.load(data.get(tgt.get("source_id")) if isinstance(data, dict) else data, tgt.get("options", {}))
            if not result:
                all_success = False
                self.logger.warning("Load to target %s reported failure", tgt.get("id"))
            self.update_execution_status(None, "load.progress", {"target": tgt.get("id"), "success": result})
        self.logger.info("Loading phase completed with success=%s", all_success)
        return all_success

    # 8. orchestrate_dq_validation
    def orchestrate_dq_validation(self, data: Any, config: Dict[str, Any]) -> Tuple[bool, Dict[str, Any]]:
        self.logger.info("Starting data quality validation")
        rules = config.get("dq_rules", [])
        dq_result = self.dq_engine.validate(data, rules)
        self.update_execution_status(None, "dq.completed", {"passed": dq_result.get("passed", False)})
        self.logger.info("DQ validation completed. Passed=%s", dq_result.get("passed", False))
        return dq_result.get("passed", False), dq_result

    # 9. handle_failure
    def handle_failure(self, error: Exception, context: Optional[Dict[str, Any]] = None) -> None:
        context = context or {}
        run_id = context.get("run_id")
        pipeline_id = context.get("pipeline_id")
        self.logger.exception("Pipeline failure. Pipeline=%s Run=%s Error=%s", pipeline_id, run_id, str(error))
        # persist error
        try:
            sql = """
                INSERT INTO ERROR_LOG (error_id, run_id, pipeline_id, error_message, error_trace, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            error_id = str(uuid.uuid4())
            params = (
                error_id,
                run_id,
                pipeline_id,
                str(error),
                getattr(error, "__traceback__", None),
                datetime.utcnow(),
            )
            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()
        except Exception:
            self.db.rollback()
            self.logger.exception("Failed to write to ERROR_LOG")

        # update run status
        if run_id:
            try:
                self.update_execution_status(run_id, "failed", {"error": str(error)})
            except Exception:
                self.logger.exception("Failed to update execution status to failed")

        # trigger alert
        try:
            self.alert_manager.send_alert(
                subject=f"Pipeline failure: {pipeline_id}",
                body=f"Run {run_id} failed with error: {error}",
                severity="HIGH",
            )
        except Exception:
            self.logger.exception("Failed to send alert for pipeline failure")

    # 10. retry_logic
    def retry_logic(self, function: Callable[[], Any], max_retries: Optional[int] = None, backoff_factor: Optional[float] = None) -> Any:
        max_retries = max_retries if max_retries is not None else self.max_retries
        backoff_factor = backoff_factor if backoff_factor is not None else self.backoff_factor

        attempt = 0
        while True:
            try:
                attempt += 1
                return function()
            except Exception as e:
                if attempt > max_retries:
                    self.logger.exception("Max retries exceeded (%s). Raising.", max_retries)
                    raise
                sleep_time = (backoff_factor ** (attempt - 1))
                self.logger.warning("Attempt %s/%s failed: %s. Retrying in %.2fs", attempt, max_retries, str(e), sleep_time)
                time.sleep(sleep_time)

    # 11. update_execution_status
    def update_execution_status(self, run_id: Optional[str], status: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        """
        Updates PIPELINE_RUNS.status or writes progress events to EXECUTION_EVENTS table.
        If run_id is None, only creates an event record.
        """
        metadata = metadata or {}
        now = datetime.utcnow()
        try:
            self.db.begin()
            if run_id:
                sql = "UPDATE PIPELINE_RUNS SET status = %s, last_update = %s WHERE run_id = %s"
                self.db.execute(sql, (status, now, run_id))
            # write event
            ev_sql = "INSERT INTO EXECUTION_EVENTS (event_id, run_id, status, metadata, created_at) VALUES (%s, %s, %s, %s, %s)"
            ev_id = str(uuid.uuid4())
            self.db.execute(ev_sql, (ev_id, run_id, status, str(metadata), now))
            self.db.commit()
            self.logger.debug("Execution status updated: run_id=%s status=%s", run_id, status)
        except Exception:
            self.db.rollback()
            self.logger.exception("Failed to update execution status")

    # 12. finalize_execution
    def finalize_execution(self, run_id: str, success: bool) -> None:
        end_time = datetime.utcnow()
        try:
            # compute duration if start_time present
            row = self.db.query("SELECT start_time FROM PIPELINE_RUNS WHERE run_id = %s", (run_id,))
            start_time = row[0]["start_time"] if row else None
            duration = None
            if start_time:
                duration = (end_time - start_time).total_seconds()
            status = "success" if success else "failed"
            sql = "UPDATE PIPELINE_RUNS SET status = %s, end_time = %s, duration_seconds = %s WHERE run_id = %s"
            self.db.begin()
            self.db.execute(sql, (status, end_time, duration, run_id))
            # audit trail
            audit_sql = "INSERT INTO AUDIT_LOG (audit_id, run_id, action, created_at) VALUES (%s, %s, %s, %s)"
            audit_id = str(uuid.uuid4())
            self.db.execute(audit_sql, (audit_id, run_id, f"finalized:{status}", end_time))
            self.db.commit()
            self.logger.info("Finalized run %s with status %s (duration=%s)", run_id, status, duration)
        except Exception:
            self.db.rollback()
            self.logger.exception("Failed to finalize execution record")

    # helpers
    def _approx_row_count(self, data: Any) -> int:
        try:
            if isinstance(data, dict):
                return sum(len(v) if hasattr(v, "__len__") else 1 for v in data.values())
            if hasattr(data, "__len__"):
                return len(data)
            return 1
        except Exception:
            return 0