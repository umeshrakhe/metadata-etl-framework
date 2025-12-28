import logging
import uuid
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("SLAMonitor")


class SLAMonitor:
    """
    SLAMonitor checks SLA compliance for pipeline executions and monitors various SLA types.
    Triggers alerts on violations and stores compliance results.
    """

    def __init__(self, db_connection: Any, alert_manager: Any):
        self.db = db_connection
        self.alert_manager = alert_manager
        self.logger = logging.getLogger(self.__class__.__name__)

    def check_sla_compliance(self, run_id: str) -> Dict[str, Any]:
        """Main compliance check method."""
        try:
            # Get pipeline_id from run_id
            run_info = self.db.query("SELECT pipeline_id FROM PIPELINE_RUNS WHERE run_id = %s", (run_id,))
            if not run_info:
                return {"error": "Run not found"}
            pipeline_id = run_info[0]["pipeline_id"]

            sla_definitions = self.get_sla_definitions(pipeline_id)
            if not sla_definitions:
                return {"status": "no_sla_defined", "run_id": run_id}

            sla_checks = {}
            for sla in sla_definitions:
                sla_type = sla.get("sla_type")
                if sla_type == "execution_time":
                    sla_checks[sla_type] = self.check_execution_time_sla(run_id, sla)
                elif sla_type == "data_freshness":
                    sla_checks[sla_type] = self.check_data_freshness_sla(run_id, sla)
                elif sla_type == "quality_score":
                    sla_checks[sla_type] = self.check_quality_score_sla(run_id, sla)
                elif sla_type == "row_count":
                    sla_checks[sla_type] = self.check_row_count_sla(run_id, sla)
                # Availability can be checked separately

            overall_compliance = all(check.get("status") == "met" for check in sla_checks.values())
            result = {
                "run_id": run_id,
                "pipeline_id": pipeline_id,
                "sla_checks": sla_checks,
                "overall_compliance": overall_compliance,
                "checked_at": datetime.utcnow().isoformat(),
            }

            self.store_compliance_results(run_id, sla_checks)

            # Trigger alerts for violations
            for sla_type, check in sla_checks.items():
                if check.get("status") == "violated":
                    self.trigger_sla_violation_alert(check, sla_definitions)

            return result
        except Exception as e:
            self.logger.exception("SLA compliance check failed for run %s", run_id)
            return {"error": str(e), "run_id": run_id}

    def get_sla_definitions(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """Reads from SLA_DEFINITIONS table."""
        try:
            sql = "SELECT * FROM SLA_DEFINITIONS WHERE pipeline_id = %s AND active = TRUE"
            rows = self.db.query(sql, (pipeline_id,))
            return [dict(row) for row in rows]
        except Exception:
            self.logger.exception("Failed to get SLA definitions for pipeline %s", pipeline_id)
            return []

    def check_execution_time_sla(self, run_id: str, sla_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validates execution duration."""
        try:
            run_info = self.db.query("SELECT start_time, end_time, duration_seconds FROM PIPELINE_RUNS WHERE run_id = %s", (run_id,))
            if not run_info:
                return {"status": "unknown", "error": "Run info not found"}
            duration = run_info[0]["duration_seconds"]
            if duration is None:
                return {"status": "unknown", "error": "Duration not available"}

            threshold = sla_config.get("threshold")
            operator = sla_config.get("operator", "lt")
            status = self.determine_compliance_status(duration, threshold, operator)
            deviation = self.calculate_deviation(duration, threshold, operator)

            return {
                "sla_type": "execution_time",
                "actual": duration,
                "threshold": threshold,
                "operator": operator,
                "status": status,
                "deviation": deviation,
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def check_data_freshness_sla(self, run_id: str, sla_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validates data age."""
        try:
            # Assume data timestamp from load log or run end_time
            run_info = self.db.query("SELECT end_time FROM PIPELINE_RUNS WHERE run_id = %s", (run_id,))
            if not run_info:
                return {"status": "unknown", "error": "Run info not found"}
            data_timestamp = run_info[0]["end_time"]
            now = datetime.utcnow()
            age_hours = (now - data_timestamp).total_seconds() / 3600

            threshold = sla_config.get("threshold")
            operator = sla_config.get("operator", "lt")
            status = self.determine_compliance_status(age_hours, threshold, operator)
            deviation = self.calculate_deviation(age_hours, threshold, operator)

            return {
                "sla_type": "data_freshness",
                "actual": age_hours,
                "threshold": threshold,
                "operator": operator,
                "status": status,
                "deviation": deviation,
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def check_quality_score_sla(self, run_id: str, sla_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validates quality score."""
        try:
            # Get quality score from QUALITY_SCORES
            score_info = self.db.query("SELECT score FROM QUALITY_SCORES WHERE run_id = %s ORDER BY created_at DESC LIMIT 1", (run_id,))
            if not score_info:
                return {"status": "unknown", "error": "Quality score not found"}
            score = score_info[0]["score"]

            threshold = sla_config.get("threshold")
            operator = sla_config.get("operator", "gt")
            status = self.determine_compliance_status(score, threshold, operator)
            deviation = self.calculate_deviation(score, threshold, operator)

            return {
                "sla_type": "quality_score",
                "actual": score,
                "threshold": threshold,
                "operator": operator,
                "status": status,
                "deviation": deviation,
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def check_row_count_sla(self, run_id: str, sla_config: Dict[str, Any]) -> Dict[str, Any]:
        """Validates row counts."""
        try:
            # Get row count from LOAD_LOG
            load_info = self.db.query("SELECT SUM(rows_inserted + rows_updated) as total_rows FROM LOAD_LOG WHERE run_id = %s", (run_id,))
            if not load_info or load_info[0]["total_rows"] is None:
                return {"status": "unknown", "error": "Row count not found"}
            row_count = load_info[0]["total_rows"]

            threshold = sla_config.get("threshold")
            operator = sla_config.get("operator", "between")
            status = self.determine_compliance_status(row_count, threshold, operator)
            deviation = self.calculate_deviation(row_count, threshold, operator)

            return {
                "sla_type": "row_count",
                "actual": row_count,
                "threshold": threshold,
                "operator": operator,
                "status": status,
                "deviation": deviation,
            }
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def calculate_deviation(self, actual: float, threshold: Any, operator: str) -> float:
        """Calculates deviation percentage."""
        if operator in ("lt", "gt"):
            return abs(actual - threshold) / threshold * 100 if threshold != 0 else 0
        elif operator == "between":
            min_val, max_val = threshold
            if actual < min_val:
                return (min_val - actual) / min_val * 100
            elif actual > max_val:
                return (actual - max_val) / max_val * 100
            else:
                return 0
        return 0

    def determine_compliance_status(self, actual: float, threshold: Any, operator: str) -> str:
        """Returns met/violated."""
        if operator == "lt":
            return "met" if actual < threshold else "violated"
        elif operator == "gt":
            return "met" if actual > threshold else "violated"
        elif operator == "eq":
            return "met" if actual == threshold else "violated"
        elif operator == "between":
            min_val, max_val = threshold
            return "met" if min_val <= actual <= max_val else "violated"
        return "unknown"

    def store_compliance_results(self, run_id: str, sla_checks: Dict[str, Any]) -> None:
        """Writes to SLA_COMPLIANCE table."""
        try:
            for sla_type, check in sla_checks.items():
                sql = """
                    INSERT INTO SLA_COMPLIANCE (compliance_id, run_id, sla_type, actual_value, threshold, operator, status, deviation, checked_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                compliance_id = str(uuid.uuid4())
                params = (
                    compliance_id,
                    run_id,
                    sla_type,
                    check.get("actual"),
                    json.dumps(check.get("threshold")),
                    check.get("operator"),
                    check.get("status"),
                    check.get("deviation"),
                    datetime.utcnow(),
                )
                self.db.begin()
                self.db.execute(sql, params)
                self.db.commit()
            self.logger.info("Stored SLA compliance results for run %s", run_id)
        except Exception:
            self.db.rollback()
            self.logger.exception("Failed to store SLA compliance results")

    def calculate_sla_compliance_rate(self, pipeline_id: str, days: int = 30) -> float:
        """Calculates compliance % over period."""
        try:
            start_date = datetime.utcnow() - timedelta(days=days)
            sql = """
                SELECT COUNT(*) as total, SUM(CASE WHEN status = 'met' THEN 1 ELSE 0 END) as met
                FROM SLA_COMPLIANCE sc
                JOIN PIPELINE_RUNS pr ON sc.run_id = pr.run_id
                WHERE pr.pipeline_id = %s AND sc.checked_at >= %s
            """
            result = self.db.query(sql, (pipeline_id, start_date))
            if result and result[0]["total"] > 0:
                return result[0]["met"] / result[0]["total"] * 100
            return 0.0
        except Exception:
            self.logger.exception("Failed to calculate SLA compliance rate")
            return 0.0

    def trigger_sla_violation_alert(self, sla_check: Dict[str, Any], alert_config: Dict[str, Any]) -> None:
        """Sends alert to AlertManager."""
        try:
            subject = f"SLA Violation: {sla_check.get('sla_type')} for run {sla_check.get('run_id')}"
            body = f"""
            SLA Violation Detected:
            Type: {sla_check.get('sla_type')}
            Actual: {sla_check.get('actual')}
            Threshold: {sla_check.get('threshold')}
            Deviation: {sla_check.get('deviation')}%
            """
            severity = "HIGH" if sla_check.get("deviation", 0) > 50 else "MEDIUM"
            self.alert_manager.send_alert(subject, body, severity)
        except Exception:
            self.logger.exception("Failed to trigger SLA violation alert")

    def generate_sla_report(self, pipeline_id: str, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Creates compliance report."""
        try:
            sql = """
                SELECT sla_type, status, COUNT(*) as count
                FROM SLA_COMPLIANCE sc
                JOIN PIPELINE_RUNS pr ON sc.run_id = pr.run_id
                WHERE pr.pipeline_id = %s AND sc.checked_at BETWEEN %s AND %s
                GROUP BY sla_type, status
            """
            rows = self.db.query(sql, (pipeline_id, start_date, end_date))
            report = {"pipeline_id": pipeline_id, "period": f"{start_date} to {end_date}", "breakdown": {}}
            for row in rows:
                sla_type = row["sla_type"]
                status = row["status"]
                if sla_type not in report["breakdown"]:
                    report["breakdown"][sla_type] = {}
                report["breakdown"][sla_type][status] = row["count"]
            return report
        except Exception:
            self.logger.exception("Failed to generate SLA report")
            return {"error": "Failed to generate report"}

    def record_run(self, run_id: str, pipeline_id: str, start_time: datetime, end_time: datetime, dq_passed: bool) -> None:
        """Record run for SLA monitoring."""
        # This can be called from orchestrator to ensure SLA checks are triggered
        pass

    def predictive_sla_warning(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """Predictive SLA violation warnings based on trends."""
        # Simple trend analysis: if recent compliance is dropping, warn
        recent_rate = self.calculate_sla_compliance_rate(pipeline_id, 7)
        overall_rate = self.calculate_sla_compliance_rate(pipeline_id, 30)
        if recent_rate < overall_rate * 0.9:
            return [{"warning": "Compliance rate dropping", "recent": recent_rate, "overall": overall_rate}]
        return []