import logging
import uuid
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

logger = logging.getLogger("AuditLogger")


class AuditLogger:
    """
    AuditLogger logs all significant events and actions to ensure compliance and traceability.
    Supports various event types and audit reporting.
    """

    def __init__(self, db_connection: Any):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)

    def log_event(
        self,
        event_type: str,
        action: str,
        run_id: Optional[str] = None,
        user_id: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None,
        ip_address: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> str:
        """Main logging method."""
        audit_id = str(uuid.uuid4())
        details = details or {}

        try:
            sql = """
                INSERT INTO AUDIT_TRAIL (audit_id, run_id, event_type, action, action_timestamp, user_id, action_details, ip_address, session_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (
                audit_id,
                run_id,
                event_type,
                action,
                datetime.utcnow(),
                user_id,
                json.dumps(details),
                ip_address,
                session_id,
            )
            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()
            self.logger.debug("Logged audit event: %s - %s", event_type, action)
            return audit_id
        except Exception:
            self.db.rollback()
            self.logger.exception("Failed to log audit event")
            raise

    def log_pipeline_start(self, run_id: str, pipeline_id: str, triggered_by: str, trigger_type: str) -> str:
        """Log pipeline execution start."""
        return self.log_event(
            "pipeline_execution",
            "start",
            run_id=run_id,
            details={
                "pipeline_id": pipeline_id,
                "triggered_by": triggered_by,
                "trigger_type": trigger_type,
            }
        )

    def log_pipeline_complete(self, run_id: str, status: str, duration: float, rows_processed: int) -> str:
        """Log pipeline execution completion."""
        return self.log_event(
            "pipeline_execution",
            "complete",
            run_id=run_id,
            details={
                "status": status,
                "duration_seconds": duration,
                "rows_processed": rows_processed,
            }
        )

    def log_pipeline_failure(self, run_id: str, error_message: str, stack_trace: Optional[str] = None) -> str:
        """Log pipeline execution failure."""
        return self.log_event(
            "pipeline_execution",
            "failure",
            run_id=run_id,
            details={
                "error_message": error_message,
                "stack_trace": stack_trace,
            }
        )

    def log_config_change(
        self,
        table_name: str,
        record_id: str,
        change_type: str,
        old_value: Optional[Dict[str, Any]],
        new_value: Optional[Dict[str, Any]],
        user_id: str,
    ) -> str:
        """Log configuration changes."""
        return self.log_event(
            "config_change",
            change_type,
            user_id=user_id,
            details={
                "table_name": table_name,
                "record_id": record_id,
                "old_value": old_value,
                "new_value": new_value,
            }
        )

    def log_user_action(
        self,
        user_id: str,
        action_type: str,
        resource: str,
        ip_address: Optional[str] = None,
        session_id: Optional[str] = None,
    ) -> str:
        """Log user actions."""
        return self.log_event(
            "user_action",
            action_type,
            user_id=user_id,
            ip_address=ip_address,
            session_id=session_id,
            details={"resource": resource}
        )

    def log_system_event(self, event_type: str, event_details: Dict[str, Any], severity: str = "INFO") -> str:
        """Log system events."""
        return self.log_event(
            "system_event",
            event_type,
            details={**event_details, "severity": severity}
        )

    def log_data_access(
        self,
        user_id: str,
        data_source: str,
        access_type: str,
        row_count: Optional[int] = None,
        timestamp: Optional[datetime] = None,
    ) -> str:
        """Log data access events."""
        return self.log_event(
            "data_access",
            access_type,
            user_id=user_id,
            details={
                "data_source": data_source,
                "row_count": row_count,
                "timestamp": timestamp.isoformat() if timestamp else None,
            }
        )

    def log_security_event(
        self,
        user_id: Optional[str],
        event_type: str,
        success: bool,
        ip_address: Optional[str],
        details: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Log security events."""
        return self.log_event(
            "security_event",
            event_type,
            user_id=user_id,
            ip_address=ip_address,
            details={**details, "success": success} if details else {"success": success}
        )

    def get_audit_trail(self, filters: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Queries audit trail with filters."""
        try:
            where_clauses = []
            params = []

            if "event_type" in filters:
                where_clauses.append("event_type = %s")
                params.append(filters["event_type"])

            if "user_id" in filters:
                where_clauses.append("user_id = %s")
                params.append(filters["user_id"])

            if "run_id" in filters:
                where_clauses.append("run_id = %s")
                params.append(filters["run_id"])

            if "start_date" in filters:
                where_clauses.append("action_timestamp >= %s")
                params.append(filters["start_date"])

            if "end_date" in filters:
                where_clauses.append("action_timestamp <= %s")
                params.append(filters["end_date"])

            if "ip_address" in filters:
                where_clauses.append("ip_address = %s")
                params.append(filters["ip_address"])

            where_sql = " AND ".join(where_clauses) if where_clauses else "1=1"

            sql = f"""
                SELECT audit_id, run_id, event_type, action, action_timestamp, user_id, action_details, ip_address, session_id
                FROM AUDIT_TRAIL
                WHERE {where_sql}
                ORDER BY action_timestamp DESC
                LIMIT %s OFFSET %s
            """
            params.extend([filters.get("limit", 100), filters.get("offset", 0)])

            rows = self.db.query(sql, tuple(params))
            return [dict(row) for row in rows]
        except Exception:
            self.logger.exception("Failed to query audit trail")
            return []

    def generate_audit_report(self, start_date: datetime, end_date: datetime, event_types: Optional[List[str]] = None) -> Dict[str, Any]:
        """Creates audit report."""
        try:
            filters = {"start_date": start_date, "end_date": end_date}
            if event_types:
                # For simplicity, generate separate reports per event type
                report = {"period": f"{start_date} to {end_date}", "event_summaries": {}}
                for et in event_types:
                    filters["event_type"] = et
                    events = self.get_audit_trail(filters)
                    report["event_summaries"][et] = {
                        "count": len(events),
                        "sample_events": events[:5],  # First 5 events
                    }
                return report
            else:
                events = self.get_audit_trail(filters)
                return {
                    "period": f"{start_date} to {end_date}",
                    "total_events": len(events),
                    "events": events,
                }
        except Exception:
            self.logger.exception("Failed to generate audit report")
            return {"error": "Failed to generate report"}

    def get_config_change_history(self, table_name: str, record_id: str) -> List[Dict[str, Any]]:
        """Shows configuration evolution."""
        try:
            sql = """
                SELECT audit_id, action_timestamp, user_id, action_details
                FROM AUDIT_TRAIL
                WHERE event_type = 'config_change' AND action_details->>'table_name' = %s AND action_details->>'record_id' = %s
                ORDER BY action_timestamp DESC
            """
            rows = self.db.query(sql, (table_name, record_id))
            return [dict(row) for row in rows]
        except Exception:
            self.logger.exception("Failed to get config change history")
            return []

    def get_user_activity(self, user_id: str, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Shows user action history."""
        try:
            sql = """
                SELECT audit_id, event_type, action, action_timestamp, action_details, ip_address, session_id
                FROM AUDIT_TRAIL
                WHERE user_id = %s AND action_timestamp BETWEEN %s AND %s
                ORDER BY action_timestamp DESC
            """
            rows = self.db.query(sql, (user_id, start_date, end_date))
            return [dict(row) for row in rows]
        except Exception:
            self.logger.exception("Failed to get user activity")
            return []

    def archive_old_logs(self, retention_days: int = 365) -> int:
        """Archives old audit logs."""
        try:
            cutoff_date = datetime.utcnow() - timedelta(days=retention_days)
            # In a real system, move to archive table or file
            # For now, just count
            sql = "SELECT COUNT(*) as count FROM AUDIT_TRAIL WHERE action_timestamp < %s"
            rows = self.db.query(sql, (cutoff_date,))
            count = rows[0]["count"] if rows else 0

            # Archive logic would go here
            self.logger.info("Would archive %d old audit logs", count)
            return count
        except Exception:
            self.logger.exception("Failed to archive old logs")
            return 0

    def get_data_access_summary(self, start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Summarizes data access events."""
        try:
            sql = """
                SELECT data_source, access_type, COUNT(*) as access_count, SUM((action_details->>'row_count')::int) as total_rows
                FROM AUDIT_TRAIL
                WHERE event_type = 'data_access' AND action_timestamp BETWEEN %s AND %s
                GROUP BY data_source, access_type
                ORDER BY access_count DESC
            """
            rows = self.db.query(sql, (start_date, end_date))
            return {
                "period": f"{start_date} to {end_date}",
                "access_summary": [dict(row) for row in rows],
            }
        except Exception:
            self.logger.exception("Failed to get data access summary")
            return {"error": "Failed to get summary"}

    def get_security_incidents(self, start_date: datetime, end_date: datetime) -> List[Dict[str, Any]]:
        """Gets security incidents."""
        try:
            sql = """
                SELECT audit_id, action_timestamp, user_id, action, action_details, ip_address
                FROM AUDIT_TRAIL
                WHERE event_type = 'security_event' AND (action_details->>'success')::boolean = false
                AND action_timestamp BETWEEN %s AND %s
                ORDER BY action_timestamp DESC
            """
            rows = self.db.query(sql, (start_date, end_date))
            return [dict(row) for row in rows]
        except Exception:
            self.logger.exception("Failed to get security incidents")
            return []