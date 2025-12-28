import logging
import uuid
import json
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# Optional imports
try:
    import requests
    REQUESTS_AVAILABLE = True
except ImportError:
    requests = None
    REQUESTS_AVAILABLE = False

logger = logging.getLogger("AlertManager")


class AlertManager:
    """
    AlertManager sends alerts through multiple channels based on severity and configuration.
    Supports rate limiting, escalation, and delivery tracking.
    """

    def __init__(self, db_connection: Any, smtp_config: Optional[Dict[str, Any]] = None):
        self.db = db_connection
        self.smtp_config = smtp_config or {}
        self.logger = logging.getLogger(self.__class__.__name__)
        self.rate_limits = {}  # In-memory rate limiting, in production use Redis

    def send_alert(self, alert_type: str, severity: str, message: str, context: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Main alert sending method."""
        context = context or {}
        alert_id = str(uuid.uuid4())

        try:
            # Check rate limit
            if not self.check_rate_limit(alert_type, 300):  # 5 minutes
                self.logger.warning("Rate limit exceeded for alert type %s", alert_type)
                return {"status": "rate_limited", "alert_id": alert_id}

            config = self.get_alert_config(alert_type)
            if not config:
                self.logger.warning("No alert config found for type %s", alert_type)
                return {"status": "no_config", "alert_id": alert_id}

            channels = self._get_channels_for_severity(severity, config)
            formatted_message = self.format_message(config.get("template", message), context)

            results = {}
            for channel in channels:
                try:
                    if channel == "email":
                        result = self.send_email(
                            config.get("email_recipients", []),
                            f"Alert: {alert_type}",
                            formatted_message,
                            context.get("attachments", [])
                        )
                    elif channel == "slack":
                        result = self.send_slack_message(
                            config.get("slack_webhook"),
                            formatted_message,
                            config.get("slack_channel")
                        )
                    elif channel == "teams":
                        result = self.send_teams_message(
                            config.get("teams_webhook"),
                            formatted_message
                        )
                    elif channel == "pagerduty":
                        result = self.create_pagerduty_incident(
                            config.get("pagerduty_key"),
                            f"Alert: {alert_type}",
                            formatted_message
                        )
                    elif channel == "dashboard":
                        result = self.send_dashboard_notification(
                            config.get("dashboard_users", []),
                            formatted_message,
                            severity
                        )
                    else:
                        result = {"status": "unknown_channel"}

                    results[channel] = result
                    self.log_alert(alert_id, channel, result.get("status", "unknown"), datetime.utcnow())
                except Exception as e:
                    self.logger.exception("Failed to send alert via %s", channel)
                    results[channel] = {"status": "error", "error": str(e)}
                    self.log_alert(alert_id, channel, "error", datetime.utcnow())

            return {
                "alert_id": alert_id,
                "alert_type": alert_type,
                "severity": severity,
                "channels": results,
                "status": "sent" if any(r.get("status") == "sent" for r in results.values()) else "failed"
            }
        except Exception as e:
            self.logger.exception("Alert sending failed")
            return {"status": "error", "error": str(e), "alert_id": alert_id}

    def get_alert_config(self, alert_type: str) -> Optional[Dict[str, Any]]:
        """Reads from ALERT_CONFIG table."""
        try:
            sql = "SELECT * FROM ALERT_CONFIG WHERE alert_type = %s AND active = TRUE"
            rows = self.db.query(sql, (alert_type,))
            return dict(rows[0]) if rows else None
        except Exception:
            self.logger.exception("Failed to get alert config for type %s", alert_type)
            return None

    def format_message(self, template: str, context: Dict[str, Any]) -> str:
        """Applies message template with variables."""
        try:
            return template.format(**context)
        except KeyError as e:
            self.logger.warning("Missing context key for template: %s", e)
            return template

    def send_email(self, recipients: List[str], subject: str, body: str, attachments: List[str] = None) -> Dict[str, Any]:
        """Sends email via SMTP."""
        if not self.smtp_config:
            return {"status": "no_smtp_config"}

        try:
            msg = MIMEMultipart()
            msg['From'] = self.smtp_config.get('from_email')
            msg['To'] = ', '.join(recipients)
            msg['Subject'] = subject
            msg.attach(MIMEText(body, 'html'))

            # Attachments would be added here

            server = smtplib.SMTP(self.smtp_config.get('server'), self.smtp_config.get('port', 587))
            server.starttls()
            server.login(self.smtp_config.get('username'), self.smtp_config.get('password'))
            server.sendmail(msg['From'], recipients, msg.as_string())
            server.quit()

            return {"status": "sent"}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def send_slack_message(self, webhook_url: str, message: str, channel: Optional[str] = None) -> Dict[str, Any]:
        """Posts to Slack."""
        if not REQUESTS_AVAILABLE or not webhook_url:
            return {"status": "not_available"}

        try:
            payload = {"text": message}
            if channel:
                payload["channel"] = channel

            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            return {"status": "sent", "response": response.text}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def send_teams_message(self, webhook_url: str, message: str) -> Dict[str, Any]:
        """Posts to Teams."""
        if not REQUESTS_AVAILABLE or not webhook_url:
            return {"status": "not_available"}

        try:
            payload = {
                "@type": "MessageCard",
                "@context": "http://schema.org/extensions",
                "summary": "Alert",
                "text": message
            }
            response = requests.post(webhook_url, json=payload, timeout=10)
            response.raise_for_status()
            return {"status": "sent", "response": response.text}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def create_pagerduty_incident(self, routing_key: str, title: str, details: str) -> Dict[str, Any]:
        """Creates PagerDuty incident."""
        if not REQUESTS_AVAILABLE or not routing_key:
            return {"status": "not_available"}

        try:
            payload = {
                "routing_key": routing_key,
                "event_action": "trigger",
                "payload": {
                    "summary": title,
                    "source": "ETL Framework",
                    "severity": "error",
                    "component": "pipeline",
                    "details": details
                }
            }
            response = requests.post(
                "https://events.pagerduty.com/v2/enqueue",
                json=payload,
                timeout=10
            )
            response.raise_for_status()
            return {"status": "sent", "incident_key": response.json().get("dedup_key")}
        except Exception as e:
            return {"status": "error", "error": str(e)}

    def send_dashboard_notification(self, user_ids: List[str], message: str, severity: str) -> Dict[str, Any]:
        """In-app notification."""
        # In a real system, this would send to a dashboard service
        # For now, just log it
        self.logger.info("Dashboard notification: %s to users %s", message, user_ids)
        return {"status": "sent"}

    def check_rate_limit(self, alert_type: str, time_window: int = 300) -> bool:
        """Prevents alert flooding."""
        now = datetime.utcnow()
        key = alert_type

        if key not in self.rate_limits:
            self.rate_limits[key] = []

        # Clean old entries
        self.rate_limits[key] = [t for t in self.rate_limits[key] if now - t < timedelta(seconds=time_window)]

        # Check if under limit (e.g., max 5 per window)
        if len(self.rate_limits[key]) >= 5:
            return False

        self.rate_limits[key].append(now)
        return True

    def log_alert(self, alert_id: str, channel: str, status: str, timestamp: datetime) -> None:
        """Tracks alert delivery."""
        try:
            sql = """
                INSERT INTO ALERT_LOG (log_id, alert_id, channel, status, timestamp)
                VALUES (%s, %s, %s, %s, %s)
            """
            log_id = str(uuid.uuid4())
            params = (log_id, alert_id, channel, status, timestamp)
            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()
        except Exception:
            self.db.rollback()
            self.logger.exception("Failed to log alert")

    def escalate_alert(self, original_alert: Dict[str, Any], escalation_level: int) -> None:
        """Escalates unacknowledged alerts."""
        # Check if alert is acknowledged
        alert_id = original_alert.get("alert_id")
        acknowledged = self._check_acknowledged(alert_id)
        if acknowledged:
            return

        # Get escalation config
        config = self.get_alert_config(original_alert.get("alert_type", ""))
        escalation = config.get("escalation", {}).get(str(escalation_level))
        if not escalation:
            return

        # Send escalated alert
        escalated_context = original_alert.get("context", {}).copy()
        escalated_context["escalation_level"] = escalation_level
        self.send_alert(
            f"{original_alert.get('alert_type')}_escalation",
            escalation.get("severity", "HIGH"),
            f"ESCALATION: {original_alert.get('message')}",
            escalated_context
        )

    def _check_acknowledged(self, alert_id: str) -> bool:
        """Check if alert is acknowledged."""
        try:
            sql = "SELECT acknowledged FROM ALERT_LOG WHERE alert_id = %s AND acknowledged = TRUE LIMIT 1"
            rows = self.db.query(sql, (alert_id,))
            return len(rows) > 0
        except Exception:
            return False

    def _get_channels_for_severity(self, severity: str, config: Dict[str, Any]) -> List[str]:
        """Get channels based on severity."""
        severity = severity.upper()
        if severity == "CRITICAL":
            return ["pagerduty", "email", "slack"]
        elif severity == "HIGH":
            return ["email", "slack"]
        elif severity == "MEDIUM":
            return ["slack"]
        elif severity == "LOW":
            return ["dashboard"]
        else:  # INFO
            return []

    def format_sla_violation_alert(self, sla_check: Dict[str, Any]) -> str:
        """Creates SLA-specific message."""
        return f"""
        SLA Violation Alert:
        Pipeline: {sla_check.get('pipeline_id')}
        SLA Type: {sla_check.get('sla_type')}
        Actual: {sla_check.get('actual')}
        Threshold: {sla_check.get('threshold')}
        Deviation: {sla_check.get('deviation')}%
        Status: {sla_check.get('status')}
        """

    def format_dq_failure_alert(self, dq_results: Dict[str, Any]) -> str:
        """Creates DQ-specific message."""
        return f"""
        Data Quality Failure Alert:
        Run ID: {dq_results.get('dq_run_id')}
        Quality Score: {dq_results.get('quality_score')}%
        Issues: {json.dumps(dq_results.get('issues', {}))}
        """

    def format_pipeline_failure_alert(self, error_log: Dict[str, Any]) -> str:
        """Creates failure-specific message."""
        return f"""
        Pipeline Failure Alert:
        Pipeline: {error_log.get('pipeline_id')}
        Run ID: {error_log.get('run_id')}
        Error: {error_log.get('error_message')}
        Trace: {error_log.get('error_trace')}
        """