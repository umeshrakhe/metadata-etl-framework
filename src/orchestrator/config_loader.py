import logging
import json
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger("ConfigLoader")


class ConfigLoader:
    """
    ConfigLoader loads and validates pipeline configurations from ETL_METADATA schema.
    Supports caching, versioning, and dependency validation.
    """

    def __init__(self, db_connection: Any, cache_ttl: int = 300):
        self.db = db_connection
        self.cache = {}  # Simple in-memory cache
        self.cache_ttl = cache_ttl  # seconds
        self.logger = logging.getLogger(self.__class__.__name__)

    def load_complete_configuration(self, pipeline_id: str) -> Dict[str, Any]:
        """Loads entire pipeline configuration."""
        cache_key = f"pipeline_{pipeline_id}"
        cached = self._get_cached(cache_key)
        if cached:
            return cached

        try:
            config = {
                "pipeline": self.load_pipeline_config(pipeline_id),
                "connections": {},
                "sources": [],
                "targets": [],
                "transformations": [],
                "dq_profile": {},
                "anomaly_config": {},
                "sla_definitions": [],
                "alert_config": {},
            }

            if not config["pipeline"]:
                raise ValueError(f"Pipeline {pipeline_id} not found")

            # Load connections
            conn_ids = set()
            sources = self.load_source_config(pipeline_id)
            for src in sources:
                conn_ids.add(src.get("connection_id"))
            targets = self.load_target_config(pipeline_id)
            for tgt in targets:
                conn_ids.add(tgt.get("connection_id"))
            config["connections"] = self.load_connections(list(conn_ids))

            config["sources"] = sources
            config["targets"] = targets
            config["transformations"] = self.load_transformations(pipeline_id)

            # Load optional configs
            profile_id = config["pipeline"].get("dq_profile_id")
            if profile_id:
                config["dq_profile"] = self.load_dq_profile(profile_id)

            config["anomaly_config"] = self.load_anomaly_config(pipeline_id)
            config["sla_definitions"] = self.load_sla_definitions(pipeline_id)
            config["alert_config"] = self.load_alert_config(pipeline_id)
            config["engine_config"] = self.load_engine_config()

            # Validate
            self.validate_configuration(config)
            self.validate_dependencies(config)

            self.cache_configuration(pipeline_id, config)
            return config
        except Exception as e:
            self.logger.exception("Failed to load complete configuration for pipeline %s", pipeline_id)
            raise

    def load_pipeline_config(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """Reads from PIPELINES table."""
        try:
            sql = "SELECT * FROM PIPELINES WHERE pipeline_id = %s AND active = TRUE"
            rows = self.db.query(sql, (pipeline_id,))
            if rows:
                row = dict(rows[0])
                # Parse JSON fields
                for field in ["parameters", "tags"]:
                    if row.get(field):
                        row[field] = self.parse_json_parameters(row[field])
                return row
            return None
        except Exception:
            self.logger.exception("Failed to load pipeline config for %s", pipeline_id)
            return None

    def load_connections(self, conn_ids: List[str]) -> Dict[str, Dict[str, Any]]:
        """Reads from CONNECTIONS table."""
        if not conn_ids:
            return {}

        try:
            placeholders = ",".join(["%s"] * len(conn_ids))
            sql = f"SELECT * FROM CONNECTIONS WHERE connection_id IN ({placeholders}) AND active = TRUE"
            rows = self.db.query(sql, tuple(conn_ids))
            connections = {}
            for row in rows:
                conn = dict(row)
                # Parse connection config
                if conn.get("connection_config"):
                    conn["connection_config"] = self.parse_json_parameters(conn["connection_config"])
                connections[conn["connection_id"]] = conn
            return connections
        except Exception:
            self.logger.exception("Failed to load connections")
            return {}

    def load_source_config(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """Reads from SOURCE_CONFIG table."""
        try:
            sql = "SELECT * FROM SOURCE_CONFIG WHERE pipeline_id = %s ORDER BY source_order"
            rows = self.db.query(sql, (pipeline_id,))
            sources = []
            for row in rows:
                src = dict(row)
                if src.get("query_params"):
                    src["query_params"] = self.parse_json_parameters(src["query_params"])
                if src.get("options"):
                    src["options"] = self.parse_json_parameters(src["options"])
                sources.append(src)
            return sources
        except Exception:
            self.logger.exception("Failed to load source config for pipeline %s", pipeline_id)
            return []

    def load_target_config(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """Reads from TARGET_CONFIG table."""
        try:
            sql = "SELECT * FROM TARGET_CONFIG WHERE pipeline_id = %s ORDER BY target_order"
            rows = self.db.query(sql, (pipeline_id,))
            targets = []
            for row in rows:
                tgt = dict(row)
                if tgt.get("options"):
                    tgt["options"] = self.parse_json_parameters(tgt["options"])
                targets.append(tgt)
            return targets
        except Exception:
            self.logger.exception("Failed to load target config for pipeline %s", pipeline_id)
            return []

    def load_transformations(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """Reads from TRANSFORMATIONS and TRANSFORM_STEPS tables."""
        try:
            sql = """
                SELECT t.*, ts.step_order, ts.step_type, ts.step_config
                FROM TRANSFORMATIONS t
                LEFT JOIN TRANSFORM_STEPS ts ON t.transformation_id = ts.transformation_id
                WHERE t.pipeline_id = %s AND t.active = TRUE
                ORDER BY t.transformation_order, ts.step_order
            """
            rows = self.db.query(sql, (pipeline_id,))
            transformations = {}
            for row in rows:
                trans_id = row["transformation_id"]
                if trans_id not in transformations:
                    transformations[trans_id] = {
                        "id": trans_id,
                        "name": row["transformation_name"],
                        "type": row["transformation_type"],
                        "pipeline_id": row["pipeline_id"],
                        "active": row["active"],
                        "steps": [],
                    }
                if row.get("step_type"):
                    step_config = self.parse_json_parameters(row["step_config"]) if row["step_config"] else {}
                    transformations[trans_id]["steps"].append({
                        "order": row["step_order"],
                        "type": row["step_type"],
                        "config": step_config,
                    })
            return list(transformations.values())
        except Exception:
            self.logger.exception("Failed to load transformations for pipeline %s", pipeline_id)
            return []

    def load_dq_profile(self, profile_id: str) -> Dict[str, Any]:
        """Reads from DQ_PROFILES and DQ_RULES tables."""
        try:
            # Load profile
            sql_profile = "SELECT * FROM DQ_PROFILES WHERE profile_id = %s"
            profile_rows = self.db.query(sql_profile, (profile_id,))
            if not profile_rows:
                return {}

            profile = dict(profile_rows[0])
            profile["rules"] = {}

            # Load rules
            sql_rules = "SELECT * FROM DQ_RULES WHERE profile_id = %s AND active = TRUE"
            rule_rows = self.db.query(sql_rules, (profile_id,))
            for row in rule_rows:
                rule_type = row["rule_type"]
                if rule_type not in profile["rules"]:
                    profile["rules"][rule_type] = []
                rule = dict(row)
                if rule.get("rule_config"):
                    rule["rule_config"] = self.parse_json_parameters(rule["rule_config"])
                profile["rules"][rule_type].append(rule)

            return profile
        except Exception:
            self.logger.exception("Failed to load DQ profile %s", profile_id)
            return {}

    def load_anomaly_config(self, pipeline_id: str) -> Dict[str, Any]:
        """Reads from ANOMALY_CONFIG table."""
        try:
            sql = "SELECT * FROM ANOMALY_CONFIG WHERE pipeline_id = %s AND active = TRUE"
            rows = self.db.query(sql, (pipeline_id,))
            if rows:
                config = dict(rows[0])
                if config.get("config"):
                    config["config"] = self.parse_json_parameters(config["config"])
                return config
            return {}
        except Exception:
            self.logger.exception("Failed to load anomaly config for pipeline %s", pipeline_id)
            return {}

    def load_sla_definitions(self, pipeline_id: str) -> List[Dict[str, Any]]:
        """Reads from SLA_DEFINITIONS table."""
        try:
            sql = "SELECT * FROM SLA_DEFINITIONS WHERE pipeline_id = %s AND active = TRUE"
            rows = self.db.query(sql, (pipeline_id,))
            slas = []
            for row in rows:
                sla = dict(row)
                if sla.get("threshold"):
                    sla["threshold"] = self.parse_json_parameters(sla["threshold"])
                slas.append(sla)
            return slas
        except Exception:
            self.logger.exception("Failed to load SLA definitions for pipeline %s", pipeline_id)
            return []

    def load_alert_config(self, pipeline_id: str) -> Dict[str, Any]:
        """Reads from ALERT_CONFIG table."""
        try:
            sql = "SELECT * FROM ALERT_CONFIG WHERE pipeline_id = %s AND active = TRUE"
            rows = self.db.query(sql, (pipeline_id,))
            if rows:
                config = dict(rows[0])
                if config.get("config"):
                    config["config"] = self.parse_json_parameters(config["config"])
                return config
            return {}
        except Exception:
            self.logger.exception("Failed to load alert config for pipeline %s", pipeline_id)
            return {}

    def load_engine_config(self) -> Dict[str, Dict[str, Any]]:
        """Reads from ENGINE_CONFIG table."""
        try:
            sql = "SELECT * FROM ENGINE_CONFIG WHERE active = TRUE"
            rows = self.db.query(sql, ())
            configs = {}
            for row in rows:
                engine = row["engine_name"]
                configs[engine] = dict(row)
                if configs[engine].get("config"):
                    configs[engine]["config"] = self.parse_json_parameters(configs[engine]["config"])
            return configs
        except Exception:
            self.logger.exception("Failed to load engine config")
            return {}

    def validate_configuration(self, config: Dict[str, Any]) -> bool:
        """Validates all required fields present."""
        required_sections = ["pipeline", "connections", "sources", "targets", "transformations"]
        missing = [s for s in required_sections if not config.get(s)]
        if missing:
            raise ValueError(f"Configuration validation failed. Missing sections: {missing}")

        pipeline = config["pipeline"]
        if not pipeline.get("pipeline_id") or not pipeline.get("pipeline_name"):
            raise ValueError("Pipeline must have id and name")

        # Validate sources and targets have connections
        conn_ids = set(config["connections"].keys())
        for src in config["sources"]:
            if src.get("connection_id") not in conn_ids:
                raise ValueError(f"Source {src.get('source_id')} references missing connection {src.get('connection_id')}")

        for tgt in config["targets"]:
            if tgt.get("connection_id") not in conn_ids:
                raise ValueError(f"Target {tgt.get('target_id')} references missing connection {tgt.get('connection_id')}")

        self.logger.info("Configuration validation passed for pipeline %s", pipeline.get("pipeline_id"))
        return True

    def validate_dependencies(self, config: Dict[str, Any]) -> bool:
        """Checks referenced records exist."""
        # Additional dependency checks can be added here
        # For now, basic validation is in validate_configuration
        return True

    def parse_json_parameters(self, json_string: str) -> Any:
        """Safely parses JSON parameters."""
        try:
            return json.loads(json_string)
        except json.JSONDecodeError as e:
            self.logger.warning("Failed to parse JSON: %s", e)
            return {}

    def cache_configuration(self, pipeline_id: str, config: Dict[str, Any]) -> None:
        """Stores in cache."""
        cache_key = f"pipeline_{pipeline_id}"
        self.cache[cache_key] = {
            "config": config,
            "timestamp": datetime.utcnow(),
            "hash": self._config_hash(config),
        }
        self.logger.debug("Cached configuration for pipeline %s", pipeline_id)

    def invalidate_cache(self, pipeline_id: str) -> None:
        """Clears cached config."""
        cache_key = f"pipeline_{pipeline_id}"
        if cache_key in self.cache:
            del self.cache[cache_key]
            self.logger.debug("Invalidated cache for pipeline %s", pipeline_id)

    def _get_cached(self, cache_key: str) -> Optional[Dict[str, Any]]:
        """Retrieves from cache if valid."""
        if cache_key in self.cache:
            entry = self.cache[cache_key]
            if datetime.utcnow() - entry["timestamp"] < timedelta(seconds=self.cache_ttl):
                # Check if config changed (simple hash check)
                current_hash = self._config_hash(entry["config"])
                if current_hash == entry["hash"]:
                    self.logger.debug("Returning cached config for %s", cache_key)
                    return entry["config"]
                else:
                    self.logger.debug("Config changed, invalidating cache for %s", cache_key)
                    del self.cache[cache_key]
        return None

    def _config_hash(self, config: Dict[str, Any]) -> str:
        """Generates hash of config for change detection."""
        config_str = json.dumps(config, sort_keys=True, default=str)
        return hashlib.md5(config_str.encode()).hexdigest()

    def get_pipeline_list(self) -> List[Dict[str, Any]]:
        """Gets list of active pipelines."""
        try:
            sql = "SELECT pipeline_id, pipeline_name, description FROM PIPELINES WHERE active = TRUE ORDER BY pipeline_name"
            rows = self.db.query(sql, ())
            return [dict(row) for row in rows]
        except Exception:
            self.logger.exception("Failed to get pipeline list")
            return []

    def get_connection_list(self) -> List[Dict[str, Any]]:
        """Gets list of active connections."""
        try:
            sql = "SELECT connection_id, connection_name, connection_type FROM CONNECTIONS WHERE active = TRUE ORDER BY connection_name"
            rows = self.db.query(sql, ())
            return [dict(row) for row in rows]
        except Exception:
            self.logger.exception("Failed to get connection list")
            return []