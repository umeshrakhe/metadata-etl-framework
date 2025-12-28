import logging
import uuid
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

# Optional imports
try:
    import pandera as pa
    from pandera import Column, DataFrameSchema, Check
    PANDERA_AVAILABLE = True
except ImportError:
    pa = None
    PANDERA_AVAILABLE = False

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    PANDAS_AVAILABLE = False

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.neighbors import LocalOutlierFactor
    from sklearn.svm import OneClassSVM
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    np = None
    NUMPY_AVAILABLE = False

logger = logging.getLogger("DQEngine")


class ProfileManager:
    """Generates data profiling statistics."""

    def profile_data(self, data: Any) -> Dict[str, Any]:
        if not PANDAS_AVAILABLE or not isinstance(data, pd.DataFrame):
            return {"error": "Pandas DataFrame required for profiling"}

        stats = {}
        for col in data.columns:
            col_stats = {
                "count": len(data),
                "null_count": data[col].isnull().sum(),
                "null_percentage": (data[col].isnull().sum() / len(data)) * 100,
                "unique_count": data[col].nunique(),
                "dtype": str(data[col].dtype),
            }
            if pd.api.types.is_numeric_dtype(data[col]):
                col_stats.update({
                    "min": data[col].min(),
                    "max": data[col].max(),
                    "mean": data[col].mean(),
                    "std": data[col].std(),
                    "median": data[col].median(),
                })
            stats[col] = col_stats
        return stats


class RuleEngine:
    """Executes rule-based validations."""

    def run_completeness_checks(self, data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        results = {}
        for rule in rules:
            col = rule.get("column")
            threshold = rule.get("threshold", 0)
            null_pct = (data[col].isnull().sum() / len(data)) * 100
            passed = null_pct <= threshold
            results[rule.get("rule_id")] = {
                "type": "completeness",
                "column": col,
                "null_percentage": null_pct,
                "threshold": threshold,
                "passed": passed,
            }
        return results

    def run_validity_checks(self, data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        results = {}
        for rule in rules:
            col = rule.get("column")
            rule_type = rule.get("type")
            passed = True
            if rule_type == "range":
                min_val, max_val = rule.get("min"), rule.get("max")
                passed = data[col].between(min_val, max_val).all()
            elif rule_type == "pattern":
                pattern = rule.get("pattern")
                passed = data[col].str.match(pattern).all()
            results[rule.get("rule_id")] = {
                "type": "validity",
                "column": col,
                "rule_type": rule_type,
                "passed": passed,
            }
        return results

    def run_uniqueness_checks(self, data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        results = {}
        for rule in rules:
            cols = rule.get("columns", [])
            dup_count = data.duplicated(subset=cols).sum()
            passed = dup_count == 0
            results[rule.get("rule_id")] = {
                "type": "uniqueness",
                "columns": cols,
                "duplicate_count": dup_count,
                "passed": passed,
            }
        return results

    def run_consistency_checks(self, data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        results = {}
        for rule in rules:
            # Example: cross-field check
            col1, col2 = rule.get("columns", [])
            condition = rule.get("condition", "")
            try:
                passed = eval(condition, {"data": data, "col1": data[col1], "col2": data[col2]})
                passed = passed.all() if hasattr(passed, 'all') else bool(passed)
            except Exception:
                passed = False
            results[rule.get("rule_id")] = {
                "type": "consistency",
                "columns": [col1, col2],
                "condition": condition,
                "passed": passed,
            }
        return results


class AnomalyManager:
    """Detects anomalies using various methods."""

    def detect_anomalies(self, data: Any, anomaly_config: Dict[str, Any]) -> Dict[str, Any]:
        method = anomaly_config.get("method", "zscore")
        results = {}

        if method == "zscore":
            results = self._zscore_anomaly(data, anomaly_config)
        elif method == "isolation_forest" and SKLEARN_AVAILABLE:
            results = self._isolation_forest_anomaly(data, anomaly_config)
        elif method == "iqr":
            results = self._iqr_anomaly(data, anomaly_config)
        else:
            results = {"error": f"Method {method} not supported"}

        return results

    def _zscore_anomaly(self, data: Any, config: Dict[str, Any]) -> Dict[str, Any]:
        col = config.get("column")
        threshold = config.get("threshold", 3)
        if not NUMPY_AVAILABLE:
            return {"error": "NumPy required for zscore"}
        z_scores = np.abs((data[col] - data[col].mean()) / data[col].std())
        anomalies = data[z_scores > threshold]
        return {
            "method": "zscore",
            "anomalies_count": len(anomalies),
            "anomalies": anomalies.head(10).to_dict() if PANDAS_AVAILABLE else [],
        }

    def _isolation_forest_anomaly(self, data: Any, config: Dict[str, Any]) -> Dict[str, Any]:
        cols = config.get("columns", [])
        contamination = config.get("contamination", 0.1)
        numeric_data = data[cols].select_dtypes(include=[np.number])
        scaler = StandardScaler()
        scaled = scaler.fit_transform(numeric_data)
        clf = IsolationForest(contamination=contamination, random_state=42)
        preds = clf.fit_predict(scaled)
        anomalies = data[preds == -1]
        return {
            "method": "isolation_forest",
            "anomalies_count": len(anomalies),
            "anomalies": anomalies.head(10).to_dict() if PANDAS_AVAILABLE else [],
        }

    def _iqr_anomaly(self, data: Any, config: Dict[str, Any]) -> Dict[str, Any]:
        col = config.get("column")
        Q1 = data[col].quantile(0.25)
        Q3 = data[col].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - 1.5 * IQR
        upper_bound = Q3 + 1.5 * IQR
        anomalies = data[(data[col] < lower_bound) | (data[col] > upper_bound)]
        return {
            "method": "iqr",
            "anomalies_count": len(anomalies),
            "anomalies": anomalies.head(10).to_dict() if PANDAS_AVAILABLE else [],
        }


class ResultsManager:
    """Stores and reports validation results."""

    def __init__(self, db_connection: Any):
        self.db = db_connection

    def store_dq_results(self, dq_run_id: str, results: Dict[str, Any]) -> None:
        try:
            # Store profiling stats
            if "profiling" in results:
                for col, stats in results["profiling"].items():
                    sql = """
                        INSERT INTO PROFILING_STATS (stat_id, dq_run_id, column_name, stats_json, created_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    stat_id = str(uuid.uuid4())
                    params = (stat_id, dq_run_id, col, json.dumps(stats), datetime.utcnow())
                    self.db.execute(sql, params)

            # Store check results
            for check_type, checks in results.items():
                if check_type in ["completeness", "validity", "uniqueness", "consistency"]:
                    table = f"{check_type.upper()}_CHECK"
                    for rule_id, res in checks.items():
                        sql = f"""
                            INSERT INTO {table} (check_id, dq_run_id, rule_id, result_json, passed, created_at)
                            VALUES (%s, %s, %s, %s, %s, %s)
                        """
                        check_id = str(uuid.uuid4())
                        params = (check_id, dq_run_id, rule_id, json.dumps(res), res.get("passed"), datetime.utcnow())
                        self.db.execute(sql, params)

            # Store anomalies
            if "anomalies" in results:
                for method, res in results["anomalies"].items():
                    sql = """
                        INSERT INTO ANOMALY_DETECTION (detection_id, dq_run_id, method, result_json, created_at)
                        VALUES (%s, %s, %s, %s, %s)
                    """
                    detection_id = str(uuid.uuid4())
                    params = (detection_id, dq_run_id, method, json.dumps(res), datetime.utcnow())
                    self.db.execute(sql, params)

            # Store quality score
            if "quality_score" in results:
                sql = """
                    INSERT INTO QUALITY_SCORES (score_id, dq_run_id, score, score_breakdown, created_at)
                    VALUES (%s, %s, %s, %s, %s)
                """
                score_id = str(uuid.uuid4())
                params = (score_id, dq_run_id, results["quality_score"], json.dumps(results.get("score_breakdown", {})), datetime.utcnow())
                self.db.execute(sql, params)

            self.db.commit()
            logger.info("Stored DQ results for run %s", dq_run_id)
        except Exception:
            self.db.rollback()
            logger.exception("Failed to store DQ results")

    def generate_dq_report(self, dq_run_id: str) -> Dict[str, Any]:
        # Generate summary report
        report = {"dq_run_id": dq_run_id, "summary": {}}
        # Query and aggregate results
        return report


class DQEngine:
    """
    DQEngine executes comprehensive data quality validation including profiling,
    rule-based checks, anomaly detection, and quality scoring.
    """

    def __init__(self, db_connection: Any, config_loader: Any):
        self.db = db_connection
        self.config_loader = config_loader
        self.profile_manager = ProfileManager()
        self.rule_engine = RuleEngine()
        self.anomaly_manager = AnomalyManager()
        self.results_manager = ResultsManager(db_connection)
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute_dq_validation(self, data: Any, profile_id: str, run_id: str) -> Dict[str, Any]:
        """Main execution method for data quality validation."""
        dq_run_id = str(uuid.uuid4())
        results = {"dq_run_id": dq_run_id}

        try:
            # Load DQ rules
            dq_rules = self.config_loader.get_dq_rules(profile_id)

            # Profiling
            results["profiling"] = self.profile_data(data)

            # Pandera validation
            if PANDERA_AVAILABLE:
                schema = self.create_pandera_schema(dq_rules)
                pandera_results = self.validate_with_pandera(data, schema)
                results["pandera_validation"] = pandera_results

            # Rule-based checks
            results["completeness"] = self.run_completeness_checks(data, dq_rules.get("completeness", []))
            results["validity"] = self.run_validity_checks(data, dq_rules.get("validity", []))
            results["uniqueness"] = self.run_uniqueness_checks(data, dq_rules.get("uniqueness", []))
            results["consistency"] = self.run_consistency_checks(data, dq_rules.get("consistency", []))

            # Anomaly detection
            anomaly_config = dq_rules.get("anomaly", {})
            results["anomalies"] = self.detect_anomalies(data, anomaly_config)

            # Quality score
            results["quality_score"], results["score_breakdown"] = self.calculate_quality_score(results)

            # Store results
            self.store_dq_results(dq_run_id, results)

            return results
        except Exception as e:
            self.logger.exception("DQ validation failed for run %s", run_id)
            return {"error": str(e), "dq_run_id": dq_run_id}

    def create_pandera_schema(self, dq_rules: Dict[str, Any]) -> Any:
        """Generates Pandera schema from DQ rules."""
        if not PANDERA_AVAILABLE:
            raise ImportError("Pandera not available")
        schema_dict = {}
        for rule in dq_rules.get("validity", []):
            col = rule.get("column")
            checks = []
            if rule.get("type") == "range":
                checks.append(Check.between(rule.get("min"), rule.get("max")))
            schema_dict[col] = Column(checks=checks)
        return DataFrameSchema(schema_dict)

    def validate_with_pandera(self, data: Any, schema: Any) -> Dict[str, Any]:
        """Runs Pandera validation."""
        try:
            validated = schema.validate(data)
            return {"passed": True, "errors": []}
        except pa.errors.SchemaError as e:
            return {"passed": False, "errors": str(e)}

    def profile_data(self, data: Any) -> Dict[str, Any]:
        return self.profile_manager.profile_data(data)

    def run_completeness_checks(self, data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self.rule_engine.run_completeness_checks(data, rules)

    def run_validity_checks(self, data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self.rule_engine.run_validity_checks(data, rules)

    def run_uniqueness_checks(self, data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self.rule_engine.run_uniqueness_checks(data, rules)

    def run_consistency_checks(self, data: Any, rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        return self.rule_engine.run_consistency_checks(data, rules)

    def detect_anomalies(self, data: Any, anomaly_config: Dict[str, Any]) -> Dict[str, Any]:
        return self.anomaly_manager.detect_anomalies(data, anomaly_config)

    def calculate_quality_score(self, validation_results: Dict[str, Any]) -> Tuple[float, Dict[str, Any]]:
        """Calculates overall quality score."""
        breakdown = {}
        total_checks = 0
        passed_checks = 0

        for check_type in ["completeness", "validity", "uniqueness", "consistency"]:
            if check_type in validation_results:
                checks = validation_results[check_type]
                total_checks += len(checks)
                passed_checks += sum(1 for c in checks.values() if c.get("passed"))
                breakdown[check_type] = {"passed": sum(1 for c in checks.values() if c.get("passed")), "total": len(checks)}

        score = (passed_checks / total_checks) * 100 if total_checks > 0 else 0
        breakdown["overall"] = {"passed": passed_checks, "total": total_checks}
        return score, breakdown

    def store_dq_results(self, dq_run_id: str, results: Dict[str, Any]) -> None:
        self.results_manager.store_dq_results(dq_run_id, results)

    def generate_dq_report(self, dq_run_id: str) -> Dict[str, Any]:
        return self.results_manager.generate_dq_report(dq_run_id)