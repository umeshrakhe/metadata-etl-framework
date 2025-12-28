import logging
import uuid
import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

# Optional imports
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    PANDAS_AVAILABLE = False

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    np = None
    NUMPY_AVAILABLE = False

try:
    from sklearn.ensemble import IsolationForest
    from sklearn.neighbors import LocalOutlierFactor
    from sklearn.svm import OneClassSVM
    from sklearn.cluster import DBSCAN
    from sklearn.preprocessing import StandardScaler
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False

try:
    from statsmodels.tsa.arima.model import ARIMA
    from statsmodels.tsa.seasonal import STL
    STATS_AVAILABLE = True
except ImportError:
    STATS_AVAILABLE = False

try:
    from prophet import Prophet
    PROPHET_AVAILABLE = True
except ImportError:
    PROPHET_AVAILABLE = False

try:
    import ruptures as rpt
    RUPTURES_AVAILABLE = True
except ImportError:
    rpt = None
    RUPTURES_AVAILABLE = False

logger = logging.getLogger("AnomalyManager")


class AnomalyManager:
    """
    AnomalyManager detects anomalies using statistical, ML-based, and time-series methods.
    Supports ensemble detection and confidence scoring.
    """

    def __init__(self, db_connection: Any):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)

    def detect_all_anomalies(self, data: Any, anomaly_config: Dict[str, Any]) -> Dict[str, Any]:
        """Runs configured detection methods and combines results."""
        methods = anomaly_config.get("methods", [])
        results = {}

        for method_config in methods:
            method_name = method_config.get("name")
            params = method_config.get("params", {})
            try:
                if method_name == "zscore":
                    result = self.zscore_detection(data, **params)
                elif method_name == "iqr":
                    result = self.iqr_detection(data, **params)
                elif method_name == "moving_average":
                    result = self.moving_average_detection(data, **params)
                elif method_name == "dbscan":
                    result = self.dbscan_detection(data, **params)
                elif method_name == "isolation_forest":
                    result = self.isolation_forest_detection(data, **params)
                elif method_name == "lof":
                    result = self.lof_detection(data, **params)
                elif method_name == "ocsvm":
                    result = self.ocsvm_detection(data, **params)
                elif method_name == "arima_residual":
                    result = self.arima_residual_detection(data, **params)
                elif method_name == "stl_decomposition":
                    result = self.stl_decomposition_detection(data, **params)
                elif method_name == "prophet":
                    result = self.prophet_detection(data, **params)
                elif method_name == "changepoint":
                    result = self.changepoint_detection(data, **params)
                else:
                    continue
                results[method_name] = result
            except Exception as e:
                self.logger.exception("Failed to run anomaly detection method %s", method_name)
                results[method_name] = {"error": str(e)}

        # Ensemble if multiple methods
        if len(results) > 1:
            results["ensemble"] = self.ensemble_vote(results)

        return results

    # Statistical Methods
    def zscore_detection(self, data: Any, column: str, threshold: float = 3.0) -> Dict[str, Any]:
        """Z-Score outlier detection."""
        if not NUMPY_AVAILABLE or not PANDAS_AVAILABLE:
            return {"error": "NumPy and Pandas required"}
        values = data[column].dropna()
        z_scores = np.abs((values - values.mean()) / values.std())
        detected_indices = data.index[z_scores > threshold].tolist()
        confidence_scores = z_scores[z_scores > threshold].tolist()
        return {
            "detected_indices": detected_indices,
            "confidence_scores": confidence_scores,
            "method_name": "zscore",
            "count": len(detected_indices),
        }

    def iqr_detection(self, data: Any, column: str, multiplier: float = 1.5) -> Dict[str, Any]:
        """Interquartile Range method."""
        if not PANDAS_AVAILABLE:
            return {"error": "Pandas required"}
        Q1 = data[column].quantile(0.25)
        Q3 = data[column].quantile(0.75)
        IQR = Q3 - Q1
        lower_bound = Q1 - multiplier * IQR
        upper_bound = Q3 + multiplier * IQR
        outliers = data[(data[column] < lower_bound) | (data[column] > upper_bound)]
        detected_indices = outliers.index.tolist()
        # Confidence based on distance from bounds
        distances = np.abs(outliers[column] - (Q1 + Q3) / 2) / (IQR / 2)
        confidence_scores = distances.tolist()
        return {
            "detected_indices": detected_indices,
            "confidence_scores": confidence_scores,
            "method_name": "iqr",
            "count": len(detected_indices),
        }

    def moving_average_detection(self, data: Any, column: str, window: int = 7, threshold: float = 2.0) -> Dict[str, Any]:
        """Deviation from moving average."""
        if not PANDAS_AVAILABLE:
            return {"error": "Pandas required"}
        ma = data[column].rolling(window=window).mean()
        std = data[column].rolling(window=window).std()
        z_scores = np.abs((data[column] - ma) / std)
        detected_indices = data.index[z_scores > threshold].tolist()
        confidence_scores = z_scores[z_scores > threshold].tolist()
        return {
            "detected_indices": detected_indices,
            "confidence_scores": confidence_scores,
            "method_name": "moving_average",
            "count": len(detected_indices),
        }

    def dbscan_detection(self, data: Any, columns: List[str], eps: float = 0.5, min_samples: int = 5) -> Dict[str, Any]:
        """Density-based clustering."""
        if not SKLEARN_AVAILABLE or not PANDAS_AVAILABLE:
            return {"error": "Scikit-learn and Pandas required"}
        numeric_data = data[columns].select_dtypes(include=[np.number])
        scaler = StandardScaler()
        scaled = scaler.fit_transform(numeric_data)
        dbscan = DBSCAN(eps=eps, min_samples=min_samples)
        labels = dbscan.fit_predict(scaled)
        # Anomalies are points with label -1
        detected_indices = data.index[labels == -1].tolist()
        # Confidence based on distance to nearest core point (simplified)
        confidence_scores = [1.0] * len(detected_indices)  # Placeholder
        return {
            "detected_indices": detected_indices,
            "confidence_scores": confidence_scores,
            "method_name": "dbscan",
            "count": len(detected_indices),
        }

    # ML-Based Methods
    def isolation_forest_detection(self, data: Any, columns: List[str], contamination: float = 0.1) -> Dict[str, Any]:
        """Isolation Forest."""
        if not SKLEARN_AVAILABLE or not PANDAS_AVAILABLE:
            return {"error": "Scikit-learn and Pandas required"}
        numeric_data = data[columns].select_dtypes(include=[np.number])
        scaler = StandardScaler()
        scaled = scaler.fit_transform(numeric_data)
        clf = IsolationForest(contamination=contamination, random_state=42)
        preds = clf.fit_predict(scaled)
        scores = clf.decision_function(scaled)
        detected_indices = data.index[preds == -1].tolist()
        confidence_scores = (-scores[preds == -1]).tolist()  # Higher score means more anomalous
        return {
            "detected_indices": detected_indices,
            "confidence_scores": confidence_scores,
            "method_name": "isolation_forest",
            "count": len(detected_indices),
        }

    def lof_detection(self, data: Any, columns: List[str], n_neighbors: int = 20) -> Dict[str, Any]:
        """Local Outlier Factor."""
        if not SKLEARN_AVAILABLE or not PANDAS_AVAILABLE:
            return {"error": "Scikit-learn and Pandas required"}
        numeric_data = data[columns].select_dtypes(include=[np.number])
        scaler = StandardScaler()
        scaled = scaler.fit_transform(numeric_data)
        lof = LocalOutlierFactor(n_neighbors=n_neighbors)
        preds = lof.fit_predict(scaled)
        scores = -lof.negative_outlier_factor_
        detected_indices = data.index[preds == -1].tolist()
        confidence_scores = scores[preds == -1].tolist()
        return {
            "detected_indices": detected_indices,
            "confidence_scores": confidence_scores,
            "method_name": "lof",
            "count": len(detected_indices),
        }

    def ocsvm_detection(self, data: Any, columns: List[str], nu: float = 0.1) -> Dict[str, Any]:
        """One-Class SVM."""
        if not SKLEARN_AVAILABLE or not PANDAS_AVAILABLE:
            return {"error": "Scikit-learn and Pandas required"}
        numeric_data = data[columns].select_dtypes(include=[np.number])
        scaler = StandardScaler()
        scaled = scaler.fit_transform(numeric_data)
        clf = OneClassSVM(nu=nu)
        preds = clf.fit_predict(scaled)
        scores = clf.decision_function(scaled)
        detected_indices = data.index[preds == -1].tolist()
        confidence_scores = (-scores[preds == -1]).tolist()
        return {
            "detected_indices": detected_indices,
            "confidence_scores": confidence_scores,
            "method_name": "ocsvm",
            "count": len(detected_indices),
        }

    # Time-Series Methods
    def arima_residual_detection(self, data: Any, column: str, order: Tuple[int, int, int] = (1, 1, 1)) -> Dict[str, Any]:
        """ARIMA residual analysis."""
        if not STATS_AVAILABLE or not PANDAS_AVAILABLE:
            return {"error": "Statsmodels and Pandas required"}
        try:
            model = ARIMA(data[column], order=order)
            fitted = model.fit()
            residuals = fitted.resid
            # Anomalies based on residual z-scores
            z_scores = np.abs((residuals - residuals.mean()) / residuals.std())
            detected_indices = data.index[z_scores > 3].tolist()
            confidence_scores = z_scores[z_scores > 3].tolist()
            return {
                "detected_indices": detected_indices,
                "confidence_scores": confidence_scores,
                "method_name": "arima_residual",
                "count": len(detected_indices),
            }
        except Exception as e:
            return {"error": str(e)}

    def stl_decomposition_detection(self, data: Any, column: str, period: int = 7) -> Dict[str, Any]:
        """Seasonal-Trend-Loess decomposition."""
        if not STATS_AVAILABLE or not PANDAS_AVAILABLE:
            return {"error": "Statsmodels and Pandas required"}
        try:
            stl = STL(data[column], seasonal=period)
            result = stl.fit()
            residuals = result.resid
            z_scores = np.abs((residuals - residuals.mean()) / residuals.std())
            detected_indices = data.index[z_scores > 3].tolist()
            confidence_scores = z_scores[z_scores > 3].tolist()
            return {
                "detected_indices": detected_indices,
                "confidence_scores": confidence_scores,
                "method_name": "stl_decomposition",
                "count": len(detected_indices),
            }
        except Exception as e:
            return {"error": str(e)}

    def prophet_detection(self, data: Any, timestamp_col: str, value_col: str) -> Dict[str, Any]:
        """Facebook Prophet anomalies."""
        if not PROPHET_AVAILABLE or not PANDAS_AVAILABLE:
            return {"error": "Prophet and Pandas required"}
        try:
            df = data[[timestamp_col, value_col]].rename(columns={timestamp_col: 'ds', value_col: 'y'})
            model = Prophet()
            model.fit(df)
            forecast = model.predict(df)
            residuals = df['y'] - forecast['yhat']
            z_scores = np.abs((residuals - residuals.mean()) / residuals.std())
            detected_indices = data.index[z_scores > 3].tolist()
            confidence_scores = z_scores[z_scores > 3].tolist()
            return {
                "detected_indices": detected_indices,
                "confidence_scores": confidence_scores,
                "method_name": "prophet",
                "count": len(detected_indices),
            }
        except Exception as e:
            return {"error": str(e)}

    def changepoint_detection(self, data: Any, column: str, method: str = "pelt") -> Dict[str, Any]:
        """Change point detection."""
        if not RUPTURES_AVAILABLE or not PANDAS_AVAILABLE:
            return {"error": "Ruptures and Pandas required"}
        try:
            signal = data[column].values
            algo = rpt.Pelt(model="rbf").fit(signal)
            change_points = algo.predict(pen=10)
            # Anomalies around change points
            detected_indices = []
            for cp in change_points[:-1]:  # Last is end
                detected_indices.extend(range(max(0, cp-5), min(len(data), cp+6)))
            confidence_scores = [1.0] * len(detected_indices)  # Placeholder
            return {
                "detected_indices": list(set(detected_indices)),
                "confidence_scores": confidence_scores,
                "method_name": "changepoint",
                "count": len(set(detected_indices)),
            }
        except Exception as e:
            return {"error": str(e)}

    def calculate_confidence_score(self, results: Dict[str, Any]) -> Dict[str, Any]:
        """Computes detection confidence."""
        # Simple average of confidence scores
        confidences = {}
        for method, res in results.items():
            if "confidence_scores" in res:
                confidences[method] = np.mean(res["confidence_scores"]) if res["confidence_scores"] else 0
        return confidences

    def classify_severity(self, anomaly: Dict[str, Any], threshold: Dict[str, float]) -> str:
        """Assigns severity level."""
        confidence = anomaly.get("confidence", 0)
        if confidence > threshold.get("high", 0.8):
            return "high"
        elif confidence > threshold.get("medium", 0.5):
            return "medium"
        else:
            return "low"

    def ensemble_vote(self, detection_results: Dict[str, Any]) -> Dict[str, Any]:
        """Combines multiple detector results."""
        all_indices = []
        for res in detection_results.values():
            if "detected_indices" in res:
                all_indices.extend(res["detected_indices"])
        # Simple majority vote
        from collections import Counter
        index_counts = Counter(all_indices)
        ensemble_indices = [idx for idx, count in index_counts.items() if count > len(detection_results) // 2]
        confidence_scores = [index_counts[idx] / len(detection_results) for idx in ensemble_indices]
        return {
            "detected_indices": ensemble_indices,
            "confidence_scores": confidence_scores,
            "method_name": "ensemble",
            "count": len(ensemble_indices),
        }

    def filter_false_positives(self, anomalies: Dict[str, Any], rules: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Reduces false positive rate."""
        # Placeholder: apply rules to filter
        filtered_indices = anomalies.get("detected_indices", [])
        # Implement filtering logic based on rules
        return {
            "detected_indices": filtered_indices,
            "confidence_scores": anomalies.get("confidence_scores", []),
            "method_name": anomalies.get("method_name", "filtered"),
            "count": len(filtered_indices),
        }

    def store_anomaly_results(self, dq_run_id: str, anomalies: Dict[str, Any]) -> None:
        """Writes to ANOMALY_DETECTION table."""
        try:
            for method, res in anomalies.items():
                if "detected_indices" in res:
                    sql = """
                        INSERT INTO ANOMALY_DETECTION (detection_id, dq_run_id, method, detected_indices, confidence_scores, count, created_at)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """
                    detection_id = str(uuid.uuid4())
                    params = (
                        detection_id,
                        dq_run_id,
                        method,
                        json.dumps(res["detected_indices"]),
                        json.dumps(res.get("confidence_scores", [])),
                        res.get("count", 0),
                        datetime.utcnow(),
                    )
                    self.db.begin()
                    self.db.execute(sql, params)
                    self.db.commit()
            self.logger.info("Stored anomaly results for DQ run %s", dq_run_id)
        except Exception:
            self.db.rollback()
            self.logger.exception("Failed to store anomaly results")

    def get_anomaly_samples(self, data: Any, anomaly_indices: List[int], sample_size: int = 10) -> Any:
        """Extracts sample records."""
        if not PANDAS_AVAILABLE:
            return []
        sample_indices = anomaly_indices[:sample_size]
        return data.iloc[sample_indices]