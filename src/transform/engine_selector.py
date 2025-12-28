import logging
import psutil
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

# Assume engine classes are defined elsewhere; import them
try:
    from .pandas_engine import PandasEngine
except ImportError:
    PandasEngine = None

try:
    from .polars_engine import PolarsEngine
except ImportError:
    PolarsEngine = None

try:
    from .dask_engine import DaskEngine
except ImportError:
    DaskEngine = None

try:
    from .sql_engine import SQLEngine
except ImportError:
    SQLEngine = None

logger = logging.getLogger("EngineSelector")


class EngineSelector:
    """
    EngineSelector automatically selects the optimal processing engine based on data characteristics,
    system resources, and transformation requirements. Supports Pandas, Polars, Dask, and SQL engines.
    """

    def __init__(self, db_connection: Any, config_loader: Any):
        self.db = db_connection
        self.config_loader = config_loader
        self.logger = logging.getLogger(self.__class__.__name__)

        # Thresholds (can be configurable)
        self.pandas_threshold = 1 * 1024 * 1024 * 1024  # 1GB
        self.polars_threshold = 10 * 1024 * 1024 * 1024  # 10GB

    def select_engine(
        self,
        data_size: int,
        transformation_config: Dict[str, Any],
        available_memory: Optional[int] = None,
        run_id: Optional[str] = None,
    ) -> Tuple[str, Any]:
        """
        Main selection logic based on data size, memory, and transformation complexity.
        Returns (engine_name, engine_instance)
        """
        if available_memory is None:
            available_memory = self.check_available_memory()

        complexity = self.analyze_transformation_complexity(transformation_config)
        memory_required = self.estimate_memory_required(data_size, transformation_config)

        # Check if SQL pushdown is possible
        if self.can_use_sql_pushdown(transformation_config):
            sql_config = self.get_engine_config("sql")
            if sql_config and sql_config.get("enabled", True):
                engine_instance = self.get_sql_engine(sql_config.get("connection"))
                self.log_engine_selection(run_id, "sql", "SQL pushdown possible", data_size)
                return "sql", engine_instance

        # Select based on data size and memory
        if data_size < self.pandas_threshold and memory_required < available_memory:
            engine_name = "pandas"
        elif data_size < self.polars_threshold and memory_required < available_memory * 2:
            engine_name = "polars"
        else:
            engine_name = "dask"

        # Check if engine is available
        engine_config = self.get_engine_config(engine_name)
        if not engine_config or not engine_config.get("enabled", True):
            fallback = self.fallback_engine(engine_name, "Engine not enabled")
            engine_name = fallback[0]
            engine_config = self.get_engine_config(engine_name)

        # Get engine instance
        if engine_name == "pandas":
            engine_instance = self.get_pandas_engine()
        elif engine_name == "polars":
            engine_instance = self.get_polars_engine()
        elif engine_name == "dask":
            cluster_config = engine_config.get("cluster", {})
            engine_instance = self.get_dask_engine(cluster_config)
        else:
            raise ValueError(f"No suitable engine found, tried {engine_name}")

        reason = f"Data size: {data_size} bytes, Memory req: {memory_required}, Available: {available_memory}"
        self.log_engine_selection(run_id, engine_name, reason, data_size)
        return engine_name, engine_instance

    def get_engine_config(self, engine_name: str) -> Optional[Dict[str, Any]]:
        """Reads engine configuration from ENGINE_CONFIG table."""
        try:
            # Assume config_loader has a method to get engine config
            return self.config_loader.get_engine_config(engine_name)
        except Exception:
            self.logger.exception("Failed to load engine config for %s", engine_name)
            return None

    def check_available_memory(self) -> int:
        """Checks system available memory in bytes."""
        return psutil.virtual_memory().available

    def analyze_transformation_complexity(self, transform_config: Dict[str, Any]) -> str:
        """Analyzes transformation complexity: simple, medium, complex."""
        steps = transform_config.get("steps", [])
        if not steps:
            return "simple"

        complex_ops = {"join", "window", "pivot", "customudf", "sqltransform"}
        has_complex = any(step.get("type", "").lower() in complex_ops for step in steps)

        if has_complex or len(steps) > 5:
            return "complex"
        elif len(steps) > 2:
            return "medium"
        else:
            return "simple"

    def can_use_sql_pushdown(self, transform_config: Dict[str, Any]) -> bool:
        """Checks if transformations can be pushed down to SQL."""
        steps = transform_config.get("steps", [])
        sql_supported = {"filter", "map", "aggregate", "join"}

        for step in steps:
            if step.get("type", "").lower() not in sql_supported:
                return False
        return True

    def estimate_memory_required(self, data_size: int, transform_config: Dict[str, Any]) -> int:
        """Estimates memory required based on data size and transformations."""
        # Rough estimate: data size * expansion factor
        expansion = 2.0  # default
        complexity = self.analyze_transformation_complexity(transform_config)
        if complexity == "complex":
            expansion = 4.0
        elif complexity == "medium":
            expansion = 3.0

        return int(data_size * expansion)

    def get_pandas_engine(self) -> Any:
        """Returns PandasEngine instance."""
        if PandasEngine is None:
            raise ImportError("PandasEngine not available")
        return PandasEngine()

    def get_polars_engine(self) -> Any:
        """Returns PolarsEngine instance."""
        if PolarsEngine is None:
            raise ImportError("PolarsEngine not available")
        return PolarsEngine()

    def get_dask_engine(self, cluster_config: Dict[str, Any]) -> Any:
        """Returns DaskEngine instance with cluster setup."""
        if DaskEngine is None:
            raise ImportError("DaskEngine not available")
        return DaskEngine(cluster_config)

    def get_sql_engine(self, connection: Any) -> Any:
        """Returns SQLEngine instance."""
        if SQLEngine is None:
            raise ImportError("SQLEngine not available")
        return SQLEngine(connection)

    def fallback_engine(self, primary_engine: str, error: str) -> Tuple[str, Any]:
        """Selects fallback engine if primary fails."""
        fallbacks = {
            "pandas": ["polars", "dask"],
            "polars": ["pandas", "dask"],
            "dask": ["polars", "pandas"],
            "sql": ["pandas", "polars"],
        }

        for alt in fallbacks.get(primary_engine, ["pandas"]):
            config = self.get_engine_config(alt)
            if config and config.get("enabled", True):
                if alt == "pandas":
                    return alt, self.get_pandas_engine()
                elif alt == "polars":
                    return alt, self.get_polars_engine()
                elif alt == "dask":
                    return alt, self.get_dask_engine(config.get("cluster", {}))

        raise ValueError(f"No fallback engine available for {primary_engine}: {error}")

    def log_engine_selection(self, run_id: Optional[str], selected_engine: str, reason: str, data_size: int) -> None:
        """Logs engine selection decision."""
        try:
            event_id = str(uuid.uuid4())
            sql = """
                INSERT INTO ENGINE_SELECTION_LOG (event_id, run_id, selected_engine, reason, data_size_bytes, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            params = (event_id, run_id, selected_engine, reason, data_size, datetime.utcnow())
            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()
            self.logger.info("Selected engine: %s for run %s (%s)", selected_engine, run_id, reason)
        except Exception:
            try:
                self.db.rollback()
            except Exception:
                pass
            self.logger.exception("Failed to log engine selection")

    def auto_tune_engine(self, run_id: str, performance_metrics: Dict[str, Any]) -> None:
        """Auto-tunes engine selection based on historical performance."""
        # Simple auto-tuning: adjust thresholds based on past runs
        # This is a placeholder; in practice, analyze metrics and update thresholds
        try:
            # Query past selections and performance
            sql = """
                SELECT selected_engine, data_size_bytes, duration_seconds
                FROM ENGINE_SELECTION_LOG
                JOIN TRANSFORM_LOG ON ENGINE_SELECTION_LOG.run_id = TRANSFORM_LOG.run_id
                WHERE created_at > NOW() - INTERVAL '30 days'
            """
            rows = self.db.query(sql)
            # Analyze and adjust thresholds if needed
            # For now, just log
            self.logger.info("Auto-tuning based on %d historical runs", len(rows))
        except Exception:
            self.logger.exception("Auto-tuning failed")