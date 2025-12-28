import logging
import time
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Callable

# Optional imports for engines
try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    pd = None
    PANDAS_AVAILABLE = False

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    pl = None
    POLARS_AVAILABLE = False

try:
    import dask.dataframe as dd
    DASK_AVAILABLE = True
except ImportError:
    dd = None
    DASK_AVAILABLE = False

try:
    import duckdb
    DUCKDB_AVAILABLE = True
except ImportError:
    duckdb = None
    DUCKDB_AVAILABLE = False

logger = logging.getLogger("TransformEngine")


class TransformEngine:
    """
    TransformEngine executes transformation pipelines using configurable engines (Pandas/Polars/Dask).
    Supports various transformation types and logs execution details.
    """

    def __init__(self, db_connection: Any):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)

    def execute_transformations(
        self,
        data: Union[Dict[str, Any], Any],
        transform_config: Dict[str, Any],
        engine: str = "pandas",
        run_id: Optional[str] = None,
    ) -> Union[Dict[str, Any], Any]:
        """
        Main method to execute a sequence of transformations.
        data: dict of {source_id: dataframe} or single dataframe
        transform_config: dict with 'id' and 'steps' list
        engine: 'pandas', 'polars', 'dask'
        """
        transform_id = transform_config.get("id")
        steps = transform_config.get("steps", [])
        current_data = data

        start_time = time.time()
        rows_in = self._estimate_rows(current_data)

        try:
            for step in steps:
                step_start = time.time()
                current_data = self.apply_transform_step(current_data, step, engine)
                step_duration = time.time() - step_start
                self.logger.debug("Applied step %s in %.2fs", step.get("type"), step_duration)

            rows_out = self._estimate_rows(current_data)
            duration = time.time() - start_time
            self.log_transformation(run_id, transform_id, engine, rows_in, rows_out, duration)
            return current_data

        except Exception as e:
            self.logger.exception("Transformation failed for %s", transform_id)
            duration = time.time() - start_time
            self.log_transformation(run_id, transform_id, engine, rows_in, 0, duration, error=str(e))
            raise

    def apply_transform_step(self, data: Union[Dict[str, Any], Any], step_config: Dict[str, Any], engine: str) -> Union[Dict[str, Any], Any]:
        """
        Applies a single transformation step.
        """
        step_type = step_config.get("type", "").lower()
        params = step_config.get("params", {})

        if step_type == "filter":
            return self.filter_data(data, params.get("condition"), engine)
        elif step_type == "map":
            return self.map_columns(data, params.get("mapping"), engine)
        elif step_type == "typeconversion":
            return self.convert_types(data, params.get("type_mapping"), engine)
        elif step_type == "join":
            left_source = params.get("left_source")
            right_source = params.get("right_source")
            left_data = data.get(left_source) if isinstance(data, dict) else data
            right_data = data.get(right_source) if isinstance(data, dict) else None
            if right_data is None:
                raise ValueError(f"Right data source {right_source} not found")
            return self.join_data(left_data, right_data, params, engine)
        elif step_type == "aggregate":
            return self.aggregate_data(data, params.get("groupby_cols", []), params.get("agg_functions", {}), engine)
        elif step_type == "pivot":
            return self.pivot_data(data, params.get("index"), params.get("columns"), params.get("values"), engine)
        elif step_type == "window":
            return self.window_function(data, params, engine)
        elif step_type == "customudf":
            return self.execute_custom_udf(data, params.get("udf_code"), engine)
        elif step_type == "sqltransform":
            return self.execute_sql_transform(data, params.get("sql_query"), engine)
        else:
            raise NotImplementedError(f"Transformation type {step_type} not supported")

    def filter_data(self, data: Any, condition: str, engine: str) -> Any:
        """Filter rows based on condition."""
        if engine == "pandas":
            if not PANDAS_AVAILABLE:
                raise ImportError("Pandas not available")
            return data.query(condition) if isinstance(data, pd.DataFrame) else data
        elif engine == "polars":
            if not POLARS_AVAILABLE:
                raise ImportError("Polars not available")
            return data.filter(pl.sql_expr(condition)) if isinstance(data, pl.DataFrame) else data
        elif engine == "dask":
            if not DASK_AVAILABLE:
                raise ImportError("Dask not available")
            return data.query(condition) if isinstance(data, dd.DataFrame) else data
        else:
            raise NotImplementedError(f"Engine {engine} not supported for filter")

    def map_columns(self, data: Any, mapping: Dict[str, str], engine: str) -> Any:
        """Rename columns based on mapping dict."""
        if engine == "pandas":
            if not PANDAS_AVAILABLE:
                raise ImportError("Pandas not available")
            return data.rename(columns=mapping) if isinstance(data, pd.DataFrame) else data
        elif engine == "polars":
            if not POLARS_AVAILABLE:
                raise ImportError("Polars not available")
            return data.rename(mapping) if isinstance(data, pl.DataFrame) else data
        elif engine == "dask":
            if not DASK_AVAILABLE:
                raise ImportError("Dask not available")
            return data.rename(columns=mapping) if isinstance(data, dd.DataFrame) else data
        else:
            raise NotImplementedError(f"Engine {engine} not supported for map")

    def convert_types(self, data: Any, type_mapping: Dict[str, str], engine: str) -> Any:
        """Convert column types."""
        if engine == "pandas":
            if not PANDAS_AVAILABLE:
                raise ImportError("Pandas not available")
            for col, dtype in type_mapping.items():
                data[col] = data[col].astype(dtype)
            return data
        elif engine == "polars":
            if not POLARS_AVAILABLE:
                raise ImportError("Polars not available")
            for col, dtype in type_mapping.items():
                data = data.with_columns(pl.col(col).cast(getattr(pl, dtype, pl.Utf8)))
            return data
        elif engine == "dask":
            if not DASK_AVAILABLE:
                raise ImportError("Dask not available")
            for col, dtype in type_mapping.items():
                data[col] = data[col].astype(dtype)
            return data
        else:
            raise NotImplementedError(f"Engine {engine} not supported for type conversion")

    def join_data(self, left_data: Any, right_data: Any, join_config: Dict[str, Any], engine: str) -> Any:
        """Perform join operation."""
        how = join_config.get("how", "inner")
        left_on = join_config.get("left_on", [])
        right_on = join_config.get("right_on", [])

        if engine == "pandas":
            if not PANDAS_AVAILABLE:
                raise ImportError("Pandas not available")
            return left_data.merge(right_data, how=how, left_on=left_on, right_on=right_on)
        elif engine == "polars":
            if not POLARS_AVAILABLE:
                raise ImportError("Polars not available")
            return left_data.join(right_data, how=how, left_on=left_on, right_on=right_on)
        elif engine == "dask":
            if not DASK_AVAILABLE:
                raise ImportError("Dask not available")
            return left_data.merge(right_data, how=how, left_on=left_on, right_on=right_on)
        else:
            raise NotImplementedError(f"Engine {engine} not supported for join")

    def aggregate_data(self, data: Any, groupby_cols: List[str], agg_functions: Dict[str, str], engine: str) -> Any:
        """Perform aggregation."""
        if engine == "pandas":
            if not PANDAS_AVAILABLE:
                raise ImportError("Pandas not available")
            return data.groupby(groupby_cols).agg(agg_functions).reset_index()
        elif engine == "polars":
            if not POLARS_AVAILABLE:
                raise ImportError("Polars not available")
            agg_exprs = [getattr(pl.col(col), func)().alias(f"{col}_{func}") for col, func in agg_functions.items()]
            return data.group_by(groupby_cols).agg(agg_exprs)
        elif engine == "dask":
            if not DASK_AVAILABLE:
                raise ImportError("Dask not available")
            return data.groupby(groupby_cols).agg(agg_functions).reset_index()
        else:
            raise NotImplementedError(f"Engine {engine} not supported for aggregate")

    def pivot_data(self, data: Any, index: List[str], columns: str, values: List[str], engine: str) -> Any:
        """Perform pivot operation."""
        if engine == "pandas":
            if not PANDAS_AVAILABLE:
                raise ImportError("Pandas not available")
            return data.pivot_table(index=index, columns=columns, values=values, aggfunc='first').reset_index()
        elif engine == "polars":
            if not POLARS_AVAILABLE:
                raise ImportError("Polars not available")
            return data.pivot(index=index, columns=columns, values=values, aggregate_function='first')
        elif engine == "dask":
            if not DASK_AVAILABLE:
                raise ImportError("Dask not available")
            return data.pivot_table(index=index, columns=columns, values=values, aggfunc='first').reset_index()
        else:
            raise NotImplementedError(f"Engine {engine} not supported for pivot")

    def window_function(self, data: Any, window_config: Dict[str, Any], engine: str) -> Any:
        """Apply window functions."""
        func = window_config.get("function", "rank")
        partition_by = window_config.get("partition_by", [])
        order_by = window_config.get("order_by", [])

        if engine == "pandas":
            if not PANDAS_AVAILABLE:
                raise ImportError("Pandas not available")
            if func == "rank":
                data[window_config.get("output_col", "rank")] = data.groupby(partition_by)[order_by[0]].rank() if partition_by else data[order_by[0]].rank()
            # Simplified, add more as needed
            return data
        elif engine == "polars":
            if not POLARS_AVAILABLE:
                raise ImportError("Polars not available")
            if func == "rank":
                data = data.with_columns(pl.col(order_by[0]).rank().over(partition_by).alias(window_config.get("output_col", "rank")))
            return data
        elif engine == "dask":
            # Dask window functions are limited, fallback to pandas-like
            if not DASK_AVAILABLE:
                raise ImportError("Dask not available")
            # Simplified implementation
            return data
        else:
            raise NotImplementedError(f"Engine {engine} not supported for window functions")

    def execute_custom_udf(self, data: Any, udf_code: str, engine: str) -> Any:
        """Execute custom UDF."""
        # Dangerous, but for demo; in production, sandbox this
        exec_globals = {"data": data}
        if engine == "pandas" and PANDAS_AVAILABLE:
            exec_globals["pd"] = pd
        elif engine == "polars" and POLARS_AVAILABLE:
            exec_globals["pl"] = pl
        elif engine == "dask" and DASK_AVAILABLE:
            exec_globals["dd"] = dd

        try:
            exec(udf_code, exec_globals)
            return exec_globals.get("result", data)
        except Exception as e:
            self.logger.exception("UDF execution failed")
            raise

    def execute_sql_transform(self, data: Any, sql_query: str, engine: str) -> Any:
        """Execute SQL query on data."""
        if not DUCKDB_AVAILABLE:
            raise ImportError("DuckDB not available for SQL transforms")

        con = duckdb.connect()
        if isinstance(data, dict):
            for name, df in data.items():
                if PANDAS_AVAILABLE and isinstance(df, pd.DataFrame):
                    con.register(name, df)
                elif POLARS_AVAILABLE and isinstance(df, pl.DataFrame):
                    con.register(name, df.to_pandas())
                elif DASK_AVAILABLE and isinstance(df, dd.DataFrame):
                    con.register(name, df.compute())
        elif PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
            con.register("data", data)
        elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
            con.register("data", data.to_pandas())
        elif DASK_AVAILABLE and isinstance(data, dd.DataFrame):
            con.register("data", data.compute())

        result = con.execute(sql_query).fetchdf()
        con.close()
        return result

    def validate_schema(self, data: Any, expected_schema: Dict[str, str]) -> bool:
        """Validate data schema against expected."""
        if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
            actual_dtypes = data.dtypes.to_dict()
            for col, expected_dtype in expected_schema.items():
                if col not in actual_dtypes or str(actual_dtypes[col]) != expected_dtype:
                    raise ValueError(f"Schema validation failed for column {col}")
        # Add for other engines if needed
        return True

    def log_transformation(
        self,
        run_id: Optional[str],
        transform_id: Optional[str],
        engine_used: str,
        rows_in: int,
        rows_out: int,
        duration: float,
        error: Optional[str] = None,
    ) -> None:
        """Log transformation execution to TRANSFORM_LOG."""
        try:
            event_id = str(uuid.uuid4())
            sql = """
                INSERT INTO TRANSFORM_LOG (event_id, run_id, transform_id, engine_used, rows_in, rows_out, duration_seconds, error_message, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            params = (event_id, run_id, transform_id, engine_used, rows_in, rows_out, duration, error, datetime.utcnow())
            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()
            self.logger.debug("Logged transformation: %s rows in, %s out (engine=%s)", rows_in, rows_out, engine_used)
        except Exception:
            try:
                self.db.rollback()
            except Exception:
                pass
            self.logger.exception("Failed to log transformation")

    def _estimate_rows(self, data: Union[Dict[str, Any], Any]) -> int:
        """Estimate total rows in data."""
        if isinstance(data, dict):
            return sum(self._estimate_rows(df) for df in data.values())
        try:
            if hasattr(data, "shape"):
                return int(data.shape[0])
            if hasattr(data, "__len__"):
                return len(data)
            return 1
        except Exception:
            return 0