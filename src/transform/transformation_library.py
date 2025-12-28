import logging
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Union, Callable

# Optional imports
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

logger = logging.getLogger("TransformationExamples")


class TransformationExamples:
    """
    Concrete transformation examples supporting Pandas, Polars, and Dask engines.
    Each method demonstrates a specific transformation type with engine-agnostic interface.
    """

    def __init__(self, db_connection: Any = None):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)

    def _validate_engine(self, engine: str) -> None:
        """Validate that the requested engine is available."""
        if engine == "pandas" and not PANDAS_AVAILABLE:
            raise ImportError("Pandas not available")
        elif engine == "polars" and not POLARS_AVAILABLE:
            raise ImportError("Polars not available")
        elif engine == "dask" and not DASK_AVAILABLE:
            raise ImportError("Dask not available")

    def _log_transformation(self, transformation_type: str, details: Dict[str, Any]) -> None:
        """Log transformation execution."""
        if self.db:
            try:
                sql = """
                    INSERT INTO TRANSFORM_LOG (event_id, transform_id, engine_used, rows_in, rows_out, duration_seconds, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """
                event_id = str(uuid.uuid4())
                params = (
                    event_id,
                    f"example_{transformation_type}",
                    details.get("engine", "unknown"),
                    details.get("rows_in", 0),
                    details.get("rows_out", 0),
                    details.get("duration", 0),
                    datetime.utcnow(),
                )
                self.db.begin()
                self.db.execute(sql, params)
                self.db.commit()
            except Exception:
                self.db.rollback()
                self.logger.exception("Failed to log transformation")

        self.logger.info(f"Executed {transformation_type}: {details}")

    # 1. Filter Transformations
    def filter_by_condition(self, data: Any, column: str, operator: str, value: Any, engine: str = "pandas") -> Any:
        """Filter data by a single condition."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            if engine == "pandas":
                if operator == "==":
                    result = data[data[column] == value]
                elif operator == "!=":
                    result = data[data[column] != value]
                elif operator == ">":
                    result = data[data[column] > value]
                elif operator == "<":
                    result = data[data[column] < value]
                elif operator == ">=":
                    result = data[data[column] >= value]
                elif operator == "<=":
                    result = data[data[column] <= value]
                else:
                    raise ValueError(f"Unsupported operator: {operator}")

            elif engine == "polars":
                if operator == "==":
                    result = data.filter(pl.col(column) == value)
                elif operator == "!=":
                    result = data.filter(pl.col(column) != value)
                elif operator == ">":
                    result = data.filter(pl.col(column) > value)
                elif operator == "<":
                    result = data.filter(pl.col(column) < value)
                elif operator == ">=":
                    result = data.filter(pl.col(column) >= value)
                elif operator == "<=":
                    result = data.filter(pl.col(column) <= value)
                else:
                    raise ValueError(f"Unsupported operator: {operator}")

            elif engine == "dask":
                if operator == "==":
                    result = data[data[column] == value]
                elif operator == "!=":
                    result = data[data[column] != value]
                elif operator == ">":
                    result = data[data[column] > value]
                elif operator == "<":
                    result = data[data[column] < value]
                elif operator == ">=":
                    result = data[data[column] >= value]
                elif operator == "<=":
                    result = data[data[column] <= value]
                else:
                    raise ValueError(f"Unsupported operator: {operator}")

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "filter_by_condition",
                "column": column,
                "operator": operator,
                "value": value,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("filter_by_condition", details)
            return result

        except Exception as e:
            self.logger.exception(f"Filter by condition failed: {e}")
            raise

    def filter_by_multiple_conditions(self, data: Any, conditions: List[Dict[str, Any]], engine: str = "pandas") -> Any:
        """Filter data by multiple conditions with AND logic."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            result = data
            for condition in conditions:
                column = condition["column"]
                operator = condition["operator"]
                value = condition["value"]
                result = self.filter_by_condition(result, column, operator, value, engine)

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "filter_by_multiple_conditions",
                "conditions": conditions,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("filter_by_multiple_conditions", details)
            return result

        except Exception as e:
            self.logger.exception(f"Filter by multiple conditions failed: {e}")
            raise

    def filter_by_date_range(self, data: Any, date_column: str, start_date: str, end_date: str, engine: str = "pandas") -> Any:
        """Filter data by date range."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            if engine == "pandas":
                result = data[(data[date_column] >= start_date) & (data[date_column] <= end_date)]
            elif engine == "polars":
                result = data.filter((pl.col(date_column) >= start_date) & (pl.col(date_column) <= end_date))
            elif engine == "dask":
                result = data[(data[date_column] >= start_date) & (data[date_column] <= end_date)]

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "filter_by_date_range",
                "date_column": date_column,
                "start_date": start_date,
                "end_date": end_date,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("filter_by_date_range", details)
            return result

        except Exception as e:
            self.logger.exception(f"Filter by date range failed: {e}")
            raise

    # 2. Column Mapping
    def rename_columns(self, data: Any, column_mapping: Dict[str, str], engine: str = "pandas") -> Any:
        """Rename columns based on mapping dictionary."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            if engine == "pandas":
                result = data.rename(columns=column_mapping)
            elif engine == "polars":
                result = data.rename(column_mapping)
            elif engine == "dask":
                result = data.rename(columns=column_mapping)

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "rename_columns",
                "mapping": column_mapping,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("rename_columns", details)
            return result

        except Exception as e:
            self.logger.exception(f"Rename columns failed: {e}")
            raise

    def drop_columns(self, data: Any, columns_to_drop: List[str], engine: str = "pandas") -> Any:
        """Drop specified columns."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            if engine == "pandas":
                result = data.drop(columns=columns_to_drop)
            elif engine == "polars":
                result = data.drop(columns_to_drop)
            elif engine == "dask":
                result = data.drop(columns=columns_to_drop)

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "drop_columns",
                "dropped_columns": columns_to_drop,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("drop_columns", details)
            return result

        except Exception as e:
            self.logger.exception(f"Drop columns failed: {e}")
            raise

    def reorder_columns(self, data: Any, new_order: List[str], engine: str = "pandas") -> Any:
        """Reorder columns to specified order."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            if engine == "pandas":
                result = data[new_order]
            elif engine == "polars":
                result = data.select(new_order)
            elif engine == "dask":
                result = data[new_order]

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "reorder_columns",
                "new_order": new_order,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("reorder_columns", details)
            return result

        except Exception as e:
            self.logger.exception(f"Reorder columns failed: {e}")
            raise

    # 3. Type Conversion
    def convert_to_numeric(self, data: Any, columns: List[str], engine: str = "pandas") -> Any:
        """Convert columns to numeric type."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            result = data.copy() if engine == "pandas" else data.clone() if engine == "polars" else data

            if engine == "pandas":
                for col in columns:
                    result[col] = pd.to_numeric(result[col], errors='coerce')
            elif engine == "polars":
                for col in columns:
                    result = result.with_columns(pl.col(col).cast(pl.Float64))
            elif engine == "dask":
                for col in columns:
                    result[col] = dd.to_numeric(result[col], errors='coerce')

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "convert_to_numeric",
                "columns": columns,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("convert_to_numeric", details)
            return result

        except Exception as e:
            self.logger.exception(f"Convert to numeric failed: {e}")
            raise

    def convert_to_datetime(self, data: Any, columns: List[str], format: Optional[str] = None, engine: str = "pandas") -> Any:
        """Convert columns to datetime type."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            result = data.copy() if engine == "pandas" else data.clone() if engine == "polars" else data

            if engine == "pandas":
                for col in columns:
                    result[col] = pd.to_datetime(result[col], format=format, errors='coerce')
            elif engine == "polars":
                for col in columns:
                    result = result.with_columns(pl.col(col).str.strptime(pl.Datetime, format))
            elif engine == "dask":
                for col in columns:
                    result[col] = dd.to_datetime(result[col], format=format, errors='coerce')

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "convert_to_datetime",
                "columns": columns,
                "format": format,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("convert_to_datetime", details)
            return result

        except Exception as e:
            self.logger.exception(f"Convert to datetime failed: {e}")
            raise

    def convert_to_string(self, data: Any, columns: List[str], engine: str = "pandas") -> Any:
        """Convert columns to string type."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            result = data.copy() if engine == "pandas" else data.clone() if engine == "polars" else data

            if engine == "pandas":
                for col in columns:
                    result[col] = result[col].astype(str)
            elif engine == "polars":
                for col in columns:
                    result = result.with_columns(pl.col(col).cast(pl.Utf8))
            elif engine == "dask":
                for col in columns:
                    result[col] = result[col].astype(str)

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "convert_to_string",
                "columns": columns,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("convert_to_string", details)
            return result

        except Exception as e:
            self.logger.exception(f"Convert to string failed: {e}")
            raise

    # 4. Join Operations
    def inner_join(self, left: Any, right: Any, on: Union[str, List[str]], engine: str = "pandas") -> Any:
        """Perform inner join."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            if engine == "pandas":
                result = left.merge(right, on=on, how='inner')
            elif engine == "polars":
                result = left.join(right, on=on, how='inner')
            elif engine == "dask":
                result = left.merge(right, on=on, how='inner')

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "inner_join",
                "join_keys": on,
                "left_rows": len(left) if hasattr(left, "__len__") else 0,
                "right_rows": len(right) if hasattr(right, "__len__") else 0,
                "result_rows": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("inner_join", details)
            return result

        except Exception as e:
            self.logger.exception(f"Inner join failed: {e}")
            raise

    def left_join(self, left: Any, right: Any, on: Union[str, List[str]], engine: str = "pandas") -> Any:
        """Perform left join."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            if engine == "pandas":
                result = left.merge(right, on=on, how='left')
            elif engine == "polars":
                result = left.join(right, on=on, how='left')
            elif engine == "dask":
                result = left.merge(right, on=on, how='left')

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "left_join",
                "join_keys": on,
                "left_rows": len(left) if hasattr(left, "__len__") else 0,
                "right_rows": len(right) if hasattr(right, "__len__") else 0,
                "result_rows": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("left_join", details)
            return result

        except Exception as e:
            self.logger.exception(f"Left join failed: {e}")
            raise

    def merge_multiple(self, dataframes: List[Any], on: Union[str, List[str]], how: str = "inner", engine: str = "pandas") -> Any:
        """Merge multiple dataframes."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            result = dataframes[0]
            for df in dataframes[1:]:
                if engine == "pandas":
                    result = result.merge(df, on=on, how=how)
                elif engine == "polars":
                    result = result.join(df, on=on, how=how)
                elif engine == "dask":
                    result = result.merge(df, on=on, how=how)

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "merge_multiple",
                "join_keys": on,
                "how": how,
                "num_dataframes": len(dataframes),
                "result_rows": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("merge_multiple", details)
            return result

        except Exception as e:
            self.logger.exception(f"Merge multiple failed: {e}")
            raise

    # 5. Aggregations
    def groupby_aggregate(self, data: Any, groupby_cols: List[str], agg_dict: Dict[str, str], engine: str = "pandas") -> Any:
        """Perform groupby aggregation."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            if engine == "pandas":
                result = data.groupby(groupby_cols).agg(agg_dict).reset_index()
            elif engine == "polars":
                agg_exprs = []
                for col, func in agg_dict.items():
                    if func == "sum":
                        agg_exprs.append(pl.col(col).sum().alias(f"{col}_sum"))
                    elif func == "mean":
                        agg_exprs.append(pl.col(col).mean().alias(f"{col}_mean"))
                    elif func == "count":
                        agg_exprs.append(pl.col(col).count().alias(f"{col}_count"))
                    elif func == "min":
                        agg_exprs.append(pl.col(col).min().alias(f"{col}_min"))
                    elif func == "max":
                        agg_exprs.append(pl.col(col).max().alias(f"{col}_max"))
                result = data.group_by(groupby_cols).agg(agg_exprs)
            elif engine == "dask":
                result = data.groupby(groupby_cols).agg(agg_dict).reset_index()

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "groupby_aggregate",
                "groupby_cols": groupby_cols,
                "agg_dict": agg_dict,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("groupby_aggregate", details)
            return result

        except Exception as e:
            self.logger.exception(f"Groupby aggregate failed: {e}")
            raise

    def pivot_table(self, data: Any, index: List[str], columns: str, values: List[str], aggfunc: str = "mean", engine: str = "pandas") -> Any:
        """Create pivot table."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            if engine == "pandas":
                result = data.pivot_table(index=index, columns=columns, values=values, aggfunc=aggfunc).reset_index()
            elif engine == "polars":
                # Polars pivot requires different approach
                # First group by index + columns, then aggregate, then pivot
                grouped = data.group_by(index + [columns]).agg([
                    pl.col(val).mean().alias(f"{val}_mean") if aggfunc == "mean" else
                    pl.col(val).sum().alias(f"{val}_sum") if aggfunc == "sum" else
                    pl.col(val).count().alias(f"{val}_count")
                    for val in values
                ])
                result = grouped.pivot(index=index, columns=columns, values=[f"{val}_{aggfunc}" for val in values])
            elif engine == "dask":
                result = data.pivot_table(index=index, columns=columns, values=values, aggfunc=aggfunc).reset_index()

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "pivot_table",
                "index": index,
                "columns": columns,
                "values": values,
                "aggfunc": aggfunc,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("pivot_table", details)
            return result

        except Exception as e:
            self.logger.exception(f"Pivot table failed: {e}")
            raise

    def rolling_aggregate(self, data: Any, column: str, window: int, function: str = "mean", engine: str = "pandas") -> Any:
        """Apply rolling window aggregation."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            result = data.copy() if engine == "pandas" else data.clone() if engine == "polars" else data

            if engine == "pandas":
                if function == "mean":
                    result[f"{column}_rolling_{function}"] = result[column].rolling(window=window).mean()
                elif function == "sum":
                    result[f"{column}_rolling_{function}"] = result[column].rolling(window=window).sum()
                elif function == "std":
                    result[f"{column}_rolling_{function}"] = result[column].rolling(window=window).std()
            elif engine == "polars":
                if function == "mean":
                    result = result.with_columns(pl.col(column).rolling_mean(window_size=window).alias(f"{column}_rolling_{function}"))
                elif function == "sum":
                    result = result.with_columns(pl.col(column).rolling_sum(window_size=window).alias(f"{column}_rolling_{function}"))
            elif engine == "dask":
                if function == "mean":
                    result[f"{column}_rolling_{function}"] = result[column].rolling(window=window).mean()
                elif function == "sum":
                    result[f"{column}_rolling_{function}"] = result[column].rolling(window=window).sum()

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "rolling_aggregate",
                "column": column,
                "window": window,
                "function": function,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("rolling_aggregate", details)
            return result

        except Exception as e:
            self.logger.exception(f"Rolling aggregate failed: {e}")
            raise

    # 6. Window Functions
    def rank_within_group(self, data: Any, partition_by: List[str], order_by: str, engine: str = "pandas") -> Any:
        """Add rank within partition."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            result = data.copy() if engine == "pandas" else data.clone() if engine == "polars" else data

            if engine == "pandas":
                result["rank"] = result.groupby(partition_by)[order_by].rank(method="dense")
            elif engine == "polars":
                result = result.with_columns(pl.col(order_by).rank().over(partition_by).alias("rank"))
            elif engine == "dask":
                result["rank"] = result.groupby(partition_by)[order_by].rank(method="dense")

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "rank_within_group",
                "partition_by": partition_by,
                "order_by": order_by,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("rank_within_group", details)
            return result

        except Exception as e:
            self.logger.exception(f"Rank within group failed: {e}")
            raise

    def lag_lead(self, data: Any, column: str, partition_by: List[str], order_by: str, offset: int = 1, engine: str = "pandas") -> Any:
        """Add lag/lead column."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            result = data.copy() if engine == "pandas" else data.clone() if engine == "polars" else data

            if engine == "pandas":
                result[f"{column}_lag_{offset}"] = result.groupby(partition_by)[column].shift(offset)
            elif engine == "polars":
                result = result.with_columns(pl.col(column).shift(offset).over(partition_by).alias(f"{column}_lag_{offset}"))
            elif engine == "dask":
                result[f"{column}_lag_{offset}"] = result.groupby(partition_by)[column].shift(offset)

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "lag_lead",
                "column": column,
                "partition_by": partition_by,
                "order_by": order_by,
                "offset": offset,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("lag_lead", details)
            return result

        except Exception as e:
            self.logger.exception(f"Lag/lead failed: {e}")
            raise

    def running_total(self, data: Any, column: str, partition_by: List[str], order_by: str, engine: str = "pandas") -> Any:
        """Add running total column."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            result = data.copy() if engine == "pandas" else data.clone() if engine == "polars" else data

            if engine == "pandas":
                result[f"{column}_running_total"] = result.groupby(partition_by)[column].cumsum()
            elif engine == "polars":
                result = result.with_columns(pl.col(column).cum_sum().over(partition_by).alias(f"{column}_running_total"))
            elif engine == "dask":
                result[f"{column}_running_total"] = result.groupby(partition_by)[column].cumsum()

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "running_total",
                "column": column,
                "partition_by": partition_by,
                "order_by": order_by,
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("running_total", details)
            return result

        except Exception as e:
            self.logger.exception(f"Running total failed: {e}")
            raise

    # 7. Custom UDF
    def apply_custom_function(self, data: Any, column: str, function: Callable, engine: str = "pandas") -> Any:
        """Apply custom function to column."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            result = data.copy() if engine == "pandas" else data.clone() if engine == "polars" else data

            if engine == "pandas":
                result[f"{column}_transformed"] = result[column].apply(function)
            elif engine == "polars":
                result = result.with_columns(pl.col(column).map_elements(function).alias(f"{column}_transformed"))
            elif engine == "dask":
                result[f"{column}_transformed"] = result[column].apply(function, meta=('x', 'f8'))

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "apply_custom_function",
                "column": column,
                "function": str(function),
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("apply_custom_function", details)
            return result

        except Exception as e:
            self.logger.exception(f"Apply custom function failed: {e}")
            raise

    def apply_row_wise_function(self, data: Any, function: Callable, engine: str = "pandas") -> Any:
        """Apply function to each row."""
        self._validate_engine(engine)
        start_time = datetime.utcnow()

        try:
            result = data.copy() if engine == "pandas" else data.clone() if engine == "polars" else data

            if engine == "pandas":
                result["custom_result"] = data.apply(function, axis=1)
            elif engine == "polars":
                # Row-wise operations are more complex in Polars
                # This is a simplified example
                result = result.with_columns(pl.struct(data.columns).map_elements(function).alias("custom_result"))
            elif engine == "dask":
                result["custom_result"] = data.apply(function, axis=1, meta=('x', 'f8'))

            duration = (datetime.utcnow() - start_time).total_seconds()
            details = {
                "engine": engine,
                "transformation": "apply_row_wise_function",
                "function": str(function),
                "rows_in": len(data) if hasattr(data, "__len__") else 0,
                "rows_out": len(result) if hasattr(result, "__len__") else 0,
                "duration": duration,
            }
            self._log_transformation("apply_row_wise_function", details)
            return result

        except Exception as e:
            self.logger.exception(f"Apply row-wise function failed: {e}")
            raise