import logging
import pandas as pd
import datetime
from typing import Dict, List, Optional, Any, Tuple, Union
from dataclasses import dataclass, asdict
from enum import Enum
import uuid

# Optional imports
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    import pyarrow as pa
    PYARROW_AVAILABLE = True
except ImportError:
    PYARROW_AVAILABLE = False

logger = logging.getLogger("IncrementalLoadManager")


class IncrementalStrategy(Enum):
    """Supported incremental loading strategies."""
    TIMESTAMP = "timestamp"
    SEQUENCE = "sequence"
    CDC = "cdc"
    SNAPSHOT = "snapshot"
    DELTA_LAKE = "delta_lake"


class SCDType(Enum):
    """Slowly Changing Dimension types."""
    TYPE1 = "type1"  # Overwrite
    TYPE2 = "type2"  # Add new row with effective dates
    TYPE3 = "type3"  # Add previous value column


@dataclass
class Watermark:
    """Represents a watermark value for incremental loading."""
    pipeline_id: str
    source_id: str
    watermark_column: str
    watermark_value: Any
    update_time: datetime.datetime
    strategy: str
    metadata: Optional[Dict[str, Any]] = None


@dataclass
class IncrementalMetrics:
    """Metrics for incremental load operations."""
    run_id: str
    pipeline_id: str
    source_id: str
    records_extracted: int
    records_inserted: int
    records_updated: int
    records_deleted: int
    duplicates_found: int
    late_arrivals: int
    processing_time_seconds: float
    watermark_before: Any
    watermark_after: Any
    gaps_detected: int
    validation_errors: int
    timestamp: datetime.datetime


@dataclass
class CDCEvent:
    """Represents a Change Data Capture event."""
    operation: str  # INSERT, UPDATE, DELETE
    table_name: str
    primary_key: Dict[str, Any]
    before_values: Optional[Dict[str, Any]]
    after_values: Optional[Dict[str, Any]]
    commit_timestamp: datetime.datetime
    transaction_id: str
    lsn: Optional[str] = None  # Log Sequence Number


class IncrementalLoadManager:
    """
    Manages incremental data loading strategies for ETL pipelines.
    Supports multiple strategies including timestamp, sequence, CDC, snapshot, and delta lake.
    """

    def __init__(self, db_connection: Any = None, config: Optional[Dict[str, Any]] = None):
        self.db = db_connection
        self.config = config or {}
        self.logger = logging.getLogger(self.__class__.__name__)

        # Default configurations
        self.default_batch_size = self.config.get('batch_size', 10000)
        self.default_timeout = self.config.get('timeout_seconds', 3600)
        self.enable_gap_detection = self.config.get('enable_gap_detection', True)
        self.enable_duplicate_handling = self.config.get('enable_duplicate_handling', True)

    def get_last_watermark(self, pipeline_id: str, source_id: str,
                          watermark_column: Optional[str] = None) -> Optional[Watermark]:
        """Retrieve the last watermark value for a pipeline source."""
        try:
            watermark = self._get_watermark_db(pipeline_id, source_id, watermark_column)

            if watermark:
                self.logger.info(f"Retrieved watermark for {pipeline_id}/{source_id}: {watermark.watermark_value}")
                return watermark

        except Exception as e:
            self.logger.exception(f"Failed to get watermark for {pipeline_id}/{source_id}: {e}")

        return None

    def extract_incremental_data(self, source_config: Dict[str, Any],
                               watermark: Optional[Watermark] = None) -> Union[pd.DataFrame, 'pl.DataFrame', None]:
        """Extract incremental data based on the configured strategy."""
        strategy = IncrementalStrategy(source_config.get('incremental_strategy', 'timestamp'))

        try:
            if strategy == IncrementalStrategy.TIMESTAMP:
                return self._extract_timestamp_based(source_config, watermark)
            elif strategy == IncrementalStrategy.SEQUENCE:
                return self._extract_sequence_based(source_config, watermark)
            elif strategy == IncrementalStrategy.CDC:
                return self._extract_cdc_based(source_config, watermark)
            elif strategy == IncrementalStrategy.SNAPSHOT:
                return self._extract_snapshot_based(source_config, watermark)
            elif strategy == IncrementalStrategy.DELTA_LAKE:
                return self._extract_delta_lake(source_config, watermark)
            else:
                raise ValueError(f"Unsupported incremental strategy: {strategy}")

        except Exception as e:
            self.logger.exception(f"Failed to extract incremental data: {e}")
            raise

    def update_watermark(self, pipeline_id: str, source_id: str,
                        new_watermark: Any, watermark_column: str = None,
                        strategy: str = None, metadata: Optional[Dict[str, Any]] = None) -> bool:
        """Update the watermark value for a pipeline source."""
        try:
            watermark = Watermark(
                pipeline_id=pipeline_id,
                source_id=source_id,
                watermark_column=watermark_column or "default",
                watermark_value=new_watermark,
                update_time=datetime.datetime.utcnow(),
                strategy=strategy or "timestamp",
                metadata=metadata
            )

            success = self._update_watermark_db(watermark)

            if success:
                self.logger.info(f"Updated watermark for {pipeline_id}/{source_id} to {new_watermark}")

            return success

        except Exception as e:
            self.logger.exception(f"Failed to update watermark for {pipeline_id}/{source_id}: {e}")
            return False

    def detect_deletes(self, source_table: str, target_table: str,
                      key_columns: List[str], watermark: Optional[Watermark] = None) -> Union[pd.DataFrame, 'pl.DataFrame', None]:
        """Detect records that have been deleted from source."""
        try:
            # Build query to find records in target but not in source
            key_condition = " AND ".join([f"t.{col} = s.{col}" for col in key_columns])

            if watermark and watermark.watermark_column:
                watermark_condition = f" AND s.{watermark.watermark_column} > '{watermark.watermark_value}'"
            else:
                watermark_condition = ""

            query = f"""
                SELECT t.* FROM {target_table} t
                LEFT JOIN {source_table} s ON {key_condition}
                WHERE s.{key_columns[0]} IS NULL{watermark_condition}
            """

            return self._execute_query(query)

        except Exception as e:
            self.logger.exception(f"Failed to detect deletes between {source_table} and {target_table}: {e}")
            return None

    def handle_late_arriving_data(self, data: Union[pd.DataFrame, 'pl.DataFrame'],
                                watermark_column: str, current_watermark: Any) -> Tuple[Union[pd.DataFrame, 'pl.DataFrame'], int]:
        """Handle data that arrives after the current watermark."""
        try:
            late_count = 0

            if isinstance(data, pd.DataFrame):
                # Filter late arriving data
                late_mask = data[watermark_column] <= current_watermark
                late_data = data[late_mask]
                on_time_data = data[~late_mask]
                late_count = len(late_data)

                # Log late arrivals
                if late_count > 0:
                    self.logger.warning(f"Found {late_count} late arriving records")

                    # Handle late data (could be inserted separately or flagged)
                    self._process_late_data(late_data)

                return on_time_data, late_count

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                # Similar logic for Polars
                late_data = data.filter(pl.col(watermark_column) <= current_watermark)
                on_time_data = data.filter(pl.col(watermark_column) > current_watermark)
                late_count = late_data.height

                if late_count > 0:
                    self.logger.warning(f"Found {late_count} late arriving records")
                    self._process_late_data(late_data)

                return on_time_data, late_count

            return data, 0

        except Exception as e:
            self.logger.exception(f"Failed to handle late arriving data: {e}")
            return data, 0

    def merge_incremental_load(self, target_table: str, data: Union[pd.DataFrame, 'pl.DataFrame'],
                             merge_keys: List[str], load_type: str = "upsert") -> Dict[str, int]:
        """Perform incremental load with merge/upsert operations."""
        try:
            metrics = {"inserted": 0, "updated": 0, "deleted": 0, "errors": 0}

            if load_type == "upsert":
                metrics = self._perform_upsert(target_table, data, merge_keys)
            elif load_type == "insert_only":
                metrics["inserted"] = self._perform_insert(target_table, data)
            elif load_type == "update_only":
                metrics["updated"] = self._perform_update(target_table, data, merge_keys)
            else:
                raise ValueError(f"Unsupported load type: {load_type}")

            self.logger.info(f"Merge completed: {metrics}")
            return metrics

        except Exception as e:
            self.logger.exception(f"Failed to merge incremental load to {target_table}: {e}")
            return {"inserted": 0, "updated": 0, "deleted": 0, "errors": 1}

    def implement_scd_type2(self, target_table: str, data: Union[pd.DataFrame, 'pl.DataFrame'],
                          business_key: Union[str, List[str]], effective_date: str,
                          end_date_column: str = "effective_end_date",
                          current_flag_column: str = "is_current") -> Dict[str, int]:
        """Implement Slowly Changing Dimension Type 2."""
        try:
            metrics = {"new_records": 0, "updated_records": 0, "closed_records": 0}

            # Convert business_key to list if string
            if isinstance(business_key, str):
                business_key = [business_key]

            # Get existing records
            existing_query = f"SELECT * FROM {target_table} WHERE {current_flag_column} = 1"
            existing_data = self._execute_query(existing_query)

            if existing_data is None or len(existing_data) == 0:
                # First load - all records are new
                self._insert_scd_records(target_table, data, effective_date, None, True)
                metrics["new_records"] = len(data) if hasattr(data, '__len__') else data.height
                return metrics

            # Find new and changed records
            new_records, changed_records = self._identify_scd_changes(
                data, existing_data, business_key, effective_date
            )

            # Insert new records
            if len(new_records) > 0:
                self._insert_scd_records(target_table, new_records, effective_date, None, True)
                metrics["new_records"] = len(new_records)

            # Close old records and insert new versions for changed records
            if len(changed_records) > 0:
                # Close existing records
                self._close_scd_records(target_table, changed_records, business_key, effective_date)
                # Insert new versions
                self._insert_scd_records(target_table, changed_records, effective_date, None, True)
                metrics["updated_records"] = len(changed_records)

            self.logger.info(f"SCD Type 2 completed: {metrics}")
            return metrics

        except Exception as e:
            self.logger.exception(f"Failed to implement SCD Type 2 for {target_table}: {e}")
            return {"new_records": 0, "updated_records": 0, "closed_records": 0}

    def optimize_incremental_query(self, query: str, watermark: Optional[Watermark],
                                 partition_column: Optional[str] = None) -> str:
        """Optimize incremental query for better performance."""
        try:
            optimized_query = query

            # Add partition pruning if partition column exists
            if partition_column and watermark:
                partition_filter = f" AND {partition_column} >= '{watermark.watermark_value}'"
                optimized_query += partition_filter

            # Add index hints for watermark columns
            if watermark and watermark.watermark_column:
                # This would be database-specific
                pass

            # Add LIMIT for testing
            if self.config.get('add_limit_for_testing', False):
                optimized_query += f" LIMIT {self.default_batch_size}"

            self.logger.debug(f"Optimized query: {optimized_query}")
            return optimized_query

        except Exception as e:
            self.logger.exception(f"Failed to optimize incremental query: {e}")
            return query

    def validate_incremental_completeness(self, expected_count: int, actual_count: int,
                                        tolerance_percent: float = 5.0) -> Dict[str, Any]:
        """Validate that incremental load captured all expected data."""
        try:
            difference = abs(expected_count - actual_count)
            difference_percent = (difference / expected_count * 100) if expected_count > 0 else 0

            validation_result = {
                "expected_count": expected_count,
                "actual_count": actual_count,
                "difference": difference,
                "difference_percent": difference_percent,
                "is_complete": difference_percent <= tolerance_percent,
                "tolerance_percent": tolerance_percent
            }

            if not validation_result["is_complete"]:
                self.logger.warning(f"Incremental completeness validation failed: {validation_result}")

            return validation_result

        except Exception as e:
            self.logger.exception(f"Failed to validate incremental completeness: {e}")
            return {"is_complete": False, "error": str(e)}

    def handle_data_correction(self, target_table: str, correction_data: Union[pd.DataFrame, 'pl.DataFrame'],
                             effective_date: str, business_key: Union[str, List[str]]) -> Dict[str, int]:
        """Handle backdated data corrections."""
        try:
            metrics = {"corrected_records": 0, "errors": 0}

            # Convert business_key to list if string
            if isinstance(business_key, str):
                business_key = [business_key]

            # For each correction record
            for _, record in correction_data.iterrows() if isinstance(correction_data, pd.DataFrame) else correction_data.rows(named=True):
                try:
                    # Find existing records to correct
                    key_conditions = [f"{col} = '{record[col]}'" for col in business_key]
                    where_clause = " AND ".join(key_conditions)

                    # Update the record with correction
                    update_fields = [f"{col} = '{record[col]}'" for col in record.keys() if col not in business_key]
                    update_clause = ", ".join(update_fields)

                    query = f"UPDATE {target_table} SET {update_clause} WHERE {where_clause}"
                    self._execute_query(query, is_update=True)

                    metrics["corrected_records"] += 1

                except Exception as e:
                    self.logger.exception(f"Failed to correct record: {e}")
                    metrics["errors"] += 1

            self.logger.info(f"Data correction completed: {metrics}")
            return metrics

        except Exception as e:
            self.logger.exception(f"Failed to handle data correction for {target_table}: {e}")
            return {"corrected_records": 0, "errors": 1}

    def get_incremental_metrics(self, run_id: str) -> Optional[IncrementalMetrics]:
        """Retrieve metrics for an incremental load run."""
        try:
            return self._get_metrics_db(run_id)
        except Exception as e:
            self.logger.exception(f"Failed to get incremental metrics for run {run_id}: {e}")
            return None

    def configure_cdc_source(self, source_config: Dict[str, Any]) -> bool:
        """Configure a source for Change Data Capture."""
        try:
            # This would set up CDC based on the source type
            cdc_type = source_config.get('cdc_type', 'log_based')

            if cdc_type == 'log_based':
                return self._configure_log_based_cdc(source_config)
            elif cdc_type == 'trigger_based':
                return self._configure_trigger_based_cdc(source_config)
            elif cdc_type == 'api_based':
                return self._configure_api_based_cdc(source_config)
            else:
                raise ValueError(f"Unsupported CDC type: {cdc_type}")

        except Exception as e:
            self.logger.exception(f"Failed to configure CDC source: {e}")
            return False

    def read_cdc_stream(self, source_config: Dict[str, Any], from_lsn: Optional[str] = None) -> List[CDCEvent]:
        """Read the CDC change stream from a source."""
        try:
            cdc_events = []

            # This would read from the specific CDC implementation
            cdc_type = source_config.get('cdc_type', 'log_based')

            if cdc_type == 'log_based':
                cdc_events = self._read_log_based_cdc(source_config, from_lsn)
            elif cdc_type == 'trigger_based':
                cdc_events = self._read_trigger_based_cdc(source_config, from_lsn)
            elif cdc_type == 'api_based':
                cdc_events = self._read_api_based_cdc(source_config, from_lsn)

            self.logger.info(f"Read {len(cdc_events)} CDC events")
            return cdc_events

        except Exception as e:
            self.logger.exception(f"Failed to read CDC stream: {e}")
            return []

    def apply_cdc_changes(self, target_table: str, cdc_data: List[CDCEvent]) -> Dict[str, int]:
        """Apply CDC changes to the target table."""
        try:
            metrics = {"inserts": 0, "updates": 0, "deletes": 0, "errors": 0}

            for event in cdc_data:
                try:
                    if event.operation.upper() == 'INSERT':
                        self._apply_cdc_insert(target_table, event)
                        metrics["inserts"] += 1
                    elif event.operation.upper() == 'UPDATE':
                        self._apply_cdc_update(target_table, event)
                        metrics["updates"] += 1
                    elif event.operation.upper() == 'DELETE':
                        self._apply_cdc_delete(target_table, event)
                        metrics["deletes"] += 1
                    else:
                        self.logger.warning(f"Unknown CDC operation: {event.operation}")

                except Exception as e:
                    self.logger.exception(f"Failed to apply CDC event: {e}")
                    metrics["errors"] += 1

            self.logger.info(f"Applied CDC changes: {metrics}")
            return metrics

        except Exception as e:
            self.logger.exception(f"Failed to apply CDC changes to {target_table}: {e}")
            return {"inserts": 0, "updates": 0, "deletes": 0, "errors": 1}

    def reset_watermark(self, pipeline_id: str, source_id: str,
                       watermark_column: Optional[str] = None) -> bool:
        """Reset watermark for reprocessing."""
        try:
            success = self._reset_watermark_db(pipeline_id, source_id, watermark_column)
            if success:
                self.logger.info(f"Reset watermark for {pipeline_id}/{source_id}")
            return success
        except Exception as e:
            self.logger.exception(f"Failed to reset watermark for {pipeline_id}/{source_id}: {e}")
            return False

    def detect_data_gaps(self, pipeline_id: str, source_id: str,
                        watermark_column: str, expected_interval: str = '1 hour') -> List[Dict[str, Any]]:
        """Detect gaps in incremental data loading."""
        try:
            gaps = []

            # Get watermark history
            history = self._get_watermark_history_db(pipeline_id, source_id, watermark_column)

            if len(history) < 2:
                return gaps

            # Check for gaps based on expected interval
            for i in range(1, len(history)):
                prev_time = history[i-1]['update_time']
                curr_time = history[i]['update_time']
                prev_value = history[i-1]['watermark_value']
                curr_value = history[i]['watermark_value']

                # Calculate expected watermark based on time difference
                time_diff = curr_time - prev_time
                expected_gap = self._calculate_expected_gap(time_diff, expected_interval)

                actual_gap = curr_value - prev_value if isinstance(curr_value, (int, float)) else 0

                if actual_gap > expected_gap * 1.5:  # Allow 50% tolerance
                    gaps.append({
                        'start_time': prev_time,
                        'end_time': curr_time,
                        'expected_records': expected_gap,
                        'actual_records': actual_gap,
                        'gap_ratio': actual_gap / expected_gap if expected_gap > 0 else 0
                    })

            self.logger.info(f"Detected {len(gaps)} data gaps")
            return gaps

        except Exception as e:
            self.logger.exception(f"Failed to detect data gaps: {e}")
            return []

    def deduplicate_data(self, data: Union[pd.DataFrame, 'pl.DataFrame'],
                        key_columns: List[str]) -> Tuple[Union[pd.DataFrame, 'pl.DataFrame'], int]:
        """Remove duplicate records based on key columns."""
        try:
            original_count = len(data) if hasattr(data, '__len__') else data.height

            if isinstance(data, pd.DataFrame):
                deduplicated = data.drop_duplicates(subset=key_columns)
            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                deduplicated = data.unique(subset=key_columns)
            else:
                return data, 0

            final_count = len(deduplicated) if hasattr(deduplicated, '__len__') else deduplicated.height
            duplicates_removed = original_count - final_count

            if duplicates_removed > 0:
                self.logger.info(f"Removed {duplicates_removed} duplicate records")

            return deduplicated, duplicates_removed

        except Exception as e:
            self.logger.exception(f"Failed to deduplicate data: {e}")
            return data, 0

    # Private helper methods

    def _get_watermark_db(self, pipeline_id: str, source_id: str,
                         watermark_column: Optional[str] = None) -> Optional[Watermark]:
        """Retrieve watermark from database."""
        if not self.db:
            return None

        try:
            query = """
                SELECT pipeline_id, source_id, watermark_column, watermark_value,
                       update_time, strategy, metadata
                FROM WATERMARKS
                WHERE pipeline_id = %s AND source_id = %s
            """
            params = [pipeline_id, source_id]

            if watermark_column:
                query += " AND watermark_column = %s"
                params.append(watermark_column)

            query += " ORDER BY update_time DESC LIMIT 1"

            self.db.execute(query, params)
            row = self.db.fetchone()

            if row:
                return Watermark(
                    pipeline_id=row[0],
                    source_id=row[1],
                    watermark_column=row[2],
                    watermark_value=row[3],
                    update_time=row[4],
                    strategy=row[5],
                    metadata=row[6] if row[6] else None
                )

        except Exception as e:
            self.logger.exception(f"Database error getting watermark: {e}")

        return None

    def _update_watermark_db(self, watermark: Watermark) -> bool:
        """Update watermark in database."""
        if not self.db:
            return False

        try:
            # Upsert watermark
            query = """
                INSERT INTO WATERMARKS (
                    pipeline_id, source_id, watermark_column, watermark_value,
                    update_time, strategy, metadata
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                    watermark_value = VALUES(watermark_value),
                    update_time = VALUES(update_time),
                    metadata = VALUES(metadata)
            """

            params = (
                watermark.pipeline_id,
                watermark.source_id,
                watermark.watermark_column,
                watermark.watermark_value,
                watermark.update_time,
                watermark.strategy,
                watermark.metadata
            )

            self.db.begin()
            self.db.execute(query, params)
            self.db.commit()

            return True

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Database error updating watermark: {e}")
            return False

    def _reset_watermark_db(self, pipeline_id: str, source_id: str,
                           watermark_column: Optional[str] = None) -> bool:
        """Reset watermark in database."""
        if not self.db:
            return False

        try:
            query = "DELETE FROM WATERMARKS WHERE pipeline_id = %s AND source_id = %s"
            params = [pipeline_id, source_id]

            if watermark_column:
                query += " AND watermark_column = %s"
                params.append(watermark_column)

            self.db.begin()
            self.db.execute(query, params)
            self.db.commit()

            return True

        except Exception as e:
            self.db.rollback()
            self.logger.exception(f"Database error resetting watermark: {e}")
            return False

    def _get_metrics_db(self, run_id: str) -> Optional[IncrementalMetrics]:
        """Retrieve metrics from database."""
        if not self.db:
            return None

        try:
            query = """
                SELECT run_id, pipeline_id, source_id, records_extracted, records_inserted,
                       records_updated, records_deleted, duplicates_found, late_arrivals,
                       processing_time_seconds, watermark_before, watermark_after,
                       gaps_detected, validation_errors, timestamp
                FROM INCREMENTAL_METRICS
                WHERE run_id = %s
            """

            self.db.execute(query, (run_id,))
            row = self.db.fetchone()

            if row:
                return IncrementalMetrics(
                    run_id=row[0],
                    pipeline_id=row[1],
                    source_id=row[2],
                    records_extracted=row[3],
                    records_inserted=row[4],
                    records_updated=row[5],
                    records_deleted=row[6],
                    duplicates_found=row[7],
                    late_arrivals=row[8],
                    processing_time_seconds=row[9],
                    watermark_before=row[10],
                    watermark_after=row[11],
                    gaps_detected=row[12],
                    validation_errors=row[13],
                    timestamp=row[14]
                )

        except Exception as e:
            self.logger.exception(f"Database error getting metrics: {e}")

        return None

    def _execute_query(self, query: str, is_update: bool = False) -> Optional[Union[pd.DataFrame, 'pl.DataFrame']]:
        """Execute a query and return results."""
        if not self.db:
            return None

        try:
            self.db.execute(query)

            if is_update:
                return None

            # Fetch results
            columns = [desc[0] for desc in self.db.description]
            rows = self.db.fetchall()

            if not rows:
                return pd.DataFrame() if pd else None

            return pd.DataFrame(rows, columns=columns)

        except Exception as e:
            self.logger.exception(f"Query execution error: {e}")
            return None

    def _extract_timestamp_based(self, source_config: Dict[str, Any],
                               watermark: Optional[Watermark]) -> Optional[Union[pd.DataFrame, 'pl.DataFrame']]:
        """Extract data using timestamp-based incremental strategy."""
        table = source_config.get('table')
        timestamp_column = source_config.get('timestamp_column', 'updated_at')

        if not table:
            raise ValueError("Table name required for timestamp-based extraction")

        watermark_value = watermark.watermark_value if watermark else None

        if watermark_value:
            query = f"SELECT * FROM {table} WHERE {timestamp_column} > '{watermark_value}' ORDER BY {timestamp_column}"
        else:
            query = f"SELECT * FROM {table} ORDER BY {timestamp_column}"

        return self._execute_query(query)

    def _extract_sequence_based(self, source_config: Dict[str, Any],
                              watermark: Optional[Watermark]) -> Optional[Union[pd.DataFrame, 'pl.DataFrame']]:
        """Extract data using sequence-based incremental strategy."""
        table = source_config.get('table')
        sequence_column = source_config.get('sequence_column', 'id')

        if not table:
            raise ValueError("Table name required for sequence-based extraction")

        watermark_value = watermark.watermark_value if watermark else 0

        query = f"SELECT * FROM {table} WHERE {sequence_column} > {watermark_value} ORDER BY {sequence_column}"

        return self._execute_query(query)

    def _extract_cdc_based(self, source_config: Dict[str, Any],
                          watermark: Optional[Watermark]) -> Optional[Union[pd.DataFrame, 'pl.DataFrame']]:
        """Extract data using CDC-based incremental strategy."""
        # This would integrate with CDC systems
        cdc_events = self.read_cdc_stream(source_config, watermark.watermark_value if watermark else None)

        # Convert CDC events to DataFrame
        if cdc_events:
            data = []
            for event in cdc_events:
                if event.operation.upper() == 'INSERT' or event.operation.upper() == 'UPDATE':
                    data.append(event.after_values)

            return pd.DataFrame(data) if data else pd.DataFrame()

        return None

    def _extract_snapshot_based(self, source_config: Dict[str, Any],
                              watermark: Optional[Watermark]) -> Optional[Union[pd.DataFrame, 'pl.DataFrame']]:
        """Extract data using snapshot-based incremental strategy."""
        table = source_config.get('table')
        partition_column = source_config.get('partition_column', 'date_partition')

        if not table:
            raise ValueError("Table name required for snapshot-based extraction")

        watermark_value = watermark.watermark_value if watermark else None

        if watermark_value:
            query = f"SELECT * FROM {table} WHERE {partition_column} >= '{watermark_value}'"
        else:
            query = f"SELECT * FROM {table}"

        return self._execute_query(query)

    def _extract_delta_lake(self, source_config: Dict[str, Any],
                          watermark: Optional[Watermark]) -> Optional[Union[pd.DataFrame, 'pl.DataFrame']]:
        """Extract data using delta lake incremental strategy."""
        # This would integrate with Delta Lake
        table = source_config.get('table')
        delta_path = source_config.get('delta_path')

        if not table and not delta_path:
            raise ValueError("Table name or delta path required for delta lake extraction")

        # Placeholder for Delta Lake integration
        self.logger.info("Delta Lake extraction not yet implemented")
        return None

    def _perform_upsert(self, target_table: str, data: Union[pd.DataFrame, 'pl.DataFrame'],
                       merge_keys: List[str]) -> Dict[str, int]:
        """Perform upsert operation."""
        metrics = {"inserted": 0, "updated": 0, "errors": 0}

        # This would be database-specific implementation
        # For now, return placeholder metrics
        if isinstance(data, pd.DataFrame):
            metrics["inserted"] = len(data)
        elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
            metrics["inserted"] = data.height

        return metrics

    def _perform_insert(self, target_table: str, data: Union[pd.DataFrame, 'pl.DataFrame']) -> int:
        """Perform insert operation."""
        # Placeholder implementation
        count = len(data) if hasattr(data, '__len__') else data.height
        return count

    def _perform_update(self, target_table: str, data: Union[pd.DataFrame, 'pl.DataFrame'],
                       merge_keys: List[str]) -> int:
        """Perform update operation."""
        # Placeholder implementation
        count = len(data) if hasattr(data, '__len__') else data.height
        return count

    def _identify_scd_changes(self, new_data: Union[pd.DataFrame, 'pl.DataFrame'],
                            existing_data: Union[pd.DataFrame, 'pl.DataFrame'],
                            business_key: List[str], effective_date: str) -> Tuple[Union[pd.DataFrame, 'pl.DataFrame'], Union[pd.DataFrame, 'pl.DataFrame']]:
        """Identify new and changed records for SCD."""
        # Placeholder implementation
        return new_data, pd.DataFrame() if isinstance(new_data, pd.DataFrame) else pl.DataFrame()

    def _insert_scd_records(self, target_table: str, data: Union[pd.DataFrame, 'pl.DataFrame'],
                          effective_date: str, end_date: Optional[str], is_current: bool) -> None:
        """Insert SCD records."""
        # Placeholder implementation
        pass

    def _close_scd_records(self, target_table: str, data: Union[pd.DataFrame, 'pl.DataFrame'],
                         business_key: List[str], end_date: str) -> None:
        """Close SCD records."""
        # Placeholder implementation
        pass

    def _process_late_data(self, late_data: Union[pd.DataFrame, 'pl.DataFrame']) -> None:
        """Process late arriving data."""
        # Placeholder - could write to separate table or log
        self.logger.info(f"Processing {len(late_data) if hasattr(late_data, '__len__') else late_data.height} late records")

    def _configure_log_based_cdc(self, source_config: Dict[str, Any]) -> bool:
        """Configure log-based CDC."""
        # Placeholder for CDC configuration
        return True

    def _configure_trigger_based_cdc(self, source_config: Dict[str, Any]) -> bool:
        """Configure trigger-based CDC."""
        return True

    def _configure_api_based_cdc(self, source_config: Dict[str, Any]) -> bool:
        """Configure API-based CDC."""
        return True

    def _read_log_based_cdc(self, source_config: Dict[str, Any], from_lsn: Optional[str]) -> List[CDCEvent]:
        """Read log-based CDC events."""
        # Placeholder implementation
        return []

    def _read_trigger_based_cdc(self, source_config: Dict[str, Any], from_lsn: Optional[str]) -> List[CDCEvent]:
        """Read trigger-based CDC events."""
        return []

    def _read_api_based_cdc(self, source_config: Dict[str, Any], from_lsn: Optional[str]) -> List[CDCEvent]:
        """Read API-based CDC events."""
        return []

    def _apply_cdc_insert(self, target_table: str, event: CDCEvent) -> None:
        """Apply CDC insert."""
        pass

    def _apply_cdc_update(self, target_table: str, event: CDCEvent) -> None:
        """Apply CDC update."""
        pass

    def _apply_cdc_delete(self, target_table: str, event: CDCEvent) -> None:
        """Apply CDC delete."""
        pass

    def _get_watermark_history_db(self, pipeline_id: str, source_id: str,
                                watermark_column: str) -> List[Dict[str, Any]]:
        """Get watermark history."""
        if not self.db:
            return []

        try:
            query = """
                SELECT watermark_value, update_time
                FROM WATERMARKS
                WHERE pipeline_id = %s AND source_id = %s AND watermark_column = %s
                ORDER BY update_time DESC
                LIMIT 100
            """

            self.db.execute(query, (pipeline_id, source_id, watermark_column))
            rows = self.db.fetchall()

            return [{"watermark_value": row[0], "update_time": row[1]} for row in rows]

        except Exception as e:
            self.logger.exception(f"Database error getting watermark history: {e}")
            return []

    def _calculate_expected_gap(self, time_diff: datetime.timedelta, expected_interval: str) -> int:
        """Calculate expected number of records based on time difference."""
        # Placeholder calculation
        hours = time_diff.total_seconds() / 3600

        if 'hour' in expected_interval:
            records_per_hour = int(expected_interval.split()[0])
            return int(hours * records_per_hour)

        return int(hours * 100)  # Default assumption