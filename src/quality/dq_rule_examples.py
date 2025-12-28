import logging
import re
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple
from collections import Counter
import uuid

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
    import pandera as pa
    from pandera import Column, DataFrameSchema, Check
    PANDERA_AVAILABLE = True
except ImportError:
    pa = None
    PANDERA_AVAILABLE = False

try:
    import numpy as np
    NUMPY_AVAILABLE = True
except ImportError:
    np = None
    NUMPY_AVAILABLE = False

logger = logging.getLogger("DQRuleExamples")


class ValidationResult:
    """Container for data quality validation results."""

    def __init__(self, rule_name: str, passed: bool, total_records: int, failed_records: int,
                 failure_percentage: float, rule_score: float, sample_failures: List[Dict[str, Any]] = None,
                 details: Dict[str, Any] = None):
        self.rule_name = rule_name
        self.passed = passed
        self.total_records = total_records
        self.failed_records = failed_records
        self.failure_percentage = failure_percentage
        self.rule_score = rule_score
        self.sample_failures = sample_failures or []
        self.details = details or {}
        self.timestamp = datetime.utcnow()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "rule_name": self.rule_name,
            "passed": self.passed,
            "total_records": self.total_records,
            "failed_records": self.failed_records,
            "failure_percentage": self.failure_percentage,
            "rule_score": self.rule_score,
            "sample_failures": self.sample_failures,
            "details": self.details,
            "timestamp": self.timestamp.isoformat()
        }


class DQRuleExamples:
    """
    Concrete data quality rule examples using Pandera for comprehensive validation.
    Supports Pandas and Polars DataFrames with configurable thresholds and detailed reporting.
    """

    def __init__(self, db_connection: Any = None):
        self.db = db_connection
        self.logger = logging.getLogger(self.__class__.__name__)

    def _validate_dataframe(self, data: Any) -> None:
        """Validate that data is a supported DataFrame type."""
        if not (PANDAS_AVAILABLE and isinstance(data, pd.DataFrame)) and \
           not (POLARS_AVAILABLE and isinstance(data, pl.DataFrame)):
            raise ValueError("Data must be a Pandas or Polars DataFrame")

    def _log_validation_result(self, result: ValidationResult) -> None:
        """Log validation result to database if available."""
        if self.db:
            try:
                sql = """
                    INSERT INTO DQ_VALIDATION_RESULTS
                    (result_id, rule_name, passed, total_records, failed_records, failure_percentage, rule_score, details, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                result_id = str(uuid.uuid4())
                params = (
                    result_id,
                    result.rule_name,
                    result.passed,
                    result.total_records,
                    result.failed_records,
                    result.failure_percentage,
                    result.rule_score,
                    str(result.details),
                    result.timestamp,
                )
                self.db.begin()
                self.db.execute(sql, params)
                self.db.commit()
            except Exception:
                self.db.rollback()
                self.logger.exception("Failed to log validation result")

        self.logger.info(f"Validation result for {result.rule_name}: {'PASSED' if result.passed else 'FAILED'} "
                        f"({result.failure_percentage:.2f}% failures, score: {result.rule_score:.2f})")

    def _calculate_score(self, failure_percentage: float, threshold: float) -> float:
        """Calculate rule score based on failure percentage and threshold."""
        if failure_percentage == 0:
            return 100.0
        elif failure_percentage <= threshold:
            return 100.0 * (1 - failure_percentage / threshold)
        else:
            return max(0.0, 50.0 - (failure_percentage - threshold) * 2)

    def _get_sample_failures(self, data: Any, failure_mask: Any, max_samples: int = 5) -> List[Dict[str, Any]]:
        """Get sample failed records."""
        if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
            failed_data = data[failure_mask]
            if len(failed_data) == 0:
                return []
            sample = failed_data.head(max_samples)
            return sample.to_dict('records')
        elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
            failed_data = data.filter(failure_mask)
            if len(failed_data) == 0:
                return []
            sample = failed_data.head(max_samples)
            return sample.to_dicts()
        return []

    # 1. Completeness Rules
    def check_not_null(self, data: Any, columns: List[str], threshold_percent: float = 95.0) -> ValidationResult:
        """Check that specified columns have no null values above threshold."""
        self._validate_dataframe(data)
        rule_name = f"not_null_{'_'.join(columns)}"

        try:
            total_records = len(data)
            failed_records = 0
            failure_details = {}

            for col in columns:
                if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                    null_count = data[col].isnull().sum()
                    null_percent = (null_count / total_records) * 100
                elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                    null_count = data.select(pl.col(col).is_null().sum()).item()
                    null_percent = (null_count / total_records) * 100

                if null_percent > (100 - threshold_percent):
                    failed_records += null_count
                    failure_details[col] = {
                        "null_count": int(null_count),
                        "null_percentage": null_percent
                    }

            failure_percentage = (failed_records / (total_records * len(columns))) * 100
            passed = failure_percentage <= (100 - threshold_percent)
            rule_score = self._calculate_score(failure_percentage, 100 - threshold_percent)

            # Get sample failures
            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                failure_mask = data[columns].isnull().any(axis=1)
            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                failure_mask = pl.any_horizontal([pl.col(col).is_null() for col in columns])

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"column_details": failure_details, "threshold_percent": threshold_percent}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Not null check failed: {e}")
            raise

    def check_required_fields(self, data: Any, required_columns: List[str]) -> ValidationResult:
        """Check that all required fields are present and not null."""
        self._validate_dataframe(data)
        rule_name = f"required_fields_{'_'.join(required_columns)}"

        try:
            total_records = len(data)
            failed_records = 0
            missing_columns = []
            null_details = {}

            # Check if columns exist
            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                existing_columns = set(data.columns)
            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                existing_columns = set(data.columns)

            for col in required_columns:
                if col not in existing_columns:
                    missing_columns.append(col)
                    failed_records = total_records  # All records fail if column is missing
                else:
                    # Check for nulls in existing required columns
                    if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                        null_count = data[col].isnull().sum()
                    elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                        null_count = data.select(pl.col(col).is_null().sum()).item()

                    failed_records += null_count
                    if null_count > 0:
                        null_details[col] = int(null_count)

            failure_percentage = (failed_records / (total_records * len(required_columns))) * 100
            passed = len(missing_columns) == 0 and failed_records == 0
            rule_score = 100.0 if passed else max(0.0, 100.0 - failure_percentage)

            # Get sample failures
            if len(missing_columns) > 0:
                sample_failures = [{"error": f"Missing required columns: {missing_columns}"}]
            else:
                failure_conditions = []
                for col in required_columns:
                    if col in existing_columns:
                        if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                            failure_conditions.append(data[col].isnull())
                        elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                            failure_conditions.append(pl.col(col).is_null())

                if failure_conditions:
                    if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                        failure_mask = pd.concat(failure_conditions, axis=1).any(axis=1)
                    elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                        failure_mask = pl.any_horizontal(failure_conditions)

                    sample_failures = self._get_sample_failures(data, failure_mask)
                else:
                    sample_failures = []

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"missing_columns": missing_columns, "null_details": null_details}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Required fields check failed: {e}")
            raise

    def check_completeness_score(self, data: Any, columns: List[str], min_score: float = 80.0) -> ValidationResult:
        """Calculate overall completeness score for specified columns."""
        self._validate_dataframe(data)
        rule_name = f"completeness_score_{'_'.join(columns)}"

        try:
            total_records = len(data)
            total_cells = total_records * len(columns)
            null_cells = 0

            completeness_details = {}

            for col in columns:
                if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                    null_count = data[col].isnull().sum()
                elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                    null_count = data.select(pl.col(col).is_null().sum()).item()

                null_cells += null_count
                completeness_details[col] = {
                    "null_count": int(null_count),
                    "completeness_percent": ((total_records - null_count) / total_records) * 100
                }

            completeness_score = ((total_cells - null_cells) / total_cells) * 100
            passed = completeness_score >= min_score
            failure_percentage = 100 - completeness_score
            rule_score = completeness_score

            # Get sample failures - records with most nulls
            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                null_counts_per_row = data[columns].isnull().sum(axis=1)
                failure_mask = null_counts_per_row > 0
            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                null_counts_per_row = data.select([pl.col(col).is_null().cast(pl.Int32) for col in columns]).sum_horizontal()
                failure_mask = null_counts_per_row > 0

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=null_cells,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"completeness_score": completeness_score, "min_score": min_score, "column_details": completeness_details}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Completeness score check failed: {e}")
            raise

    # 2. Validity Rules
    def check_data_type(self, data: Any, column: str, expected_type: str) -> ValidationResult:
        """Check that column values match expected data type."""
        self._validate_dataframe(data)
        rule_name = f"data_type_{column}_{expected_type}"

        try:
            total_records = len(data)
            failed_records = 0

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                if expected_type == "numeric":
                    failure_mask = pd.to_numeric(data[column], errors='coerce').isnull() & data[column].notna()
                elif expected_type == "string":
                    failure_mask = data[column].astype(str).isnull()
                elif expected_type == "datetime":
                    failure_mask = pd.to_datetime(data[column], errors='coerce').isnull() & data[column].notna()
                else:
                    failure_mask = pd.Series([False] * total_records)
                failed_records = failure_mask.sum()

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                if expected_type == "numeric":
                    failure_mask = pl.col(column).cast(pl.Float64, strict=False).is_null() & pl.col(column).is_not_null()
                elif expected_type == "string":
                    failure_mask = pl.col(column).cast(pl.Utf8, strict=False).is_null()
                elif expected_type == "datetime":
                    failure_mask = pl.col(column).str.strptime(pl.Datetime, "%Y-%m-%d", strict=False).is_null() & pl.col(column).is_not_null()
                else:
                    failure_mask = pl.lit(False)
                failed_records = data.select(failure_mask.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"expected_type": expected_type}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Data type check failed: {e}")
            raise

    def check_value_range(self, data: Any, column: str, min_value: Optional[float] = None,
                         max_value: Optional[float] = None) -> ValidationResult:
        """Check that numeric values fall within specified range."""
        self._validate_dataframe(data)
        rule_name = f"value_range_{column}_{min_value}_{max_value}"

        try:
            total_records = len(data)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                numeric_col = pd.to_numeric(data[column], errors='coerce')
                failure_conditions = []
                if min_value is not None:
                    failure_conditions.append(numeric_col < min_value)
                if max_value is not None:
                    failure_conditions.append(numeric_col > max_value)
                failure_conditions.append(numeric_col.isnull() & data[column].notna())

                if failure_conditions:
                    failure_mask = pd.concat(failure_conditions, axis=1).any(axis=1)
                else:
                    failure_mask = pd.Series([False] * total_records)

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                numeric_col = pl.col(column).cast(pl.Float64, strict=False)
                failure_conditions = []
                if min_value is not None:
                    failure_conditions.append(numeric_col < min_value)
                if max_value is not None:
                    failure_conditions.append(numeric_col > max_value)
                failure_conditions.append(numeric_col.is_null() & pl.col(column).is_not_null())

                if failure_conditions:
                    failure_mask = pl.any_horizontal(failure_conditions)
                else:
                    failure_mask = pl.lit(False)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                failed_records = failure_mask.sum()
            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                failed_records = data.select(failure_mask.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"min_value": min_value, "max_value": max_value}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Value range check failed: {e}")
            raise

    def check_regex_pattern(self, data: Any, column: str, pattern: str) -> ValidationResult:
        """Check that string values match regex pattern."""
        self._validate_dataframe(data)
        rule_name = f"regex_pattern_{column}"

        try:
            total_records = len(data)
            compiled_pattern = re.compile(pattern)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                string_col = data[column].astype(str)
                failure_mask = ~string_col.str.match(pattern, na=False) & data[column].notna()
                failed_records = failure_mask.sum()

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                failure_mask = pl.col(column).str.contains(f"^({pattern})$", literal=False).is_not() & pl.col(column).is_not_null()
                failed_records = data.select(failure_mask.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"pattern": pattern}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Regex pattern check failed: {e}")
            raise

    def check_allowed_values(self, data: Any, column: str, allowed_list: List[Any]) -> ValidationResult:
        """Check that values are in allowed list."""
        self._validate_dataframe(data)
        rule_name = f"allowed_values_{column}"

        try:
            total_records = len(data)
            allowed_set = set(allowed_list)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                failure_mask = ~data[column].isin(allowed_set) & data[column].notna()
                failed_records = failure_mask.sum()

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                failure_mask = ~pl.col(column).is_in(allowed_set) & pl.col(column).is_not_null()
                failed_records = data.select(failure_mask.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"allowed_values": allowed_list}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Allowed values check failed: {e}")
            raise

    def check_date_format(self, data: Any, column: str, date_format: str) -> ValidationResult:
        """Check that date values match specified format."""
        self._validate_dataframe(data)
        rule_name = f"date_format_{column}_{date_format}"

        try:
            total_records = len(data)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                try:
                    parsed_dates = pd.to_datetime(data[column], format=date_format, errors='coerce')
                    failure_mask = parsed_dates.isnull() & data[column].notna()
                    failed_records = failure_mask.sum()
                except Exception:
                    failure_mask = pd.Series([True] * total_records)
                    failed_records = total_records

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                try:
                    parsed_dates = pl.col(column).str.strptime(pl.Datetime, date_format, strict=False)
                    failure_mask = parsed_dates.is_null() & pl.col(column).is_not_null()
                    failed_records = data.select(failure_mask.sum()).item()
                except Exception:
                    failure_mask = pl.lit(True)
                    failed_records = total_records

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"date_format": date_format}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Date format check failed: {e}")
            raise

    def check_email_format(self, data: Any, column: str) -> ValidationResult:
        """Check that values are valid email addresses."""
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return self.check_regex_pattern(data, column, email_pattern)

    def check_phone_format(self, data: Any, column: str, country_code: str = "US") -> ValidationResult:
        """Check that values are valid phone numbers."""
        # Simple phone pattern - can be extended for different countries
        if country_code == "US":
            phone_pattern = r'^\+?1?[-.\s]?\(?([0-9]{3})\)?[-.\s]?([0-9]{3})[-.\s]?([0-9]{4})$'
        else:
            phone_pattern = r'^\+?[0-9\s\-\(\)]+$'  # Generic pattern

        return self.check_regex_pattern(data, column, phone_pattern)

    # 3. Uniqueness Rules
    def check_primary_key_unique(self, data: Any, key_columns: List[str]) -> ValidationResult:
        """Check that primary key columns contain unique combinations."""
        self._validate_dataframe(data)
        rule_name = f"primary_key_unique_{'_'.join(key_columns)}"

        try:
            total_records = len(data)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                duplicates = data.duplicated(subset=key_columns, keep=False)
                failed_records = duplicates.sum()

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                duplicates = data.is_duplicated()
                failed_records = data.select(duplicates.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, duplicates)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"key_columns": key_columns}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Primary key unique check failed: {e}")
            raise

    def check_duplicate_rows(self, data: Any, subset_columns: Optional[List[str]] = None,
                           threshold_percent: float = 5.0) -> ValidationResult:
        """Check for duplicate rows, optionally within subset of columns."""
        self._validate_dataframe(data)
        subset_str = '_'.join(subset_columns) if subset_columns else 'all'
        rule_name = f"duplicate_rows_{subset_str}"

        try:
            total_records = len(data)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                duplicates = data.duplicated(subset=subset_columns, keep=False)
                failed_records = duplicates.sum()

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                if subset_columns:
                    duplicates = data.select(subset_columns).is_duplicated()
                else:
                    duplicates = data.is_duplicated()
                failed_records = data.select(duplicates.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failure_percentage <= threshold_percent
            rule_score = self._calculate_score(failure_percentage, threshold_percent)

            sample_failures = self._get_sample_failures(data, duplicates)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"subset_columns": subset_columns, "threshold_percent": threshold_percent}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Duplicate rows check failed: {e}")
            raise

    def check_unique_count(self, data: Any, column: str, min_unique_percent: float = 50.0) -> ValidationResult:
        """Check that column has sufficient unique values."""
        self._validate_dataframe(data)
        rule_name = f"unique_count_{column}"

        try:
            total_records = len(data)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                unique_count = data[column].nunique()
            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                unique_count = data.select(pl.col(column).n_unique()).item()

            unique_percentage = (unique_count / total_records) * 100
            passed = unique_percentage >= min_unique_percent
            failure_percentage = max(0, min_unique_percent - unique_percentage)
            rule_score = unique_percentage

            # For this rule, "failures" are records that are duplicates
            failed_records = total_records - unique_count

            # Sample failures are the most common duplicate values
            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                value_counts = data[column].value_counts()
                duplicates = value_counts[value_counts > 1]
                sample_failures = [{"value": idx, "count": count} for idx, count in duplicates.head(5).items()]
            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                value_counts = data.group_by(column).agg(pl.len().alias("count")).filter(pl.col("count") > 1)
                sample_failures = value_counts.sort("count", descending=True).head(5).to_dicts()

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"unique_count": unique_count, "unique_percentage": unique_percentage, "min_unique_percent": min_unique_percent}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Unique count check failed: {e}")
            raise

    # 4. Consistency Rules
    def check_referential_integrity(self, data: Any, foreign_key: str, reference_data: Any,
                                  primary_key: str) -> ValidationResult:
        """Check referential integrity between datasets."""
        self._validate_dataframe(data)
        self._validate_dataframe(reference_data)
        rule_name = f"referential_integrity_{foreign_key}_{primary_key}"

        try:
            total_records = len(data)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame) and isinstance(reference_data, pd.DataFrame):
                reference_values = set(reference_data[primary_key].dropna())
                failure_mask = ~data[foreign_key].isin(reference_values) & data[foreign_key].notna()
                failed_records = failure_mask.sum()

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame) and isinstance(reference_data, pl.DataFrame):
                reference_values = reference_data.select(pl.col(primary_key)).unique().to_series().to_list()
                failure_mask = ~pl.col(foreign_key).is_in(reference_values) & pl.col(foreign_key).is_not_null()
                failed_records = data.select(failure_mask.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"foreign_key": foreign_key, "primary_key": primary_key}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Referential integrity check failed: {e}")
            raise

    def check_cross_field_logic(self, data: Any, field1: str, operator: str, field2: str) -> ValidationResult:
        """Check logical relationships between fields."""
        self._validate_dataframe(data)
        rule_name = f"cross_field_logic_{field1}_{operator}_{field2}"

        try:
            total_records = len(data)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                if operator == ">":
                    failure_mask = ~(data[field1] > data[field2]) & data[field1].notna() & data[field2].notna()
                elif operator == "<":
                    failure_mask = ~(data[field1] < data[field2]) & data[field1].notna() & data[field2].notna()
                elif operator == "==":
                    failure_mask = ~(data[field1] == data[field2]) & data[field1].notna() & data[field2].notna()
                elif operator == ">=":
                    failure_mask = ~(data[field1] >= data[field2]) & data[field1].notna() & data[field2].notna()
                elif operator == "<=":
                    failure_mask = ~(data[field1] <= data[field2]) & data[field1].notna() & data[field2].notna()
                else:
                    failure_mask = pd.Series([False] * total_records)
                failed_records = failure_mask.sum()

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                if operator == ">":
                    failure_mask = ~((pl.col(field1) > pl.col(field2)) & pl.col(field1).is_not_null() & pl.col(field2).is_not_null()) | (pl.col(field1).is_null() | pl.col(field2).is_null())
                elif operator == "<":
                    failure_mask = ~((pl.col(field1) < pl.col(field2)) & pl.col(field1).is_not_null() & pl.col(field2).is_not_null()) | (pl.col(field1).is_null() | pl.col(field2).is_null())
                elif operator == "==":
                    failure_mask = ~((pl.col(field1) == pl.col(field2)) & pl.col(field1).is_not_null() & pl.col(field2).is_not_null()) | (pl.col(field1).is_null() | pl.col(field2).is_null())
                elif operator == ">=":
                    failure_mask = ~((pl.col(field1) >= pl.col(field2)) & pl.col(field1).is_not_null() & pl.col(field2).is_not_null()) | (pl.col(field1).is_null() | pl.col(field2).is_null())
                elif operator == "<=":
                    failure_mask = ~((pl.col(field1) <= pl.col(field2)) & pl.col(field1).is_not_null() & pl.col(field2).is_not_null()) | (pl.col(field1).is_null() | pl.col(field2).is_null())
                else:
                    failure_mask = pl.lit(False)
                failed_records = data.select(failure_mask.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"field1": field1, "operator": operator, "field2": field2}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Cross field logic check failed: {e}")
            raise

    def check_sum_equals(self, data: Any, columns: List[str], expected_sum: float,
                        tolerance: float = 0.01) -> ValidationResult:
        """Check that sum of specified columns equals expected value."""
        self._validate_dataframe(data)
        rule_name = f"sum_equals_{'_'.join(columns)}"

        try:
            total_records = len(data)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                row_sums = data[columns].sum(axis=1)
                failure_mask = ~((row_sums >= expected_sum - tolerance) & (row_sums <= expected_sum + tolerance))
                failed_records = failure_mask.sum()

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                row_sums = data.select([pl.col(col).cast(pl.Float64) for col in columns]).sum_horizontal()
                failure_mask = ~((row_sums >= expected_sum - tolerance) & (row_sums <= expected_sum + tolerance))
                failed_records = data.select(failure_mask.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"columns": columns, "expected_sum": expected_sum, "tolerance": tolerance}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Sum equals check failed: {e}")
            raise

    def check_date_sequence(self, data: Any, start_date_col: str, end_date_col: str) -> ValidationResult:
        """Check that start dates are before end dates."""
        self._validate_dataframe(data)
        rule_name = f"date_sequence_{start_date_col}_{end_date_col}"

        try:
            total_records = len(data)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                start_dates = pd.to_datetime(data[start_date_col], errors='coerce')
                end_dates = pd.to_datetime(data[end_date_col], errors='coerce')
                failure_mask = (start_dates > end_dates) & start_dates.notna() & end_dates.notna()
                failed_records = failure_mask.sum()

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                start_dates = pl.col(start_date_col).str.strptime(pl.Datetime, "%Y-%m-%d", strict=False)
                end_dates = pl.col(end_date_col).str.strptime(pl.Datetime, "%Y-%m-%d", strict=False)
                failure_mask = (start_dates > end_dates) & start_dates.is_not_null() & end_dates.is_not_null()
                failed_records = data.select(failure_mask.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"start_date_col": start_date_col, "end_date_col": end_date_col}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Date sequence check failed: {e}")
            raise

    # 5. Accuracy Rules
    def check_calculated_field(self, data: Any, field: str, calculation_logic: str) -> ValidationResult:
        """Check that calculated field matches expected computation."""
        self._validate_dataframe(data)
        rule_name = f"calculated_field_{field}"

        try:
            total_records = len(data)

            # This is a simplified implementation - in practice, you'd parse calculation_logic
            # For demo purposes, assume simple calculations like "field1 + field2"
            if "+" in calculation_logic:
                parts = calculation_logic.split("+")
                if len(parts) == 2:
                    col1, col2 = parts[0].strip(), parts[1].strip()
                    if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                        expected = pd.to_numeric(data[col1], errors='coerce') + pd.to_numeric(data[col2], errors='coerce')
                        actual = pd.to_numeric(data[field], errors='coerce')
                        failure_mask = ~(abs(expected - actual) < 0.01) & expected.notna() & actual.notna()
                        failed_records = failure_mask.sum()
                    elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                        expected = pl.col(col1).cast(pl.Float64) + pl.col(col2).cast(pl.Float64)
                        actual = pl.col(field).cast(pl.Float64)
                        failure_mask = ~((expected - actual).abs() < 0.01) & expected.is_not_null() & actual.is_not_null()
                        failed_records = data.select(failure_mask.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"field": field, "calculation_logic": calculation_logic}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Calculated field check failed: {e}")
            raise

    def check_statistical_distribution(self, data: Any, column: str, expected_distribution: str) -> ValidationResult:
        """Check that data follows expected statistical distribution."""
        self._validate_dataframe(data)
        rule_name = f"statistical_distribution_{column}_{expected_distribution}"

        try:
            total_records = len(data)

            if not NUMPY_AVAILABLE:
                raise ImportError("NumPy required for statistical distribution checks")

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                values = data[column].dropna()
            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                values = data.select(pl.col(column)).filter(pl.col(column).is_not_null()).to_series().to_numpy()

            if len(values) < 10:
                passed = False
                failure_percentage = 100.0
                rule_score = 0.0
                failed_records = total_records
                sample_failures = [{"error": "Insufficient data for distribution test"}]
            else:
                # Simple distribution check - in practice, use statistical tests
                if expected_distribution == "normal":
                    from scipy import stats
                    _, p_value = stats.shapiro(values[:5000])  # Shapiro-Wilk test
                    passed = p_value > 0.05
                else:
                    passed = True  # Placeholder for other distributions

                failure_percentage = 0.0 if passed else 50.0
                rule_score = 100.0 if passed else 50.0
                failed_records = 0 if passed else total_records // 2
                sample_failures = []

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"column": column, "expected_distribution": expected_distribution}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Statistical distribution check failed: {e}")
            raise

    def check_correlation(self, data: Any, col1: str, col2: str, min_correlation: float = 0.5) -> ValidationResult:
        """Check correlation between two columns."""
        self._validate_dataframe(data)
        rule_name = f"correlation_{col1}_{col2}"

        try:
            total_records = len(data)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                corr = data[col1].corr(data[col2])
            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                corr = data.select(pl.corr(col1, col2)).item()

            passed = abs(corr) >= min_correlation
            failure_percentage = max(0, (min_correlation - abs(corr)) * 100)
            rule_score = abs(corr) * 100
            failed_records = 0 if passed else total_records  # All records "fail" if correlation is too low

            sample_failures = [] if passed else [{"correlation": corr, "min_required": min_correlation}]

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"col1": col1, "col2": col2, "correlation": corr, "min_correlation": min_correlation}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Correlation check failed: {e}")
            raise

    # 6. Timeliness Rules
    def check_data_freshness(self, data: Any, timestamp_column: str, max_age_hours: int = 24) -> ValidationResult:
        """Check that data is fresh within specified time window."""
        self._validate_dataframe(data)
        rule_name = f"data_freshness_{timestamp_column}"

        try:
            total_records = len(data)
            current_time = datetime.utcnow()
            max_age = timedelta(hours=max_age_hours)

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                timestamps = pd.to_datetime(data[timestamp_column], errors='coerce')
                age = current_time - timestamps
                failure_mask = (age > max_age) & timestamps.notna()
                failed_records = failure_mask.sum()

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                timestamps = pl.col(timestamp_column).str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
                age_hours = (current_time - timestamps).dt.total_hours()
                failure_mask = (age_hours > max_age_hours) & timestamps.is_not_null()
                failed_records = data.select(failure_mask.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"timestamp_column": timestamp_column, "max_age_hours": max_age_hours}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Data freshness check failed: {e}")
            raise

    def check_future_dates(self, data: Any, date_column: str, allow_future: bool = False) -> ValidationResult:
        """Check for future dates when they shouldn't exist."""
        self._validate_dataframe(data)
        rule_name = f"future_dates_{date_column}"

        try:
            total_records = len(data)
            current_time = datetime.utcnow()

            if PANDAS_AVAILABLE and isinstance(data, pd.DataFrame):
                dates = pd.to_datetime(data[date_column], errors='coerce')
                if allow_future:
                    failure_mask = pd.Series([False] * total_records)
                else:
                    failure_mask = (dates > current_time) & dates.notna()
                failed_records = failure_mask.sum()

            elif POLARS_AVAILABLE and isinstance(data, pl.DataFrame):
                dates = pl.col(date_column).str.strptime(pl.Datetime, "%Y-%m-%d", strict=False)
                if allow_future:
                    failure_mask = pl.lit(False)
                else:
                    failure_mask = (dates > current_time) & dates.is_not_null()
                failed_records = data.select(failure_mask.sum()).item()

            failure_percentage = (failed_records / total_records) * 100
            passed = failed_records == 0
            rule_score = 100.0 - failure_percentage

            sample_failures = self._get_sample_failures(data, failure_mask)

            result = ValidationResult(
                rule_name=rule_name,
                passed=passed,
                total_records=total_records,
                failed_records=failed_records,
                failure_percentage=failure_percentage,
                rule_score=rule_score,
                sample_failures=sample_failures,
                details={"date_column": date_column, "allow_future": allow_future}
            )

            self._log_validation_result(result)
            return result

        except Exception as e:
            self.logger.exception(f"Future dates check failed: {e}")
            raise

    # 7. Custom Pandera Schema Examples
    def create_schema_from_metadata(self, dq_rules: List[Dict[str, Any]]) -> DataFrameSchema:
        """Create Pandera schema from metadata rules configuration."""
        if not PANDERA_AVAILABLE:
            raise ImportError("Pandera not available")

        schema_dict = {}

        for rule in dq_rules:
            column = rule.get("column")
            rule_type = rule.get("type")

            if rule_type == "not_null":
                schema_dict[column] = Column(pa.String, nullable=False)
            elif rule_type == "data_type":
                if rule.get("expected_type") == "numeric":
                    schema_dict[column] = Column(pa.Float64)
                elif rule.get("expected_type") == "string":
                    schema_dict[column] = Column(pa.String)
            elif rule_type == "value_range":
                min_val = rule.get("min_value")
                max_val = rule.get("max_value")
                checks = []
                if min_val is not None:
                    checks.append(Check.ge(min_val))
                if max_val is not None:
                    checks.append(Check.le(max_val))
                schema_dict[column] = Column(pa.Float64, checks=checks)
            elif rule_type == "regex_pattern":
                pattern = rule.get("pattern")
                schema_dict[column] = Column(pa.String, checks=[Check.str_matches(pattern)])
            elif rule_type == "allowed_values":
                allowed = rule.get("allowed_list", [])
                schema_dict[column] = Column(pa.String, checks=[Check.isin(allowed)])

        return DataFrameSchema(schema_dict)

    def schema_with_custom_checks(self, rules_config: Dict[str, Any]) -> DataFrameSchema:
        """Create schema with custom Pandera checks."""
        if not PANDERA_AVAILABLE:
            raise ImportError("Pandera not available")

        def custom_completeness_check(df):
            """Custom check for completeness across multiple columns."""
            completeness_threshold = rules_config.get("completeness_threshold", 0.95)
            required_cols = rules_config.get("required_columns", [])

            for col in required_cols:
                if col in df.columns:
                    completeness = df[col].notna().mean()
                    if completeness < completeness_threshold:
                        return False
            return True

        def custom_uniqueness_check(df):
            """Custom check for uniqueness constraints."""
            unique_cols = rules_config.get("unique_columns", [])
            for col in unique_cols:
                if col in df.columns and df[col].duplicated().any():
                    return False
            return True

        schema = DataFrameSchema(
            checks=[
                Check(custom_completeness_check, name="custom_completeness"),
                Check(custom_uniqueness_check, name="custom_uniqueness")
            ]
        )

        return schema

    def schema_with_hypothesis_tests(self, statistical_tests: List[Dict[str, Any]]) -> DataFrameSchema:
        """Create schema with statistical hypothesis tests."""
        if not PANDERA_AVAILABLE:
            raise ImportError("Pandera not available")

        checks = []

        for test in statistical_tests:
            test_type = test.get("type")
            column = test.get("column")

            if test_type == "normal_distribution":
                def normality_test(df):
                    if NUMPY_AVAILABLE:
                        from scipy import stats
                        data = df[column].dropna()
                        if len(data) > 3:
                            _, p_value = stats.shapiro(data)
                            return p_value > 0.05
                    return True

                checks.append(Check(normality_test, name=f"normality_{column}"))

            elif test_type == "correlation":
                col1 = test.get("col1")
                col2 = test.get("col2")
                min_corr = test.get("min_correlation", 0.5)

                def correlation_test(df):
                    corr = df[col1].corr(df[col2])
                    return abs(corr) >= min_corr

                checks.append(Check(correlation_test, name=f"correlation_{col1}_{col2}"))

        return DataFrameSchema(checks=checks)

    def run_comprehensive_profile(self, data: Any, profile_config: Dict[str, Any]) -> Dict[str, ValidationResult]:
        """Run comprehensive data quality profile with multiple rules."""
        results = {}

        # Completeness rules
        if "completeness" in profile_config:
            completeness_config = profile_config["completeness"]
            for rule in completeness_config:
                if rule["type"] == "not_null":
                    results[f"completeness_{rule['column']}"] = self.check_not_null(
                        data, [rule["column"]], rule.get("threshold", 95.0)
                    )
                elif rule["type"] == "required_fields":
                    results["required_fields"] = self.check_required_fields(
                        data, rule["columns"]
                    )

        # Validity rules
        if "validity" in profile_config:
            validity_config = profile_config["validity"]
            for rule in validity_config:
                if rule["type"] == "data_type":
                    results[f"validity_{rule['column']}"] = self.check_data_type(
                        data, rule["column"], rule["expected_type"]
                    )
                elif rule["type"] == "value_range":
                    results[f"validity_{rule['column']}"] = self.check_value_range(
                        data, rule["column"], rule.get("min_value"), rule.get("max_value")
                    )
                elif rule["type"] == "regex_pattern":
                    results[f"validity_{rule['column']}"] = self.check_regex_pattern(
                        data, rule["column"], rule["pattern"]
                    )

        # Uniqueness rules
        if "uniqueness" in profile_config:
            uniqueness_config = profile_config["uniqueness"]
            for rule in uniqueness_config:
                if rule["type"] == "primary_key":
                    results["primary_key_uniqueness"] = self.check_primary_key_unique(
                        data, rule["columns"]
                    )
                elif rule["type"] == "unique_count":
                    results[f"uniqueness_{rule['column']}"] = self.check_unique_count(
                        data, rule["column"], rule.get("min_percent", 50.0)
                    )

        # Consistency rules
        if "consistency" in profile_config:
            consistency_config = profile_config["consistency"]
            for rule in consistency_config:
                if rule["type"] == "cross_field":
                    results[f"consistency_{rule['field1']}_{rule['field2']}"] = self.check_cross_field_logic(
                        data, rule["field1"], rule["operator"], rule["field2"]
                    )
                elif rule["type"] == "date_sequence":
                    results[f"consistency_{rule['start']}_{rule['end']}"] = self.check_date_sequence(
                        data, rule["start"], rule["end"]
                    )

        # Accuracy rules
        if "accuracy" in profile_config:
            accuracy_config = profile_config["accuracy"]
            for rule in accuracy_config:
                if rule["type"] == "correlation":
                    results[f"accuracy_{rule['col1']}_{rule['col2']}"] = self.check_correlation(
                        data, rule["col1"], rule["col2"], rule.get("min_correlation", 0.5)
                    )

        # Timeliness rules
        if "timeliness" in profile_config:
            timeliness_config = profile_config["timeliness"]
            for rule in timeliness_config:
                if rule["type"] == "freshness":
                    results[f"timeliness_{rule['column']}"] = self.check_data_freshness(
                        data, rule["column"], rule.get("max_age_hours", 24)
                    )

        return results