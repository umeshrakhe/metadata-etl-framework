"""
Schema Manager Module

Provides database schema management, migrations, validation, and DDL operations
for the ETL framework. Handles schema versioning, integrity checks, and automated
schema updates.
"""

import logging
import hashlib
import os
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from .database_utils import DatabaseUtils, DatabaseType, ConnectionConfig, QueryResult


class SchemaVersionError(Exception):
    """Schema version related errors"""
    pass


class SchemaValidationError(Exception):
    """Schema validation related errors"""
    pass


class MigrationError(Exception):
    """Migration related errors"""
    pass


class SchemaStatus(Enum):
    """Schema status enumeration"""
    CURRENT = "current"
    OUTDATED = "outdated"
    MISSING = "missing"
    INVALID = "invalid"


@dataclass
class SchemaInfo:
    """Schema information"""
    name: str
    version: str
    checksum: str
    created_at: datetime
    updated_at: datetime
    status: SchemaStatus
    tables: List[str]
    description: Optional[str] = None


@dataclass
class MigrationStep:
    """Migration step information"""
    version: str
    description: str
    up_sql: str
    down_sql: Optional[str] = None
    checksum: str = ""
    applied_at: Optional[datetime] = None


@dataclass
class SchemaValidationResult:
    """Schema validation result"""
    is_valid: bool
    errors: List[str]
    warnings: List[str]
    missing_tables: List[str]
    extra_tables: List[str]
    invalid_columns: Dict[str, List[str]]


class SchemaManager:
    """
    Database schema manager for ETL framework.

    Handles schema creation, validation, migrations, and version management
    across different database types with unified interface.
    """

    def __init__(self, db_utils: DatabaseUtils, schema_name: str = "etl_metadata",
                 logger: Optional[logging.Logger] = None):
        """
        Initialize schema manager.

        Args:
            db_utils: Database utilities instance
            schema_name: Name of the schema to manage
            logger: Optional logger instance
        """
        self.db_utils = db_utils
        self.schema_name = schema_name
        self.logger = logger or logging.getLogger(__name__)

        # Schema version tracking table
        self.version_table = f"{schema_name}_schema_versions"

        # Initialize schema tracking if needed
        self._ensure_schema_tracking()

    def _ensure_schema_tracking(self):
        """Ensure schema version tracking table exists"""
        create_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {self.version_table} (
                schema_name VARCHAR(255) NOT NULL,
                version VARCHAR(50) NOT NULL,
                checksum VARCHAR(64) NOT NULL,
                description TEXT,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                applied_by VARCHAR(255),
                PRIMARY KEY (schema_name, version)
            );
        """
        if self.db_utils is None:
            raise SchemaVersionError("Database utilities not initialized")
        
        # Add indexes for better performance
        if self.db_utils.config.db_type == DatabaseType.POSTGRESQL:
            create_table_sql += f"""
                CREATE INDEX IF NOT EXISTS idx_{self.version_table}_schema_name
                ON {self.version_table}(schema_name);
                CREATE INDEX IF NOT EXISTS idx_{self.version_table}_applied_at
                ON {self.version_table}(applied_at);
            """
        elif self.db_utils.config.db_type == DatabaseType.MYSQL:
            create_table_sql += f"""
                CREATE INDEX idx_{self.version_table}_schema_name
                ON {self.version_table}(schema_name);
                CREATE INDEX idx_{self.version_table}_applied_at
                ON {self.version_table}(applied_at);
            """

        #print(create_table_sql)
        result = self.db_utils.execute_query(create_table_sql)
        if not result.success:
            self.logger.error(f"Failed to create schema tracking table: {result.error_message}")
            raise SchemaVersionError("Cannot initialize schema tracking")

    def get_current_schema_version(self, schema_name: Optional[str] = None) -> Optional[str]:
        """
        Get current version of a schema.

        Args:
            schema_name: Name of schema (defaults to managed schema)

        Returns:
            Current version string or None if not found
        """
        schema = schema_name or self.schema_name

        query = f"""
            SELECT version FROM {self.version_table}
            WHERE schema_name = %s
            ORDER BY applied_at DESC
            LIMIT 1
        """

        result = self.db_utils.execute_query(query, (schema,))

        if result.success and result.rows:
            return result.rows[0][0]

        return None

    def apply_schema_from_file(self, schema_file: str, version: str,
                             description: str = "") -> bool:
        """
        Apply schema from SQL file.

        Args:
            schema_file: Path to SQL schema file
            version: Schema version
            description: Optional description

        Returns:
            True if successful, False otherwise
        """
        try:
            # Read schema file
            with open(schema_file, 'r', encoding='utf-8') as f:
                schema_sql = f.read()

            # Calculate checksum
            checksum = hashlib.sha256(schema_sql.encode()).hexdigest()

            # Check if already applied
            current_version = self.get_current_schema_version()
            if current_version == version:
                self.logger.info(f"Schema version {version} already applied")
                return True

            # Split SQL into individual statements
            statements = self._split_sql_statements(schema_sql)

            # Execute statements
            self.logger.info(f"Applying schema version {version}")
            for i, statement in enumerate(statements, 1):
                if statement.strip():
                    self.logger.debug(f"Executing statement {i}/{len(statements)}")
                    result = self.db_utils.execute_query(statement)
                    if not result.success:
                        self.logger.error(f"Failed to execute statement {i}: {result.error_message}")
                        return False

            # Record schema application
            self._record_schema_version(version, checksum, description)

            self.logger.info(f"Successfully applied schema version {version}")
            return True

        except FileNotFoundError:
            self.logger.error(f"Schema file not found: {schema_file}")
            return False
        except Exception as e:
            self.logger.error(f"Failed to apply schema from file: {e}")
            return False

    def apply_migration(self, migration: MigrationStep) -> bool:
        """
        Apply a single migration step.

        Args:
            migration: Migration step to apply

        Returns:
            True if successful, False otherwise
        """
        try:
            # Check if already applied
            if self._is_migration_applied(migration.version):
                self.logger.info(f"Migration {migration.version} already applied")
                return True

            self.logger.info(f"Applying migration {migration.version}: {migration.description}")

            # Execute migration SQL
            result = self.db_utils.execute_query(migration.up_sql)
            if not result.success:
                self.logger.error(f"Failed to apply migration {migration.version}: {result.error_message}")
                return False

            # Record migration
            self._record_schema_version(
                migration.version,
                migration.checksum,
                migration.description
            )

            self.logger.info(f"Successfully applied migration {migration.version}")
            return True

        except Exception as e:
            self.logger.error(f"Migration application failed: {e}")
            return False

    def rollback_migration(self, version: str) -> bool:
        """
        Rollback a migration to previous version.

        Args:
            version: Version to rollback to

        Returns:
            True if successful, False otherwise
        """
        try:
            # Get migration info
            migration = self._get_migration_info(version)
            if not migration or not migration.down_sql:
                self.logger.error(f"No rollback SQL available for version {version}")
                return False

            self.logger.info(f"Rolling back migration {version}")

            # Execute rollback SQL
            result = self.db_utils.execute_query(migration.down_sql)
            if not result.success:
                self.logger.error(f"Failed to rollback migration {version}: {result.error_message}")
                return False

            # Remove version record
            delete_query = f"""
                DELETE FROM {self.version_table}
                WHERE schema_name = %s AND version = %s
            """
            self.db_utils.execute_query(delete_query, (self.schema_name, version))

            self.logger.info(f"Successfully rolled back migration {version}")
            return True

        except Exception as e:
            self.logger.error(f"Migration rollback failed: {e}")
            return False

    def validate_schema(self, expected_schema: Dict[str, Any]) -> SchemaValidationResult:
        """
        Validate current schema against expected structure.

        Args:
            expected_schema: Expected schema structure

        Returns:
            Validation result with details
        """
        result = SchemaValidationResult(
            is_valid=True,
            errors=[],
            warnings=[],
            missing_tables=[],
            extra_tables=[],
            invalid_columns={}
        )

        try:
            # Get current tables
            current_tables = self._get_current_tables()
            expected_tables = set(expected_schema.get('tables', {}).keys())

            # Check missing tables
            result.missing_tables = list(expected_tables - current_tables)
            if result.missing_tables:
                result.errors.append(f"Missing tables: {result.missing_tables}")
                result.is_valid = False

            # Check extra tables
            result.extra_tables = list(current_tables - expected_tables)
            if result.extra_tables:
                result.warnings.append(f"Extra tables found: {result.extra_tables}")

            # Validate table structures
            for table_name, expected_columns in expected_schema.get('tables', {}).items():
                if table_name in current_tables:
                    table_validation = self._validate_table_structure(table_name, expected_columns)
                    if not table_validation['valid']:
                        result.invalid_columns[table_name] = table_validation['errors']
                        result.errors.extend([f"{table_name}: {err}" for err in table_validation['errors']])
                        result.is_valid = False

        except Exception as e:
            result.is_valid = False
            result.errors.append(f"Schema validation failed: {e}")

        return result

    def get_schema_info(self, schema_name: Optional[str] = None) -> Optional[SchemaInfo]:
        """
        Get comprehensive schema information.

        Args:
            schema_name: Name of schema (defaults to managed schema)

        Returns:
            SchemaInfo object or None if not found
        """
        schema = schema_name or self.schema_name

        try:
            # Get version info
            version_query = f"""
                SELECT version, checksum, applied_at
                FROM {self.version_table}
                WHERE schema_name = %s
                ORDER BY applied_at DESC
                LIMIT 1
            """

            version_result = self.db_utils.execute_query(version_query, (schema,))

            if not version_result.success or not version_result.rows:
                return None

            version, checksum, applied_at = version_result.rows[0]

            # Get tables
            tables = self._get_current_tables()

            # Determine status (simplified - could be more sophisticated)
            status = SchemaStatus.CURRENT

            return SchemaInfo(
                name=schema,
                version=version,
                checksum=checksum,
                created_at=applied_at,
                updated_at=applied_at,
                status=status,
                tables=list(tables)
            )

        except Exception as e:
            self.logger.error(f"Failed to get schema info: {e}")
            return None

    def create_backup_script(self, output_file: str) -> bool:
        """
        Create database backup script.

        Args:
            output_file: Path to output backup file

        Returns:
            True if successful, False otherwise
        """
        try:
            tables = self._get_current_tables()
            backup_sql = []

            # Add schema version info
            backup_sql.append(f"-- Schema: {self.schema_name}")
            backup_sql.append(f"-- Generated: {datetime.now().isoformat()}")

            for table in tables:
                # Get table structure
                table_info = self.db_utils.get_table_info(table)
                if table_info:
                    backup_sql.append(f"\n-- Table: {table}")
                    backup_sql.append(f"DROP TABLE IF EXISTS {table};")

                    # Create table DDL (simplified)
                    columns = []
                    for col in table_info['columns']:
                        col_def = f"{col['column_name']} {col['data_type']}"
                        if col['is_nullable'] == 'NO':
                            col_def += " NOT NULL"
                        if col['column_default']:
                            col_def += f" DEFAULT {col['column_default']}"
                        columns.append(col_def)

                    backup_sql.append(f"CREATE TABLE {table} ({', '.join(columns)});")

                    # Get data
                    data_result = self.db_utils.execute_query(f"SELECT * FROM {table}")
                    if data_result.success and data_result.rows:
                        for row in data_result.rows:
                            # Escape values (simplified)
                            values = []
                            for val in row:
                                if val is None:
                                    values.append("NULL")
                                elif isinstance(val, str):
                                    values.append(f"'{val.replace(chr(39), chr(39)*2)}'")
                                else:
                                    values.append(str(val))

                            backup_sql.append(f"INSERT INTO {table} VALUES ({', '.join(values)});")

            # Write backup file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write('\n'.join(backup_sql))

            self.logger.info(f"Backup script created: {output_file}")
            return True

        except Exception as e:
            self.logger.error(f"Failed to create backup script: {e}")
            return False

    def _split_sql_statements(self, sql: str) -> List[str]:
        """Split SQL script into individual statements"""
        statements = []
        current_statement = []
        in_string = False
        string_char = None

        for char in sql:
            if not in_string:
                if char in ("'", '"'):
                    in_string = True
                    string_char = char
                elif char == ';':
                    statement = ''.join(current_statement).strip()
                    if statement:
                        statements.append(statement)
                    current_statement = []
                    continue
            else:
                if char == string_char:
                    in_string = False
                    string_char = None

            current_statement.append(char)

        # Add final statement if any
        final_statement = ''.join(current_statement).strip()
        if final_statement:
            statements.append(final_statement)

        return statements

    def _record_schema_version(self, version: str, checksum: str, description: str):
        """Record schema version application"""
        insert_query = f"""
            INSERT INTO {self.version_table}
            (schema_name, version, checksum, description, applied_by)
            VALUES (%s, %s, %s, %s, %s)
        """

        self.db_utils.execute_query(insert_query, (
            self.schema_name,
            version,
            checksum,
            description,
            os.getenv('USER', 'system')
        ))

    def _is_migration_applied(self, version: str) -> bool:
        """Check if migration version is already applied"""
        query = f"""
            SELECT 1 FROM {self.version_table}
            WHERE schema_name = %s AND version = %s
        """

        result = self.db_utils.execute_query(query, (self.schema_name, version))
        return result.success and len(result.rows) > 0

    def _get_migration_info(self, version: str) -> Optional[MigrationStep]:
        """Get migration information (placeholder for future implementation)"""
        # This would typically read from migration files or database
        return None

    def _get_current_tables(self) -> Set[str]:
        """Get set of current table names"""
        tables = set()

        try:
            if self.db_utils.config.db_type == DatabaseType.POSTGRESQL:
                query = """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                """
            elif self.db_utils.config.db_type == DatabaseType.MYSQL:
                query = """
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = DATABASE() AND table_type = 'BASE TABLE'
                """
            elif self.db_utils.config.db_type == DatabaseType.SQLITE:
                query = "SELECT name FROM sqlite_master WHERE type='table'"
            else:
                return tables

            result = self.db_utils.execute_query(query)
            if result.success:
                for row in result.rows:
                    tables.add(row[0])

        except Exception as e:
            self.logger.error(f"Failed to get current tables: {e}")

        return tables

    def _validate_table_structure(self, table_name: str,
                                expected_columns: Dict[str, Any]) -> Dict[str, Any]:
        """Validate table structure against expected columns"""
        result = {'valid': True, 'errors': []}

        try:
            table_info = self.db_utils.get_table_info(table_name)
            if not table_info:
                result['valid'] = False
                result['errors'].append("Table not found")
                return result

            current_columns = {col['column_name']: col for col in table_info['columns']}
            expected_names = set(expected_columns.keys())

            # Check missing columns
            missing = expected_names - set(current_columns.keys())
            for col in missing:
                result['errors'].append(f"Missing column: {col}")
                result['valid'] = False

            # Check column types (simplified)
            for col_name, expected_info in expected_columns.items():
                if col_name in current_columns:
                    current_info = current_columns[col_name]
                    expected_type = expected_info.get('type', '').upper()
                    current_type = current_info['data_type'].upper()

                    # Basic type matching (could be more sophisticated)
                    if expected_type and expected_type not in current_type:
                        result['errors'].append(
                            f"Column {col_name} type mismatch: expected {expected_type}, got {current_type}"
                        )
                        result['valid'] = False

        except Exception as e:
            result['valid'] = False
            result['errors'].append(f"Validation error: {e}")

        return result