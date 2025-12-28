"""
Database Utilities Module

Provides common database operations, connection management, and utility functions
for the ETL framework. Supports multiple database backends with unified interface.
"""

import logging
import time
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum

# Optional imports for database drivers
try:
    import psycopg2
    import psycopg2.extras
    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False

try:
    import pymysql
    HAS_MYSQL = True
except ImportError:
    HAS_MYSQL = False

try:
    import sqlite3
    HAS_SQLITE = True
except ImportError:
    HAS_SQLITE = False

try:
    import pandas as pd
    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False


class DatabaseType(Enum):
    """Supported database types"""
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    SQLITE = "sqlite"
    ORACLE = "oracle"
    SQLSERVER = "sqlserver"


@dataclass
class ConnectionConfig:
    """Database connection configuration"""
    host: str
    port: int
    database: str
    username: str
    password: str
    db_type: DatabaseType
    ssl_mode: str = "prefer"
    connection_timeout: int = 30
    max_connections: int = 10
    connection_pool: bool = True


@dataclass
class QueryResult:
    """Query execution result"""
    rows: List[Tuple]
    columns: List[str]
    row_count: int
    execution_time: float
    success: bool
    error_message: Optional[str] = None


class DatabaseConnectionError(Exception):
    """Database connection related errors"""
    pass


class DatabaseQueryError(Exception):
    """Database query execution errors"""
    pass


class DatabaseUtils:
    """
    Unified database utilities for ETL operations.

    Provides connection management, query execution, data loading,
    and common database operations across multiple database types.
    """

    def __init__(self, config: ConnectionConfig, logger: Optional[logging.Logger] = None):
        """
        Initialize database utilities.

        Args:
            config: Database connection configuration
            logger: Optional logger instance
        """
        self.config = config
        self.logger = logger or logging.getLogger(__name__)
        self._connection_pool = []
        self._active_connections = 0

        # Validate driver availability
        self._validate_driver_availability()

    def _validate_driver_availability(self):
        """Validate that required database driver is available"""
        if self.config.db_type == DatabaseType.POSTGRESQL and not HAS_POSTGRES:
            raise ImportError("psycopg2 is required for PostgreSQL connections")
        elif self.config.db_type == DatabaseType.MYSQL and not HAS_MYSQL:
            raise ImportError("pymysql is required for MySQL connections")
        elif self.config.db_type == DatabaseType.SQLITE and not HAS_SQLITE:
            raise ImportError("sqlite3 is built-in, should be available")

    def _create_connection(self):
        """Create a new database connection"""
        try:
            if self.config.db_type == DatabaseType.POSTGRESQL:
                conn = psycopg2.connect(
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    user=self.config.username,
                    password=self.config.password,
                    sslmode=self.config.ssl_mode,
                    connect_timeout=self.config.connection_timeout
                )
            elif self.config.db_type == DatabaseType.MYSQL:
                conn = pymysql.connect(
                    host=self.config.host,
                    port=self.config.port,
                    database=self.config.database,
                    user=self.config.username,
                    password=self.config.password,
                    connect_timeout=self.config.connection_timeout
                )
            elif self.config.db_type == DatabaseType.SQLITE:
                conn = sqlite3.connect(
                    self.config.database,
                    timeout=self.config.connection_timeout
                )
            else:
                raise DatabaseConnectionError(f"Unsupported database type: {self.config.db_type}")

            return conn

        except Exception as e:
            self.logger.error(f"Failed to create database connection: {e}")
            raise DatabaseConnectionError(f"Connection failed: {e}")

    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections.

        Provides automatic connection management with proper cleanup.
        """
        conn = None
        try:
            if self.config.connection_pool and self._connection_pool:
                conn = self._connection_pool.pop()
            else:
                conn = self._create_connection()

            self._active_connections += 1
            yield conn

        except Exception as e:
            self.logger.error(f"Connection context manager error: {e}")
            raise
        finally:
            if conn:
                if self.config.connection_pool and len(self._connection_pool) < self.config.max_connections:
                    self._connection_pool.append(conn)
                else:
                    try:
                        conn.close()
                    except Exception:
                        pass  # Connection might already be closed
                self._active_connections -= 1

    def execute_query(self, query: str, params: Optional[Tuple] = None,
                     fetch_results: bool = True) -> QueryResult:
        """
        Execute a database query.

        Args:
            query: SQL query string
            params: Query parameters
            fetch_results: Whether to fetch and return results

        Returns:
            QueryResult with execution details
        """
        start_time = time.time()

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    self.logger.debug(f"Executing query: {query}")
                    if params:
                        self.logger.debug(f"Parameters: {params}")

                    cursor.execute(query, params or ())

                    if fetch_results and cursor.description:
                        rows = cursor.fetchall()
                        columns = [desc[0] for desc in cursor.description]
                        row_count = len(rows)
                    else:
                        rows = []
                        columns = []
                        row_count = cursor.rowcount

                    conn.commit()

                    execution_time = time.time() - start_time

                    return QueryResult(
                        rows=rows,
                        columns=columns,
                        row_count=row_count,
                        execution_time=execution_time,
                        success=True
                    )

        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = f"Query execution failed: {e}"
            self.logger.error(error_msg)

            return QueryResult(
                rows=[],
                columns=[],
                row_count=0,
                execution_time=execution_time,
                success=False,
                error_message=error_msg
            )

    def execute_batch(self, queries: List[Tuple[str, Optional[Tuple]]]) -> List[QueryResult]:
        """
        Execute multiple queries in batch.

        Args:
            queries: List of (query, params) tuples

        Returns:
            List of QueryResult objects
        """
        results = []

        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    for query, params in queries:
                        start_time = time.time()

                        try:
                            cursor.execute(query, params or ())
                            execution_time = time.time() - start_time

                            if cursor.description:
                                rows = cursor.fetchall()
                                columns = [desc[0] for desc in cursor.description]
                                row_count = len(rows)
                            else:
                                rows = []
                                columns = []
                                row_count = cursor.rowcount

                            results.append(QueryResult(
                                rows=rows,
                                columns=columns,
                                row_count=row_count,
                                execution_time=execution_time,
                                success=True
                            ))

                        except Exception as e:
                            execution_time = time.time() - start_time
                            results.append(QueryResult(
                                rows=[],
                                columns=[],
                                row_count=0,
                                execution_time=execution_time,
                                success=False,
                                error_message=str(e)
                            ))

                    conn.commit()

        except Exception as e:
            self.logger.error(f"Batch execution failed: {e}")
            # Return failed results for remaining queries
            for _ in range(len(queries) - len(results)):
                results.append(QueryResult(
                    rows=[], columns=[], row_count=0,
                    execution_time=0, success=False, error_message=str(e)
                ))

        return results

    def load_dataframe(self, query: str, params: Optional[Tuple] = None,
                      library: str = "pandas") -> Optional[Any]:
        """
        Load query results into a DataFrame.

        Args:
            query: SQL query string
            params: Query parameters
            library: DataFrame library ('pandas' or 'polars')

        Returns:
            DataFrame or None if loading fails
        """
        result = self.execute_query(query, params, fetch_results=True)

        if not result.success or not result.rows:
            return None

        try:
            if library.lower() == "pandas" and HAS_PANDAS:
                return pd.DataFrame(result.rows, columns=result.columns)
            elif library.lower() == "polars" and HAS_POLARS:
                return pl.DataFrame(result.rows, schema=result.columns)
            else:
                self.logger.warning(f"DataFrame library {library} not available")
                return None
        except Exception as e:
            self.logger.error(f"Failed to create DataFrame: {e}")
            return None

    def insert_dataframe(self, table_name: str, df: Any,
                        if_exists: str = "append") -> QueryResult:
        """
        Insert DataFrame data into database table.

        Args:
            table_name: Target table name
            df: DataFrame (pandas or polars)
            if_exists: Action if table exists ('append', 'replace', 'fail')

        Returns:
            QueryResult with insertion details
        """
        if not HAS_PANDAS and not HAS_POLARS:
            return QueryResult(
                rows=[], columns=[], row_count=0, execution_time=0,
                success=False, error_message="No DataFrame library available"
            )

        try:
            # Convert to list of tuples for insertion
            if HAS_PANDAS and isinstance(df, pd.DataFrame):
                columns = list(df.columns)
                data = [tuple(row) for row in df.values]
            elif HAS_POLARS and isinstance(df, pl.DataFrame):
                columns = df.columns
                data = [tuple(row) for row in df.rows()]
            else:
                return QueryResult(
                    rows=[], columns=[], row_count=0, execution_time=0,
                    success=False, error_message="Unsupported DataFrame type"
                )

            if not data:
                return QueryResult(
                    rows=[], columns=[], row_count=0, execution_time=0,
                    success=True, error_message="No data to insert"
                )

            # Handle table existence
            if if_exists == "replace":
                self.execute_query(f"DROP TABLE IF EXISTS {table_name}")
                # Create table (basic implementation)
                col_defs = []
                for col in columns:
                    if self.config.db_type == DatabaseType.SQLITE:
                        col_defs.append(f'"{col}" TEXT')
                    else:
                        col_defs.append(f'"{col}" VARCHAR(255)')
                create_query = f"CREATE TABLE {table_name} ({', '.join(col_defs)})"
                self.execute_query(create_query)

            # Insert data
            placeholders = ", ".join(["%s"] * len(columns))
            if self.config.db_type == DatabaseType.SQLITE:
                placeholders = ", ".join(["?"] * len(columns))

            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"

            # Execute batch insert
            start_time = time.time()
            success_count = 0
            failed_count = 0

            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    for row in data:
                        try:
                            cursor.execute(insert_query, row)
                            success_count += 1
                        except Exception as e:
                            failed_count += 1
                            self.logger.warning(f"Failed to insert row: {e}")
                    conn.commit()

            execution_time = time.time() - start_time

            return QueryResult(
                rows=[], columns=[], row_count=success_count,
                execution_time=execution_time, success=True,
                error_message=f"Inserted {success_count} rows, {failed_count} failed"
            )

        except Exception as e:
            return QueryResult(
                rows=[], columns=[], row_count=0, execution_time=0,
                success=False, error_message=f"DataFrame insertion failed: {e}"
            )

    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a database table.

        Args:
            table_name: Name of the table

        Returns:
            Dictionary with table information or None if not found
        """
        try:
            if self.config.db_type == DatabaseType.POSTGRESQL:
                query = """
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = %s AND table_schema = 'public'
                    ORDER BY ordinal_position
                """
            elif self.config.db_type == DatabaseType.MYSQL:
                query = """
                    SELECT column_name, data_type, is_nullable, column_default
                    FROM information_schema.columns
                    WHERE table_name = %s AND table_schema = DATABASE()
                    ORDER BY ordinal_position
                """
            elif self.config.db_type == DatabaseType.SQLITE:
                query = f"PRAGMA table_info({table_name})"
            else:
                return None

            result = self.execute_query(query, (table_name,) if table_name else None)

            if result.success and result.rows:
                if self.config.db_type == DatabaseType.SQLITE:
                    columns = []
                    for row in result.rows:
                        columns.append({
                            'column_name': row[1],
                            'data_type': row[2],
                            'is_nullable': 'YES' if row[3] == 0 else 'NO',
                            'column_default': row[4]
                        })
                else:
                    columns = [{
                        'column_name': row[0],
                        'data_type': row[1],
                        'is_nullable': row[2],
                        'column_default': row[3]
                    } for row in result.rows]

                return {
                    'table_name': table_name,
                    'columns': columns,
                    'column_count': len(columns)
                }

        except Exception as e:
            self.logger.error(f"Failed to get table info for {table_name}: {e}")

        return None

    def table_exists(self, table_name: str) -> bool:
        """
        Check if a table exists in the database.

        Args:
            table_name: Name of the table to check

        Returns:
            True if table exists, False otherwise
        """
        try:
            if self.config.db_type == DatabaseType.POSTGRESQL:
                query = """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = %s AND table_schema = 'public'
                    )
                """
            elif self.config.db_type == DatabaseType.MYSQL:
                query = """
                    SELECT EXISTS (
                        SELECT 1 FROM information_schema.tables
                        WHERE table_name = %s AND table_schema = DATABASE()
                    )
                """
            elif self.config.db_type == DatabaseType.SQLITE:
                query = """
                    SELECT name FROM sqlite_master
                    WHERE type='table' AND name=?
                """
            else:
                return False

            result = self.execute_query(query, (table_name,))

            if self.config.db_type == DatabaseType.SQLITE:
                return len(result.rows) > 0
            else:
                return result.rows and result.rows[0][0]

        except Exception as e:
            self.logger.error(f"Failed to check table existence for {table_name}: {e}")
            return False

    def get_database_stats(self) -> Dict[str, Any]:
        """
        Get database statistics and information.

        Returns:
            Dictionary with database statistics
        """
        stats = {
            'database_type': self.config.db_type.value,
            'active_connections': self._active_connections,
            'pool_size': len(self._connection_pool),
            'max_connections': self.config.max_connections
        }

        try:
            # Get table count
            if self.config.db_type == DatabaseType.POSTGRESQL:
                query = "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'"
            elif self.config.db_type == DatabaseType.MYSQL:
                query = "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE()"
            elif self.config.db_type == DatabaseType.SQLITE:
                query = "SELECT count(*) FROM sqlite_master WHERE type='table'"
            else:
                query = None

            if query:
                result = self.execute_query(query)
                if result.success and result.rows:
                    stats['table_count'] = result.rows[0][0]

        except Exception as e:
            self.logger.warning(f"Failed to get database stats: {e}")

        return stats

    def health_check(self) -> Dict[str, Any]:
        """
        Perform database health check.

        Returns:
            Dictionary with health check results
        """
        health = {
            'healthy': False,
            'response_time': None,
            'error': None
        }

        try:
            start_time = time.time()
            result = self.execute_query("SELECT 1 as health_check")
            response_time = time.time() - start_time

            health.update({
                'healthy': result.success,
                'response_time': round(response_time, 3),
                'error': result.error_message if not result.success else None
            })

        except Exception as e:
            health['error'] = str(e)

        return health

    def close_all_connections(self):
        """Close all active connections and clear connection pool"""
        for conn in self._connection_pool:
            try:
                conn.close()
            except Exception:
                pass

        self._connection_pool.clear()
        self._active_connections = 0
        self.logger.info("All database connections closed")