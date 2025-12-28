"""
Relational Database Connector

Provides connectivity to relational databases including PostgreSQL, MySQL,
SQL Server, and Oracle through a unified interface.
"""

import logging
import threading
from typing import Any, Dict, List, Optional, Tuple

from .base_connector import BaseConnector

logger = logging.getLogger(__name__)


class RelationalDBConnector(BaseConnector):
    """
    Connector for relational databases.

    Supports multiple database backends through appropriate drivers:
    - PostgreSQL (psycopg2)
    - MySQL (pymysql)
    - SQL Server (pyodbc)
    - Oracle (cx_Oracle)
    """

    def __init__(self, conn_config: Dict[str, Any]):
        """
        Initialize the relational database connector.

        Args:
            conn_config: Configuration dictionary containing:
                - driver: Database driver ('psycopg2', 'pymysql', 'pyodbc', 'cx_Oracle')
                - conn_args: Connection arguments for the driver
                - table: Default table name (optional)
        """
        super().__init__(conn_config)
        self.conn = None
        self.cursor = None
        self.driver = conn_config.get("driver")
        self.lock = threading.Lock()

    def connect(self) -> None:
        """
        Establish connection to the database.

        Raises:
            ValueError: If driver is not specified
            ImportError: If required driver is not installed
            Exception: If connection fails
        """
        if self.conn:
            return

        if not self.driver:
            raise ValueError("Relational connector requires 'driver' in config")

        # Lazy import of driver
        if self.driver == "psycopg2":
            import psycopg2
            self.conn = psycopg2.connect(**self.config.get("conn_args", {}))
            self.cursor = self.conn.cursor()
        elif self.driver == "pymysql":
            import pymysql
            self.conn = pymysql.connect(**self.config.get("conn_args", {}))
            self.cursor = self.conn.cursor()
        elif self.driver == "pyodbc":
            import pyodbc
            self.conn = pyodbc.connect(self.config.get("connection_string"))
            self.cursor = self.conn.cursor()
        elif self.driver == "cx_Oracle":
            import cx_Oracle
            self.conn = cx_Oracle.connect(**self.config.get("conn_args", {}))
            self.cursor = self.conn.cursor()
        else:
            raise NotImplementedError(f"Driver {self.driver} not supported")

    def disconnect(self) -> None:
        """
        Close database connection and cursor.
        """
        with self.lock:
            try:
                if self.cursor:
                    self.cursor.close()
                if self.conn:
                    self.conn.close()
            finally:
                self.conn = None
                self.cursor = None

    def read(self, query: str, batch_size: Optional[int] = None) -> List[Tuple]:
        """
        Execute query and return results.

        Args:
            query: SQL query to execute
            batch_size: If specified, use fetchmany for batched reading

        Returns:
            List of tuples containing query results
        """
        self.connect()
        self.cursor.execute(query)

        if batch_size:
            rows = []
            while True:
                batch = self.cursor.fetchmany(batch_size)
                if not batch:
                    break
                rows.extend(batch)
            return rows
        return self.cursor.fetchall()

    def write(self, data: List[Tuple], table: str, mode: str = "append") -> Dict[str, int]:
        """
        Write data to database table.

        Args:
            data: List of tuples to insert
            table: Target table name
            mode: Write mode ("append", "overwrite", "truncate_and_load")

        Returns:
            Dictionary with operation statistics
        """
        self.connect()
        inserted = 0
        updated = 0

        if not data:
            return {"inserted": 0, "updated": 0}

        if mode in ("append", "upsert"):
            insert_sql = self.config.get("insert_sql")
            if not insert_sql:
                raise ValueError("Relational connector requires 'insert_sql' when writing")
            self.cursor.executemany(insert_sql, data)
            try:
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
            inserted = len(data)

        elif mode == "truncate_and_load":
            truncate_sql = f"TRUNCATE TABLE {table}"
            self.cursor.execute(truncate_sql)
            insert_sql = self.config.get("insert_sql")
            if not insert_sql:
                raise ValueError("Relational connector requires 'insert_sql' when writing")
            self.cursor.executemany(insert_sql, data)
            try:
                self.conn.commit()
            except Exception:
                self.conn.rollback()
                raise
            inserted = len(data)
        else:
            raise NotImplementedError(f"Write mode {mode} is not implemented")

        return {"inserted": inserted, "updated": updated}

    def get_schema(self) -> Dict[str, Any]:
        """
        Get schema information for the configured table.

        Returns:
            Dictionary containing column information
        """
        table = self.config.get("table")
        if not table:
            return {}

        self.connect()
        self.cursor.execute(f"SELECT * FROM {table} LIMIT 0")
        return {"columns": [d[0] for d in self.cursor.description]}

    def validate(self) -> bool:
        """
        Validate connection and configuration.

        Returns:
            True if validation passes
        """
        try:
            self.connect()
            return True
        except Exception:
            return False