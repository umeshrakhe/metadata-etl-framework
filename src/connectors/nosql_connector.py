"""
NoSQL Database Connector

Provides connectivity to NoSQL databases including MongoDB, Cassandra,
Redis, and DynamoDB through a unified interface.
"""

import logging
import threading
from typing import Any, Dict, List, Optional, Tuple

from .base_connector import BaseConnector

logger = logging.getLogger(__name__)


class NoSQLConnector(BaseConnector):
    """
    Connector for NoSQL databases.

    Supports multiple NoSQL backends:
    - MongoDB (pymongo)
    - Cassandra (cassandra-driver)
    - Redis (redis)
    - DynamoDB (boto3)
    """

    def __init__(self, conn_config: Dict[str, Any]):
        """
        Initialize the NoSQL database connector.

        Args:
            conn_config: Configuration dictionary containing:
                - driver: Database driver ('pymongo', 'cassandra', 'redis', 'boto3')
                - conn_args: Connection arguments for the driver
                - collection/table: Default collection/table name (optional)
        """
        super().__init__(conn_config)
        self.conn = None
        self.driver = conn_config.get("driver")
        self.lock = threading.Lock()

    def connect(self) -> None:
        """
        Establish connection to the NoSQL database.

        Raises:
            ValueError: If driver is not specified
            ImportError: If required driver is not installed
            Exception: If connection fails
        """
        if self.conn:
            return

        if not self.driver:
            raise ValueError("NoSQL connector requires 'driver' in config")

        # Lazy import of driver
        if self.driver == "pymongo":
            from pymongo import MongoClient
            self.conn = MongoClient(**self.config.get("conn_args", {}))
        elif self.driver == "cassandra":
            from cassandra.cluster import Cluster
            cluster = Cluster(**self.config.get("conn_args", {}))
            self.conn = cluster.connect()
        elif self.driver == "redis":
            import redis
            self.conn = redis.Redis(**self.config.get("conn_args", {}))
        elif self.driver == "boto3":
            import boto3
            self.conn = boto3.resource("dynamodb", **self.config.get("conn_args", {}))
        else:
            raise NotImplementedError(f"Driver {self.driver} not supported")

    def disconnect(self) -> None:
        """
        Close database connection.
        """
        with self.lock:
            try:
                if self.conn:
                    if self.driver == "pymongo":
                        self.conn.close()
                    elif self.driver == "cassandra":
                        self.conn.shutdown()
                    elif self.driver == "redis":
                        self.conn.close()
                    # boto3 doesn't need explicit close
            finally:
                self.conn = None

    def read(self, query: Dict[str, Any], batch_size: Optional[int] = None) -> List[Dict]:
        """
        Execute query and return results.

        Args:
            query: Query dictionary specific to the database type
            batch_size: If specified, limit number of results

        Returns:
            List of dictionaries containing query results
        """
        self.connect()

        if self.driver == "pymongo":
            collection = self.config.get("collection")
            if not collection:
                raise ValueError("MongoDB connector requires 'collection' in config")
            db = self.conn[self.config.get("database", "default")]
            cursor = db[collection].find(query)
            if batch_size:
                cursor = cursor.limit(batch_size)
            return list(cursor)

        elif self.driver == "cassandra":
            table = self.config.get("table")
            if not table:
                raise ValueError("Cassandra connector requires 'table' in config")
            # Simplified query execution
            prepared = self.conn.prepare(f"SELECT * FROM {table}")
            rows = self.conn.execute(prepared)
            return [dict(row) for row in rows]

        elif self.driver == "redis":
            # Redis key-value operations
            key = query.get("key")
            if key:
                return [{"key": key, "value": self.conn.get(key)}]
            return []

        elif self.driver == "boto3":
            table_name = self.config.get("table")
            if not table_name:
                raise ValueError("DynamoDB connector requires 'table' in config")
            table = self.conn.Table(table_name)
            response = table.scan()
            return response.get("Items", [])

        return []

    def write(self, data: List[Dict], collection: str, mode: str = "append") -> Dict[str, int]:
        """
        Write data to NoSQL collection/table.

        Args:
            data: List of dictionaries to insert/update
            collection: Target collection/table name
            mode: Write mode ("append", "overwrite", "upsert")

        Returns:
            Dictionary with operation statistics
        """
        self.connect()
        inserted = 0
        updated = 0

        if not data:
            return {"inserted": 0, "updated": 0}

        if self.driver == "pymongo":
            db = self.conn[self.config.get("database", "default")]
            if mode == "overwrite":
                db[collection].drop()
            db[collection].insert_many(data)
            inserted = len(data)

        elif self.driver == "cassandra":
            # Simplified insert
            table = collection
            for item in data:
                columns = ", ".join(item.keys())
                placeholders = ", ".join(["%s"] * len(item))
                values = list(item.values())
                query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
                prepared = self.conn.prepare(query)
                self.conn.execute(prepared, values)
            inserted = len(data)

        elif self.driver == "redis":
            for item in data:
                key = item.get("key")
                value = item.get("value")
                if key and value:
                    self.conn.set(key, value)
                    inserted += 1

        elif self.driver == "boto3":
            table = self.conn.Table(collection)
            for item in data:
                table.put_item(Item=item)
                inserted += 1

        return {"inserted": inserted, "updated": updated}

    def get_schema(self) -> Dict[str, Any]:
        """
        Get schema information for the configured collection/table.

        Returns:
            Dictionary containing schema information
        """
        if self.driver == "pymongo":
            collection = self.config.get("collection")
            if collection:
                db = self.conn[self.config.get("database", "default")]
                sample = db[collection].find_one()
                if sample:
                    return {"fields": list(sample.keys())}
        elif self.driver == "boto3":
            table_name = self.config.get("table")
            if table_name:
                table = self.conn.Table(table_name)
                return {"key_schema": table.key_schema}

        return {}

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