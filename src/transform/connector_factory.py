import logging
import threading
import time
import uuid
from datetime import datetime
from queue import Queue, Empty
from typing import Any, Dict, Optional, Tuple, List, Callable

from cryptography.fernet import Fernet, InvalidToken

# Import connector classes from the connectors package
from connectors import (
    BaseConnector,
    RelationalDBConnector,
    NoSQLConnector,
    FileConnector,
    APIConnector,
    CloudConnector
)

# Lightweight optional imports - connectors will raise informative errors if libs missing
try:
    import pandas as pd
except Exception:
    pd = None

try:
    import boto3
except Exception:
    boto3 = None

try:
    from google.cloud import storage as gcs_lib  # type: ignore
except Exception:
    gcs_lib = None

try:
    from azure.storage.blob import BlobServiceClient  # type: ignore
except Exception:
    BlobServiceClient = None

# Database drivers are optional; connectors should import appropriate driver at runtime
# e.g. psycopg2, pymysql, cx_Oracle, pyodbc, pymongo

logger = logging.getLogger("ConnectorFactory")
logger.setLevel(logging.INFO)


class RetryExceededError(Exception):
    pass


def _retry(
    func: Callable,
    max_retries: int = 3,
    backoff_factor: float = 2.0,
    base_delay: float = 0.5,
) -> Any:
    attempt = 0
    while True:
        try:
            attempt += 1
            return func()
        except Exception as exc:
            if attempt > max_retries:
                logger.exception("Retry attempts exhausted")
                raise RetryExceededError(str(exc)) from exc
            sleep_for = base_delay * (backoff_factor ** (attempt - 1))
            logger.warning("Transient error (attempt %s/%s): %s — sleeping %.2fs", attempt, max_retries, exc, sleep_for)
            time.sleep(sleep_for)

# Base connector interface - minimal contract
class BaseConnector:
    def connect(self) -> None:
        raise NotImplementedError

    def disconnect(self) -> None:
        raise NotImplementedError

    def read(self, query_or_path: str, batch_size: Optional[int] = None) -> Any:
        raise NotImplementedError

    def write(self, data: Any, table_or_path: str, mode: str = "append") -> Dict[str, int]:
        raise NotImplementedError

    def get_schema(self) -> Dict[str, Any]:
        raise NotImplementedError

    def validate(self) -> bool:
        raise NotImplementedError

class ConnectorFactory:
    """
    Factory to create connectors for sources and targets, manage pools, encryption, retries, and logs.
    """

    def __init__(
        self,
        config_loader: Any,
        db_connection: Any,
        encryption_key: Optional[bytes] = None,
        pool_max_size: int = 5,
        max_retries: int = 3,
        backoff_factor: float = 2.0,
    ):
        self.config_loader = config_loader
        self.db = db_connection
        self.pool_max_size = pool_max_size
        self.max_retries = max_retries
        self.backoff_factor = backoff_factor

        if encryption_key:
            self.fernet = Fernet(encryption_key)
        else:
            # create ephemeral key if none provided (not for production)
            self.fernet = Fernet(Fernet.generate_key())

        # pools keyed by a unique connection fingerprint
        self._pools: Dict[str, Queue] = {}
        self._pools_lock = threading.Lock()
        self.logger = logging.getLogger("ConnectorFactory")

    # Helper to create pool key
    def _pool_key(self, conn_config: Dict[str, Any]) -> str:
        key = conn_config.get("name") or conn_config.get("id") or str(hash(frozenset(conn_config.items())))
        return str(key)

    # 1 / 2. factory methods
    def create_source_connector(self, conn_config: Dict[str, Any]) -> BaseConnector:
        return self._create_connector(conn_config)

    def create_target_connector(self, conn_config: Dict[str, Any]) -> BaseConnector:
        return self._create_connector(conn_config)

    def _create_connector(self, conn_config: Dict[str, Any]) -> BaseConnector:
        conn_type = (conn_config.get("type") or "").lower()
        if conn_type in ("oracle", "postgresql", "mysql", "sqlserver", "mssql"):
            return RelationalDBConnector(conn_config)
        if conn_type in ("mongodb", "cassandra"):
            return NoSQLConnector(conn_config)
        if conn_type in ("csv", "excel", "parquet", "json", "file"):
            return FileConnector(conn_config)
        if conn_type in ("rest", "soap", "api"):
            return APIConnector(conn_config)
        if conn_type in ("s3", "azure_blob", "gcs", "gcs_bucket", "azure", "gcs"):
            return CloudConnector(conn_config)
        raise NotImplementedError(f"Connector type '{conn_type}' is not supported")

    # 3. extract_data - orchestrates read with retry and logs extraction
    def extract_data(self, run_id: str, source_config: Dict[str, Any]) -> Any:
        start = datetime.utcnow()
        src_id = source_config.get("id")
        connector = self.create_source_connector(source_config)
        try:
            def _do():
                connector.connect()
                result = connector.read(source_config.get("query") or source_config.get("path") or source_config.get("endpoint"), batch_size=source_config.get("batch_size"))
                return result

            data = _retry(lambda: _do(), max_retries=self.max_retries, backoff_factor=self.backoff_factor)
            duration = (datetime.utcnow() - start).total_seconds()
            rows = self._estimate_rows(data)
            self.log_extraction(run_id, src_id, rows, duration)
            return data
        finally:
            try:
                connector.disconnect()
            except Exception:
                logger.exception("Error during connector disconnect in extract")

    # 4. load_data - orchestrates write with retry and logs load
    def load_data(self, run_id: str, target_config: Dict[str, Any], data: Any, load_type: str = "append") -> Dict[str, Any]:
        start = datetime.utcnow()
        tgt_id = target_config.get("id")
        connector = self.create_target_connector(target_config)
        try:
            def _do():
                connector.connect()
                # Normalise data for connectors expecting list/bytes/pandas
                res = connector.write(data, target_config.get("table") or target_config.get("path") or target_config.get("endpoint"), mode=load_type)
                return res

            result = _retry(lambda: _do(), max_retries=self.max_retries, backoff_factor=self.backoff_factor)
            duration = (datetime.utcnow() - start).total_seconds()
            rows_inserted = result.get("inserted") or result.get("written") or 0
            rows_updated = result.get("updated") or 0
            self.log_load(run_id, tgt_id, rows_inserted, rows_updated, duration)
            return result
        finally:
            try:
                connector.disconnect()
            except Exception:
                logger.exception("Error during connector disconnect in load")

    # 5. test_connection - lightweight connectivity check with retries
    def test_connection(self, conn_config: Dict[str, Any]) -> bool:
        connector = self._create_connector(conn_config)
        try:
            def _do():
                connector.connect()
                valid = connector.validate()
                return valid

            return bool(_retry(lambda: _do(), max_retries=self.max_retries, backoff_factor=self.backoff_factor))
        finally:
            try:
                connector.disconnect()
            except Exception:
                pass

    # 6. get_connection_pool - returns a Queue of pre-created connector instances
    def get_connection_pool(self, conn_config: Dict[str, Any]) -> Queue:
        key = self._pool_key(conn_config)
        with self._pools_lock:
            if key in self._pools:
                return self._pools[key]
            q: Queue = Queue(maxsize=self.pool_max_size)
            # lazily create one connector; others created on demand
            for _ in range(min(1, self.pool_max_size)):
                q.put(self._create_connector(conn_config))
            self._pools[key] = q
            return q

    def acquire_from_pool(self, conn_config: Dict[str, Any], timeout: float = 5.0) -> BaseConnector:
        pool = self.get_connection_pool(conn_config)
        try:
            conn = pool.get(timeout=timeout)
            return conn
        except Empty:
            # create a new connector if pool not full
            return self._create_connector(conn_config)

    def release_to_pool(self, conn_config: Dict[str, Any], connector: BaseConnector) -> None:
        pool = self.get_connection_pool(conn_config)
        try:
            pool.put(connector, block=False)
        except Exception:
            # pool full, dispose connector
            try:
                connector.disconnect()
            except Exception:
                pass

    # 7 / 8. encryption helpers
    def encrypt_credentials(self, password: str) -> str:
        token = self.fernet.encrypt(password.encode("utf-8"))
        return token.decode("utf-8")

    def decrypt_credentials(self, encrypted_password: str) -> str:
        try:
            plain = self.fernet.decrypt(encrypted_password.encode("utf-8"))
            return plain.decode("utf-8")
        except InvalidToken:
            logger.exception("Invalid encryption token provided")
            raise

    # 9 / 10. logging extraction/load events to database tables
    def log_extraction(self, run_id: str, source_id: str, rows: int, duration: float) -> None:
        try:
            event_id = str(uuid.uuid4())
            sql = """
                INSERT INTO EXTRACTION_LOG (event_id, run_id, source_id, rows_extracted, duration_seconds, created_at)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            params = (event_id, run_id, source_id, rows, duration, datetime.utcnow())
            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()
            logger.debug("Logged extraction: %s rows from %s (run=%s)", rows, source_id, run_id)
        except Exception:
            try:
                self.db.rollback()
            except Exception:
                pass
            logger.exception("Failed to log extraction event")

    def log_load(self, run_id: str, target_id: str, rows_inserted: int, rows_updated: int, duration: float) -> None:
        try:
            event_id = str(uuid.uuid4())
            sql = """
                INSERT INTO LOAD_LOG (event_id, run_id, target_id, rows_inserted, rows_updated, duration_seconds, created_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """
            params = (event_id, run_id, target_id, rows_inserted, rows_updated, duration, datetime.utcnow())
            self.db.begin()
            self.db.execute(sql, params)
            self.db.commit()
            logger.debug("Logged load: %s inserted, %s updated into %s (run=%s)", rows_inserted, rows_updated, target_id, run_id)
        except Exception:
            try:
                self.db.rollback()
            except Exception:
                pass
            logger.exception("Failed to log load event")

    # utilities
    def _estimate_rows(self, data: Any) -> int:
        try:
            if data is None:
                return 0
            if hasattr(data, "shape"):
                return int(data.shape[0])
            if hasattr(data, "__len__"):
                return len(data)
            return 1
        except Exception:
            return 0

    # Resource cleanup for all pools
    def shutdown(self) -> None:
        with self._pools_lock:
            for key, q in self._pools.items():
                while True:
                    try:
                        conn = q.get_nowait()
                        try:
                            conn.disconnect()
                        except Exception:
                            pass
                    except Empty:
                        break
            self._pools.clear()
            logger.info("ConnectorFactory pools shutdown complete")