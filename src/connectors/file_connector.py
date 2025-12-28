"""
File System Connector

Provides connectivity to various file formats and storage systems
including local files, cloud storage, and distributed file systems.
"""

import logging
import os
import threading
from typing import Any, Dict, List, Optional, Tuple

from .base_connector import BaseConnector

logger = logging.getLogger(__name__)


class FileConnector(BaseConnector):
    """
    Connector for file-based data sources.

    Supports multiple file formats and storage backends:
    - Local files (CSV, JSON, Parquet, Excel)
    - Cloud storage (AWS S3, Azure Blob, GCS)
    - Distributed systems (HDFS)
    """

    def __init__(self, conn_config: Dict[str, Any]):
        """
        Initialize the file connector.

        Args:
            conn_config: Configuration dictionary containing:
                - file_path: Path to file or directory
                - file_format: Format ('csv', 'json', 'parquet', 'excel')
                - storage_type: Storage backend ('local', 's3', 'azure', 'gcs', 'hdfs')
                - credentials: Authentication credentials for cloud storage
        """
        super().__init__(conn_config)
        self.file_path = conn_config.get("file_path")
        self.file_format = conn_config.get("file_format", "csv")
        self.storage_type = conn_config.get("storage_type", "local")
        self.lock = threading.Lock()

    def connect(self) -> None:
        """
        Establish connection to file storage system.

        For local files, this is a no-op. For cloud storage,
        this establishes the client connection.
        """
        if self.storage_type == "local":
            return
        elif self.storage_type == "s3":
            import boto3
            self.client = boto3.client("s3", **self.config.get("credentials", {}))
        elif self.storage_type == "azure":
            from azure.storage.blob import BlobServiceClient
            self.client = BlobServiceClient(**self.config.get("credentials", {}))
        elif self.storage_type == "gcs":
            from google.cloud import storage
            self.client = storage.Client(**self.config.get("credentials", {}))
        elif self.storage_type == "hdfs":
            from hdfs import InsecureClient
            self.client = InsecureClient(**self.config.get("credentials", {}))
        else:
            raise NotImplementedError(f"Storage type {self.storage_type} not supported")

    def disconnect(self) -> None:
        """
        Close connections to storage systems.
        """
        with self.lock:
            if hasattr(self, 'client'):
                # Most cloud clients don't need explicit close
                pass

    def read(self, query: Optional[Dict[str, Any]] = None, batch_size: Optional[int] = None) -> List[Dict]:
        """
        Read data from file.

        Args:
            query: Optional query parameters (e.g., sheet name for Excel)
            batch_size: If specified, read in chunks

        Returns:
            List of dictionaries containing file data
        """
        self.connect()

        if self.storage_type == "local":
            return self._read_local_file(query, batch_size)
        elif self.storage_type == "s3":
            return self._read_s3_file(query, batch_size)
        elif self.storage_type == "azure":
            return self._read_azure_file(query, batch_size)
        elif self.storage_type == "gcs":
            return self._read_gcs_file(query, batch_size)
        elif self.storage_type == "hdfs":
            return self._read_hdfs_file(query, batch_size)
        else:
            raise NotImplementedError(f"Storage type {self.storage_type} not supported")

    def _read_local_file(self, query: Optional[Dict[str, Any]], batch_size: Optional[int]) -> List[Dict]:
        """Read from local file system."""
        if not os.path.exists(self.file_path):
            raise FileNotFoundError(f"File not found: {self.file_path}")

        if self.file_format == "csv":
            import pandas as pd
            df = pd.read_csv(self.file_path)
            return df.to_dict('records')
        elif self.file_format == "json":
            import json
            with open(self.file_path, 'r') as f:
                data = json.load(f)
            return data if isinstance(data, list) else [data]
        elif self.file_format == "parquet":
            import pandas as pd
            df = pd.read_parquet(self.file_path)
            return df.to_dict('records')
        elif self.file_format == "excel":
            import pandas as pd
            sheet = query.get("sheet_name", 0) if query else 0
            df = pd.read_excel(self.file_path, sheet_name=sheet)
            return df.to_dict('records')
        else:
            raise NotImplementedError(f"File format {self.file_format} not supported")

    def _read_s3_file(self, query: Optional[Dict[str, Any]], batch_size: Optional[int]) -> List[Dict]:
        """Read from AWS S3."""
        bucket, key = self._parse_s3_path(self.file_path)
        obj = self.client.get_object(Bucket=bucket, Key=key)
        content = obj['Body'].read()

        if self.file_format == "csv":
            import pandas as pd
            import io
            df = pd.read_csv(io.BytesIO(content))
            return df.to_dict('records')
        elif self.file_format == "json":
            import json
            return json.loads(content.decode('utf-8'))
        elif self.file_format == "parquet":
            import pandas as pd
            import io
            df = pd.read_parquet(io.BytesIO(content))
            return df.to_dict('records')
        else:
            raise NotImplementedError(f"File format {self.file_format} not supported for S3")

    def _read_azure_file(self, query: Optional[Dict[str, Any]], batch_size: Optional[int]) -> List[Dict]:
        """Read from Azure Blob Storage."""
        container, blob = self._parse_azure_path(self.file_path)
        blob_client = self.client.get_blob_client(container=container, blob=blob)
        content = blob_client.download_blob().readall()

        if self.file_format == "csv":
            import pandas as pd
            import io
            df = pd.read_csv(io.BytesIO(content))
            return df.to_dict('records')
        elif self.file_format == "json":
            import json
            return json.loads(content.decode('utf-8'))
        else:
            raise NotImplementedError(f"File format {self.file_format} not supported for Azure")

    def _read_gcs_file(self, query: Optional[Dict[str, Any]], batch_size: Optional[int]) -> List[Dict]:
        """Read from Google Cloud Storage."""
        bucket_name, blob_name = self._parse_gcs_path(self.file_path)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        content = blob.download_as_text()

        if self.file_format == "csv":
            import pandas as pd
            import io
            df = pd.read_csv(io.StringIO(content))
            return df.to_dict('records')
        elif self.file_format == "json":
            import json
            return json.loads(content)
        else:
            raise NotImplementedError(f"File format {self.file_format} not supported for GCS")

    def _read_hdfs_file(self, query: Optional[Dict[str, Any]], batch_size: Optional[int]) -> List[Dict]:
        """Read from HDFS."""
        with self.client.read(self.file_path) as reader:
            content = reader.read().decode('utf-8')

        if self.file_format == "csv":
            import pandas as pd
            import io
            df = pd.read_csv(io.StringIO(content))
            return df.to_dict('records')
        elif self.file_format == "json":
            import json
            return json.loads(content)
        else:
            raise NotImplementedError(f"File format {self.file_format} not supported for HDFS")

    def write(self, data: List[Dict], file_path: Optional[str] = None, mode: str = "overwrite") -> Dict[str, int]:
        """
        Write data to file.

        Args:
            data: List of dictionaries to write
            file_path: Target file path (uses config if not provided)
            mode: Write mode ("overwrite", "append")

        Returns:
            Dictionary with operation statistics
        """
        self.connect()
        target_path = file_path or self.file_path

        if self.storage_type == "local":
            return self._write_local_file(data, target_path, mode)
        elif self.storage_type == "s3":
            return self._write_s3_file(data, target_path, mode)
        elif self.storage_type == "azure":
            return self._write_azure_file(data, target_path, mode)
        elif self.storage_type == "gcs":
            return self._write_gcs_file(data, target_path, mode)
        elif self.storage_type == "hdfs":
            return self._write_hdfs_file(data, target_path, mode)
        else:
            raise NotImplementedError(f"Storage type {self.storage_type} not supported")

    def _write_local_file(self, data: List[Dict], file_path: str, mode: str) -> Dict[str, int]:
        """Write to local file system."""
        import pandas as pd

        df = pd.DataFrame(data)
        if self.file_format == "csv":
            df.to_csv(file_path, index=False, mode='w' if mode == "overwrite" else 'a')
        elif self.file_format == "json":
            df.to_json(file_path, orient='records', indent=2)
        elif self.file_format == "parquet":
            df.to_parquet(file_path, index=False)
        elif self.file_format == "excel":
            df.to_excel(file_path, index=False)
        else:
            raise NotImplementedError(f"File format {self.file_format} not supported")

        return {"written": len(data)}

    def _write_s3_file(self, data: List[Dict], file_path: str, mode: str) -> Dict[str, int]:
        """Write to AWS S3."""
        import pandas as pd
        import io

        bucket, key = self._parse_s3_path(file_path)
        df = pd.DataFrame(data)

        if self.file_format == "csv":
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            self.client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        elif self.file_format == "json":
            json_str = df.to_json(orient='records', indent=2)
            self.client.put_object(Bucket=bucket, Key=key, Body=json_str)
        elif self.file_format == "parquet":
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            self.client.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())
        else:
            raise NotImplementedError(f"File format {self.file_format} not supported for S3")

        return {"written": len(data)}

    def _write_azure_file(self, data: List[Dict], file_path: str, mode: str) -> Dict[str, int]:
        """Write to Azure Blob Storage."""
        import pandas as pd
        import io

        container, blob = self._parse_azure_path(file_path)
        blob_client = self.client.get_blob_client(container=container, blob=blob)
        df = pd.DataFrame(data)

        if self.file_format == "csv":
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            blob_client.upload_blob(buffer.getvalue(), overwrite=True)
        elif self.file_format == "json":
            json_str = df.to_json(orient='records', indent=2)
            blob_client.upload_blob(json_str, overwrite=True)
        else:
            raise NotImplementedError(f"File format {self.file_format} not supported for Azure")

        return {"written": len(data)}

    def _write_gcs_file(self, data: List[Dict], file_path: str, mode: str) -> Dict[str, int]:
        """Write to Google Cloud Storage."""
        import pandas as pd

        bucket_name, blob_name = self._parse_gcs_path(file_path)
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        df = pd.DataFrame(data)

        if self.file_format == "csv":
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            blob.upload_from_string(buffer.getvalue())
        elif self.file_format == "json":
            json_str = df.to_json(orient='records', indent=2)
            blob.upload_from_string(json_str)
        else:
            raise NotImplementedError(f"File format {self.file_format} not supported for GCS")

        return {"written": len(data)}

    def _write_hdfs_file(self, data: List[Dict], file_path: str, mode: str) -> Dict[str, int]:
        """Write to HDFS."""
        import pandas as pd
        import io

        df = pd.DataFrame(data)
        if self.file_format == "csv":
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            with self.client.write(file_path, overwrite=True) as writer:
                writer.write(buffer.getvalue())
        elif self.file_format == "json":
            json_str = df.to_json(orient='records', indent=2)
            with self.client.write(file_path, overwrite=True) as writer:
                writer.write(json_str)
        else:
            raise NotImplementedError(f"File format {self.file_format} not supported for HDFS")

        return {"written": len(data)}

    def get_schema(self) -> Dict[str, Any]:
        """
        Get schema information for the file.

        Returns:
            Dictionary containing column information
        """
        try:
            sample_data = self.read(batch_size=1)
            if sample_data:
                return {"columns": list(sample_data[0].keys())}
        except Exception:
            pass
        return {}

    def validate(self) -> bool:
        """
        Validate file path and accessibility.

        Returns:
            True if validation passes
        """
        try:
            if self.storage_type == "local":
                return os.path.exists(self.file_path)
            else:
                # For cloud storage, try to list/access the file
                self.connect()
                return True
        except Exception:
            return False

    def _parse_s3_path(self, path: str) -> Tuple[str, str]:
        """Parse S3 path into bucket and key."""
        if path.startswith("s3://"):
            path = path[5:]
        parts = path.split("/", 1)
        return parts[0], parts[1] if len(parts) > 1 else ""

    def _parse_azure_path(self, path: str) -> Tuple[str, str]:
        """Parse Azure path into container and blob."""
        if path.startswith("azure://"):
            path = path[8:]
        parts = path.split("/", 1)
        return parts[0], parts[1] if len(parts) > 1 else ""

    def _parse_gcs_path(self, path: str) -> Tuple[str, str]:
        """Parse GCS path into bucket and blob."""
        if path.startswith("gs://"):
            path = path[5:]
        parts = path.split("/", 1)
        return parts[0], parts[1] if len(parts) > 1 else ""
    #</content>
#<parameter name="filePath">d:\development\2026\metadata-etl-framework\src\connectors\file_connector.py