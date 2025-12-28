"""
Cloud Storage Connector

Provides unified connectivity to cloud storage services including
AWS S3, Azure Blob Storage, Google Cloud Storage, and other cloud providers.
"""

import logging
import threading
from typing import Any, Dict, List, Optional, Tuple

from .base_connector import BaseConnector

logger = logging.getLogger(__name__)


class CloudConnector(BaseConnector):
    """
    Connector for cloud storage services.

    Supports multiple cloud providers and storage services:
    - AWS S3
    - Azure Blob Storage
    - Google Cloud Storage
    - IBM Cloud Object Storage
    - Oracle Cloud Storage
    """

    def __init__(self, conn_config: Dict[str, Any]):
        """
        Initialize the cloud storage connector.

        Args:
            conn_config: Configuration dictionary containing:
                - provider: Cloud provider ('aws', 'azure', 'gcp', 'ibm', 'oracle')
                - bucket/container: Storage bucket/container name
                - credentials: Authentication credentials
                - region: Cloud region
                - endpoint_url: Custom endpoint URL (for S3-compatible services)
        """
        super().__init__(conn_config)
        self.provider = conn_config.get("provider")
        self.bucket = conn_config.get("bucket") or conn_config.get("container")
        self.region = conn_config.get("region")
        self.endpoint_url = conn_config.get("endpoint_url")
        self.client = None
        self.lock = threading.Lock()

    def connect(self) -> None:
        """
        Establish connection to cloud storage service.

        Raises:
            ValueError: If provider is not specified or supported
            Exception: If connection fails
        """
        if self.client:
            return

        if not self.provider:
            raise ValueError("Cloud connector requires 'provider' in config")

        credentials = self.config.get("credentials", {})

        if self.provider == "aws":
            import boto3
            self.client = boto3.client(
                "s3",
                region_name=self.region,
                endpoint_url=self.endpoint_url,
                **credentials
            )
        elif self.provider == "azure":
            from azure.storage.blob import BlobServiceClient
            account_url = credentials.get("account_url")
            if not account_url:
                account_name = credentials.get("account_name")
                account_key = credentials.get("account_key")
                account_url = f"https://{account_name}.blob.core.windows.net"
            self.client = BlobServiceClient(account_url=account_url, credential=account_key)
        elif self.provider == "gcp":
            from google.cloud import storage
            self.client = storage.Client(**credentials)
        elif self.provider == "ibm":
            import boto3
            # IBM COS uses S3-compatible API
            self.client = boto3.client(
                "s3",
                region_name=self.region,
                endpoint_url=self.endpoint_url,
                **credentials
            )
        elif self.provider == "oracle":
            import boto3
            # Oracle uses S3-compatible API
            self.client = boto3.client(
                "s3",
                region_name=self.region,
                endpoint_url=self.endpoint_url,
                **credentials
            )
        else:
            raise NotImplementedError(f"Cloud provider {self.provider} not supported")

    def disconnect(self) -> None:
        """
        Close cloud storage connection.
        """
        with self.lock:
            # Most cloud clients don't need explicit close
            self.client = None

    def read(self, object_key: str, query: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """
        Read data from cloud storage object.

        Args:
            object_key: Key/path of the object to read
            query: Optional query parameters

        Returns:
            List of dictionaries containing object data
        """
        self.connect()

        if self.provider == "aws":
            return self._read_s3_object(object_key)
        elif self.provider == "azure":
            return self._read_azure_blob(object_key)
        elif self.provider == "gcp":
            return self._read_gcs_object(object_key)
        elif self.provider in ["ibm", "oracle"]:
            return self._read_s3_object(object_key)
        else:
            raise NotImplementedError(f"Provider {self.provider} not supported for read")

    def _read_s3_object(self, object_key: str) -> List[Dict]:
        """Read object from S3-compatible storage."""
        try:
            response = self.client.get_object(Bucket=self.bucket, Key=object_key)
            content = response['Body'].read()

            # Determine format from file extension or config
            file_format = self._get_file_format(object_key)

            if file_format == "json":
                import json
                data = json.loads(content.decode('utf-8'))
                return data if isinstance(data, list) else [data]
            elif file_format == "csv":
                import pandas as pd
                import io
                df = pd.read_csv(io.BytesIO(content))
                return df.to_dict('records')
            elif file_format == "parquet":
                import pandas as pd
                import io
                df = pd.read_parquet(io.BytesIO(content))
                return df.to_dict('records')
            else:
                # Return raw content as single item
                return [{"content": content.decode('utf-8'), "key": object_key}]

        except self.client.exceptions.NoSuchKey:
            raise FileNotFoundError(f"Object {object_key} not found in bucket {self.bucket}")
        except Exception as e:
            raise Exception(f"Error reading S3 object: {e}")

    def _read_azure_blob(self, blob_name: str) -> List[Dict]:
        """Read blob from Azure Blob Storage."""
        try:
            blob_client = self.client.get_blob_client(container=self.bucket, blob=blob_name)
            content = blob_client.download_blob().readall()

            file_format = self._get_file_format(blob_name)

            if file_format == "json":
                import json
                data = json.loads(content.decode('utf-8'))
                return data if isinstance(data, list) else [data]
            elif file_format == "csv":
                import pandas as pd
                import io
                df = pd.read_csv(io.BytesIO(content))
                return df.to_dict('records')
            else:
                return [{"content": content.decode('utf-8'), "key": blob_name}]

        except Exception as e:
            raise Exception(f"Error reading Azure blob: {e}")

    def _read_gcs_object(self, object_name: str) -> List[Dict]:
        """Read object from Google Cloud Storage."""
        try:
            bucket = self.client.bucket(self.bucket)
            blob = bucket.blob(object_name)
            content = blob.download_as_text()

            file_format = self._get_file_format(object_name)

            if file_format == "json":
                import json
                data = json.loads(content)
                return data if isinstance(data, list) else [data]
            elif file_format == "csv":
                import pandas as pd
                import io
                df = pd.read_csv(io.StringIO(content))
                return df.to_dict('records')
            else:
                return [{"content": content, "key": object_name}]

        except Exception as e:
            raise Exception(f"Error reading GCS object: {e}")

    def write(self, data: List[Dict], object_key: str, mode: str = "overwrite") -> Dict[str, int]:
        """
        Write data to cloud storage object.

        Args:
            data: List of dictionaries to write
            object_key: Target object key/path
            mode: Write mode ("overwrite", "append" - append not always supported)

        Returns:
            Dictionary with operation statistics
        """
        self.connect()

        if self.provider == "aws":
            return self._write_s3_object(data, object_key, mode)
        elif self.provider == "azure":
            return self._write_azure_blob(data, object_key, mode)
        elif self.provider == "gcp":
            return self._write_gcs_object(data, object_key, mode)
        elif self.provider in ["ibm", "oracle"]:
            return self._write_s3_object(data, object_key, mode)
        else:
            raise NotImplementedError(f"Provider {self.provider} not supported for write")

    def _write_s3_object(self, data: List[Dict], object_key: str, mode: str) -> Dict[str, int]:
        """Write object to S3-compatible storage."""
        import pandas as pd
        import io

        df = pd.DataFrame(data)
        file_format = self._get_file_format(object_key)

        if file_format == "json":
            json_str = df.to_json(orient='records', indent=2)
            self.client.put_object(
                Bucket=self.bucket,
                Key=object_key,
                Body=json_str,
                ContentType='application/json'
            )
        elif file_format == "csv":
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            self.client.put_object(
                Bucket=self.bucket,
                Key=object_key,
                Body=buffer.getvalue(),
                ContentType='text/csv'
            )
        elif file_format == "parquet":
            buffer = io.BytesIO()
            df.to_parquet(buffer, index=False)
            self.client.put_object(
                Bucket=self.bucket,
                Key=object_key,
                Body=buffer.getvalue(),
                ContentType='application/octet-stream'
            )
        else:
            # Write as JSON by default
            json_str = df.to_json(orient='records', indent=2)
            self.client.put_object(
                Bucket=self.bucket,
                Key=object_key,
                Body=json_str,
                ContentType='application/json'
            )

        return {"written": len(data)}

    def _write_azure_blob(self, data: List[Dict], blob_name: str, mode: str) -> Dict[str, int]:
        """Write blob to Azure Blob Storage."""
        import pandas as pd
        import io

        df = pd.DataFrame(data)
        file_format = self._get_file_format(blob_name)

        blob_client = self.client.get_blob_client(container=self.bucket, blob=blob_name)

        if file_format == "json":
            json_str = df.to_json(orient='records', indent=2)
            blob_client.upload_blob(json_str, overwrite=True, content_type='application/json')
        elif file_format == "csv":
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            blob_client.upload_blob(buffer.getvalue(), overwrite=True, content_type='text/csv')
        else:
            json_str = df.to_json(orient='records', indent=2)
            blob_client.upload_blob(json_str, overwrite=True, content_type='application/json')

        return {"written": len(data)}

    def _write_gcs_object(self, data: List[Dict], object_name: str, mode: str) -> Dict[str, int]:
        """Write object to Google Cloud Storage."""
        import pandas as pd
        import io

        df = pd.DataFrame(data)
        file_format = self._get_file_format(object_name)

        bucket = self.client.bucket(self.bucket)
        blob = bucket.blob(object_name)

        if file_format == "json":
            json_str = df.to_json(orient='records', indent=2)
            blob.upload_from_string(json_str, content_type='application/json')
        elif file_format == "csv":
            buffer = io.StringIO()
            df.to_csv(buffer, index=False)
            blob.upload_from_string(buffer.getvalue(), content_type='text/csv')
        else:
            json_str = df.to_json(orient='records', indent=2)
            blob.upload_from_string(json_str, content_type='application/json')

        return {"written": len(data)}

    def list_objects(self, prefix: str = "") -> List[str]:
        """
        List objects in the storage bucket/container.

        Args:
            prefix: Object key prefix to filter by

        Returns:
            List of object keys
        """
        self.connect()

        if self.provider == "aws":
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        elif self.provider == "azure":
            container_client = self.client.get_container_client(self.bucket)
            blobs = container_client.list_blobs(name_starts_with=prefix)
            return [blob.name for blob in blobs]
        elif self.provider == "gcp":
            bucket = self.client.bucket(self.bucket)
            blobs = bucket.list_blobs(prefix=prefix)
            return [blob.name for blob in blobs]
        elif self.provider in ["ibm", "oracle"]:
            response = self.client.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
            return [obj['Key'] for obj in response.get('Contents', [])]
        else:
            raise NotImplementedError(f"Provider {self.provider} not supported for list")

    def delete_object(self, object_key: str) -> bool:
        """
        Delete object from storage.

        Args:
            object_key: Key of object to delete

        Returns:
            True if deletion successful
        """
        self.connect()

        try:
            if self.provider == "aws":
                self.client.delete_object(Bucket=self.bucket, Key=object_key)
            elif self.provider == "azure":
                blob_client = self.client.get_blob_client(container=self.bucket, blob=object_key)
                blob_client.delete_blob()
            elif self.provider == "gcp":
                bucket = self.client.bucket(self.bucket)
                blob = bucket.blob(object_key)
                blob.delete()
            elif self.provider in ["ibm", "oracle"]:
                self.client.delete_object(Bucket=self.bucket, Key=object_key)
            return True
        except Exception:
            return False

    def get_schema(self) -> Dict[str, Any]:
        """
        Get schema information for the storage bucket/container.

        Returns:
            Dictionary containing bucket/container metadata
        """
        self.connect()

        try:
            if self.provider == "aws":
                response = self.client.head_bucket(Bucket=self.bucket)
                return {"metadata": dict(response)}
            elif self.provider == "azure":
                container_client = self.client.get_container_client(self.bucket)
                properties = container_client.get_container_properties()
                return {"metadata": properties}
            elif self.provider == "gcp":
                bucket = self.client.bucket(self.bucket)
                return {"metadata": {"name": bucket.name, "location": bucket.location}}
            else:
                return {}
        except Exception:
            return {}

    def validate(self) -> bool:
        """
        Validate cloud storage connectivity and permissions.

        Returns:
            True if validation passes
        """
        try:
            self.connect()
            # Try to list objects (should work if credentials are valid)
            self.list_objects()
            return True
        except Exception:
            return False

    def _get_file_format(self, object_key: str) -> str:
        """Determine file format from object key or config."""
        # Check config first
        configured_format = self.config.get("file_format")
        if configured_format:
            return configured_format

        # Infer from file extension
        if object_key.endswith('.json'):
            return 'json'
        elif object_key.endswith('.csv'):
            return 'csv'
        elif object_key.endswith('.parquet'):
            return 'parquet'
        elif object_key.endswith('.txt'):
            return 'text'
        else:
            return 'json'  # Default to JSON</content>
#<parameter name="filePath">d:\development\2026\metadata-etl-framework\src\connectors\cloud_connector.py