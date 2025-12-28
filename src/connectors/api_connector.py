"""
API Connector

Provides connectivity to REST APIs, GraphQL endpoints, and web services
through a unified interface with authentication, pagination, and error handling.
"""

import logging
import threading
import time
from typing import Any, Dict, List, Optional, Tuple

import requests

from .base_connector import BaseConnector

logger = logging.getLogger(__name__)


class APIConnector(BaseConnector):
    """
    Connector for REST APIs and web services.

    Supports multiple authentication methods and API types:
    - REST APIs with various auth methods (Basic, Bearer, API Key, OAuth)
    - GraphQL endpoints
    - SOAP web services (limited support)
    - Pagination handling (offset, cursor, page-based)
    """

    def __init__(self, conn_config: Dict[str, Any]):
        """
        Initialize the API connector.

        Args:
            conn_config: Configuration dictionary containing:
                - base_url: Base URL for the API
                - auth_type: Authentication type ('basic', 'bearer', 'api_key', 'oauth2')
                - auth_config: Authentication configuration
                - headers: Default headers
                - timeout: Request timeout in seconds
                - retry_config: Retry configuration
        """
        super().__init__(conn_config)
        self.base_url = conn_config.get("base_url", "").rstrip("/")
        self.auth_type = conn_config.get("auth_type")
        self.auth_config = conn_config.get("auth_config", {})
        self.headers = conn_config.get("headers", {})
        self.timeout = conn_config.get("timeout", 30)
        self.retry_config = conn_config.get("retry_config", {"max_retries": 3, "backoff_factor": 0.3})
        self.session = None
        self.lock = threading.Lock()

    def connect(self) -> None:
        """
        Establish connection to the API (create session with auth).
        """
        if self.session:
            return

        self.session = requests.Session()
        self.session.headers.update(self.headers)

        # Configure authentication
        if self.auth_type == "basic":
            from requests.auth import HTTPBasicAuth
            username = self.auth_config.get("username")
            password = self.auth_config.get("password")
            self.session.auth = HTTPBasicAuth(username, password)
        elif self.auth_type == "bearer":
            token = self.auth_config.get("token")
            self.session.headers["Authorization"] = f"Bearer {token}"
        elif self.auth_type == "api_key":
            key_name = self.auth_config.get("key_name", "X-API-Key")
            key_value = self.auth_config.get("key_value")
            self.session.headers[key_name] = key_value
        elif self.auth_type == "oauth2":
            # OAuth2 implementation would go here
            # For now, assume token is provided
            token = self.auth_config.get("access_token")
            if token:
                self.session.headers["Authorization"] = f"Bearer {token}"

    def disconnect(self) -> None:
        """
        Close the API session.
        """
        with self.lock:
            if self.session:
                self.session.close()
                self.session = None

    def read(self, endpoint: str, params: Optional[Dict[str, Any]] = None,
             method: str = "GET", data: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """
        Execute API request and return results.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            method: HTTP method
            data: Request body data

        Returns:
            List of dictionaries containing API response data
        """
        self.connect()

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        # Handle pagination
        all_results = []
        page = 1
        has_more = True

        while has_more:
            current_params = params.copy() if params else {}
            current_params.update(self._get_pagination_params(page))

            response = self._make_request(method, url, current_params, data)
            response_data = response.json()

            # Extract data from response
            results = self._extract_data_from_response(response_data)
            all_results.extend(results)

            # Check for more pages
            has_more = self._has_more_pages(response_data, page)
            page += 1

            # Safety limit
            if page > 100:  # Prevent infinite loops
                break

        return all_results

    def _make_request(self, method: str, url: str, params: Optional[Dict] = None,
                     data: Optional[Dict] = None) -> requests.Response:
        """Make HTTP request with retry logic."""
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry

        # Configure retries
        retry_strategy = Retry(
            total=self.retry_config.get("max_retries", 3),
            backoff_factor=self.retry_config.get("backoff_factor", 0.3),
            status_forcelist=[429, 500, 502, 503, 504],
        )

        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        response = self.session.request(
            method=method,
            url=url,
            params=params,
            json=data,
            timeout=self.timeout
        )
        response.raise_for_status()
        return response

    def _get_pagination_params(self, page: int) -> Dict[str, Any]:
        """Get pagination parameters based on configured pagination type."""
        pagination_type = self.config.get("pagination_type", "none")

        if pagination_type == "offset":
            page_size = self.config.get("page_size", 100)
            return {"offset": (page - 1) * page_size, "limit": page_size}
        elif pagination_type == "page":
            return {"page": page, "per_page": self.config.get("page_size", 100)}
        elif pagination_type == "cursor":
            # Cursor-based pagination would need cursor tracking
            return {}
        else:
            return {}

    def _extract_data_from_response(self, response_data: Dict[str, Any]) -> List[Dict]:
        """Extract data array from API response."""
        data_path = self.config.get("data_path", "")
        if data_path:
            # Navigate to data using dot notation (e.g., "data.items")
            current = response_data
            for key in data_path.split("."):
                current = current.get(key, {})
            return current if isinstance(current, list) else [current]
        else:
            # Assume response is directly the data array
            return response_data if isinstance(response_data, list) else [response_data]

    def _has_more_pages(self, response_data: Dict[str, Any], current_page: int) -> bool:
        """Check if there are more pages to fetch."""
        pagination_type = self.config.get("pagination_type", "none")

        if pagination_type == "none":
            return False

        # Check for pagination indicators
        if "has_more" in response_data:
            return response_data["has_more"]
        elif "next_page" in response_data:
            return response_data["next_page"] is not None
        elif "total_pages" in response_data:
            return current_page < response_data["total_pages"]
        elif "total_count" in response_data and "data" in response_data:
            page_size = self.config.get("page_size", 100)
            return len(response_data["data"]) == page_size

        return False

    def write(self, endpoint: str, data: List[Dict], method: str = "POST") -> Dict[str, int]:
        """
        Write data to API endpoint.

        Args:
            endpoint: API endpoint path
            data: List of dictionaries to send
            method: HTTP method ("POST", "PUT", "PATCH")

        Returns:
            Dictionary with operation statistics
        """
        self.connect()

        url = f"{self.base_url}/{endpoint.lstrip('/')}"
        success_count = 0

        for item in data:
            try:
                response = self._make_request(method, url, data=item)
                if response.status_code in [200, 201, 202]:
                    success_count += 1
                else:
                    logger.warning(f"Failed to write item: {response.status_code}")
            except Exception as e:
                logger.error(f"Error writing item: {e}")

        return {"written": success_count, "failed": len(data) - success_count}

    def get_schema(self) -> Dict[str, Any]:
        """
        Get schema information from API documentation or sample response.

        Returns:
            Dictionary containing field information
        """
        # Try to get schema from OpenAPI/Swagger endpoint
        schema_url = self.config.get("schema_url")
        if schema_url:
            try:
                response = self._make_request("GET", schema_url)
                schema_data = response.json()
                # Parse OpenAPI schema - simplified implementation
                return {"endpoints": list(schema_data.get("paths", {}).keys())}
            except Exception:
                pass

        # Fallback: get sample data and infer schema
        sample_endpoint = self.config.get("sample_endpoint")
        if sample_endpoint:
            try:
                sample_data = self.read(sample_endpoint, {"limit": 1})
                if sample_data:
                    return {"fields": list(sample_data[0].keys())}
            except Exception:
                pass

        return {}

    def validate(self) -> bool:
        """
        Validate API connectivity and authentication.

        Returns:
            True if validation passes
        """
        try:
            self.connect()
            # Try a simple health check or ping endpoint
            health_endpoint = self.config.get("health_endpoint", "/health")
            response = self._make_request("GET", f"{self.base_url}/{health_endpoint.lstrip('/')}")
            return response.status_code < 400
        except Exception:
            return False

    def graphql_query(self, query: str, variables: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """
        Execute GraphQL query.

        Args:
            query: GraphQL query string
            variables: Query variables

        Returns:
            GraphQL response data
        """
        self.connect()

        graphql_endpoint = self.config.get("graphql_endpoint", "/graphql")
        url = f"{self.base_url}/{graphql_endpoint.lstrip('/')}"

        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        response = self._make_request("POST", url, data=payload)
        return response.json()
    #</content>
#<parameter name="filePath">d:\development\2026\metadata-etl-framework\src\connectors\api_connector.py