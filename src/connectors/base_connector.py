"""
Base Connector Interface

This module defines the abstract base class for all data connectors in the ETL framework.
All connector implementations must inherit from BaseConnector and implement its abstract methods.
"""

import logging
import threading
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """
    Abstract base class for all data connectors.

    This class defines the standard interface that all connectors must implement,
    providing a unified way to interact with different data sources and destinations.
    """

    def __init__(self, config: Dict[str, Any]):
        """
        Initialize the connector with configuration.

        Args:
            config: Dictionary containing connector-specific configuration parameters
        """
        self.config = config
        self.logger = logging.getLogger(f"{self.__class__.__name__}")
        self._connected = False
        self._lock = threading.Lock()

    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to the data source.

        This method must be implemented by all concrete connector classes.
        It should establish the necessary connections, authenticate if required,
        and prepare the connector for read/write operations.

        Raises:
            ConnectionError: If connection cannot be established
            ValueError: If configuration is invalid
        """
        pass

    @abstractmethod
    def disconnect(self) -> None:
        """
        Close connection to the data source.

        This method must be implemented by all concrete connector classes.
        It should properly close all connections, release resources,
        and clean up any temporary state.

        Note: This method should be idempotent and not raise exceptions
        if called when already disconnected.
        """
        pass

    @abstractmethod
    def read(self, query: Any, **kwargs) -> Any:
        """
        Read data from the data source.

        Args:
            query: Query specification (format depends on connector type)
            **kwargs: Additional connector-specific parameters

        Returns:
            Data read from the source (format depends on connector)

        Raises:
            ConnectionError: If not connected or connection lost
            ValueError: If query is invalid
        """
        pass

    @abstractmethod
    def write(self, data: Any, destination: Any, **kwargs) -> Dict[str, int]:
        """
        Write data to the data destination.

        Args:
            data: Data to write (format depends on connector)
            destination: Destination specification (format depends on connector)
            **kwargs: Additional connector-specific parameters

        Returns:
            Dictionary with operation statistics (e.g., {"inserted": 10, "updated": 5})

        Raises:
            ConnectionError: If not connected or connection lost
            ValueError: If data or destination is invalid
        """
        pass

    @abstractmethod
    def get_schema(self) -> Dict[str, Any]:
        """
        Get schema information for the data source/destination.

        Returns:
            Dictionary containing schema information (columns, types, etc.)

        Raises:
            ConnectionError: If not connected or connection lost
        """
        pass

    @abstractmethod
    def validate(self) -> bool:
        """
        Validate the connector configuration and connectivity.

        Returns:
            True if validation passes, False otherwise

        Note: This method should not raise exceptions. It should return False
        for any validation failures and log appropriate error messages.
        """
        pass

    def is_connected(self) -> bool:
        """
        Check if the connector is currently connected.

        Returns:
            True if connected, False otherwise
        """
        return self._connected

    def test_connection(self) -> bool:
        """
        Test the connection to the data source.

        This is a convenience method that attempts to connect if not already connected,
        validates the connection, and then disconnects.

        Returns:
            True if connection test passes, False otherwise
        """
        try:
            if not self.is_connected():
                self.connect()
            result = self.validate()
            return result
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
            return False
        finally:
            try:
                self.disconnect()
            except Exception:
                pass  # Ignore disconnect errors during testing