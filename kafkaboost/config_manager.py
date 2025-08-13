import os
import json
import time
import threading
import requests
from typing import Dict, Any, Optional
from datetime import datetime, timedelta

class ConfigManager:
    _instance = None
    _lock = threading.Lock()
    
    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super(ConfigManager, cls).__new__(cls)
                    cls._instance._initialized = False
        return cls._instance
    
    def __init__(self):
        if self._initialized:
            return
            
        self._initialized = True
        self._config: Dict[str, Any] = {}
        self._last_update: Optional[datetime] = None
        self._update_interval = timedelta(minutes=5)
        self._config_url = os.getenv('CONFIG_SERVER_URL')
        self._user_id = os.getenv('USER_ID')
        
        if not self._config_url:
            raise ValueError("CONFIG_SERVER_URL environment variable is not set")
        if not self._user_id:
            raise ValueError("USER_ID environment variable is not set")
        
    def _fetch_config(self) -> None:
        """Fetch the configuration from the HTTP endpoint."""
        try:
            headers = {
                'Authorization': f'{self._user_id}',
                'Content-Type': 'application/json'
            }
            response = requests.get(
                self._config_url,
                headers=headers
            )
            response.raise_for_status()  # Raise an exception for bad status codes
            self._config = response.json()
            self._last_update = datetime.now()
            print(f"Config updated successfully at {self._last_update}")
        except requests.RequestException as e:
            print(f"Error fetching config from server: {str(e)}")
            if not self._config:
                raise RuntimeError("Failed to fetch initial config from server")
    
    def get_config(self) -> Dict[str, Any]:
        """
        Get the current configuration, fetching from server if necessary.
        
        Returns:
            The current configuration dictionary
        """
        if (self._last_update is None or 
            datetime.now() - self._last_update >= self._update_interval):
            self._fetch_config()
        return self._config.copy()
    
    def force_update(self) -> None:
        """Force an immediate update of the configuration from the server."""
        self._fetch_config() 