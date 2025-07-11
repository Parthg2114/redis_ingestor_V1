import json
import os
from typing import Dict, Any, Optional
from pathlib import Path

class ConfigManager:
   
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConfigManager, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self._config = self._load_config()
    
    def _load_config(self) -> Dict[str, Any]:
        
        try:
            
            possible_paths = [
                Path("config/config.json"),
                Path("../config/config.json"),
                Path("config.json"),
                Path(os.path.join(os.path.dirname(__file__), "../../config/config.json"))
            ]
            
            config_path = None
            for path in possible_paths:
                if path.exists():
                    config_path = path
                    break
            
            if config_path is None:
                raise FileNotFoundError("Configuration file not found in any expected location")
            
            with open(config_path, 'r') as config_file:
                config_data = json.load(config_file)
                
            return config_data
            
        except FileNotFoundError as e:
            raise FileNotFoundError(f"Configuration file not found: {e}")
        except json.JSONDecodeError as e:
            raise json.JSONDecodeError(f"Invalid JSON in configuration file: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
       
        keys = key.split('.')
        value = self._config
        
        try:
            for k in keys:
                value = value[k]
            return value
        except (KeyError, TypeError):
            return default
    
    def get_mqtt_config(self) -> Dict[str, Any]:
        """Get MQTT configuration section."""
        return self.get('mqtt', {})
    
    def get_redis_config(self) -> Dict[str, Any]:
        """Get Redis configuration section."""
        return self.get('redis', {})
    
    def get_app_config(self) -> Dict[str, Any]:
        """Get application configuration section."""
        return self.get('application', {})
    
    def get_data_processing_config(self) -> Dict[str, Any]:
        """Get data processing configuration section."""
        return self.get('data_processing', {})
    
    def reload_config(self):
        """Reload configuration from file."""
        self._config = self._load_config()
    
    @property
    def config(self) -> Dict[str, Any]:
        """Get the entire configuration dictionary."""
        return self._config.copy()


# Create a global instance for easy access
config_manager = ConfigManager()
