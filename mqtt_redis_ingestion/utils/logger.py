import json
import logging
import logging.config
import os
from pathlib import Path
from typing import Optional


class LoggerManager:
    """
    Manager class for handling logging configuration and logger creation.
    """
    
    _configured = False
    
    @classmethod
    def configure_logging(cls, config_path: Optional[str] = None) -> None:
        """
        Configure logging from JSON configuration file.
        
        Args:
            config_path: Path to logging configuration JSON file.
                        If None, searches for default locations.
        """
        if cls._configured:
            return
        
        try:
            # Find logging configuration file
            if config_path is None:
                possible_paths = [
                    Path("config/logging_config.json"),
                    Path("../config/logging_config.json"),
                    Path("logging_config.json"),
                    Path(os.path.join(os.path.dirname(__file__), "../../logging_config.json"))
                ]
                
                config_path = None
                for path in possible_paths:
                    if path.exists():
                        config_path = path
                        break
            
            if config_path is None:
                # Fallback to basic configuration
                cls._configure_basic_logging()
                return
            
            # Load and apply logging configuration
            with open(config_path, 'r') as config_file:
                config_dict = json.load(config_file)
            
            # Ensure log directory exists
            cls._ensure_log_directories(config_dict)
            
            # Apply configuration
            logging.config.dictConfig(config_dict)
            cls._configured = True
            
            # Log configuration success
            logger = logging.getLogger(__name__)
            logger.info(f"Logging configured from {config_path}")
            
        except Exception as e:
            print(f"Failed to configure logging from file: {e}")
            cls._configure_basic_logging()
    
    @classmethod
    def _configure_basic_logging(cls) -> None:
        """Configure basic logging as fallback."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
            handlers=[logging.StreamHandler()]
        )
        cls._configured = True
        print("Using basic logging configuration")
    
    @classmethod
    def _ensure_log_directories(cls, config_dict: dict) -> None:
        """
        Ensure that all log directories specified in config exist.
        
        Args:
            config_dict: Logging configuration dictionary
        """
        handlers = config_dict.get('handlers', {})
        
        for handler_name, handler_config in handlers.items():
            if 'filename' in handler_config:
                log_file_path = Path(handler_config['filename'])
                log_dir = log_file_path.parent
                
                # Create directory if it doesn't exist
                log_dir.mkdir(parents=True, exist_ok=True)
    
    @classmethod
    def get_logger(cls, name: str) -> logging.Logger:
        """
        Get a configured logger instance.
        
        Args:
            name: Logger name
            
        Returns:
            Configured logger instance
        """
        if not cls._configured:
            cls.configure_logging()
        
        return logging.getLogger(name)


def get_logger(name: str) -> logging.Logger:
    """
    Convenience function to get a configured logger.
    
    Args:
        name: Logger name (typically __name__)
        
    Returns:
        Configured logger instance
    """
    return LoggerManager.get_logger(name)


def configure_logging(config_path: Optional[str] = None) -> None:
    """
    Convenience function to configure logging.
    
    Args:
        config_path: Path to logging configuration file
    """
    LoggerManager.configure_logging(config_path)
