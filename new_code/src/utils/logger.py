import logging
import logging.config
from pathlib import Path
import json
from typing import Optional, Dict, Any
from logging.handlers import RotatingFileHandler
import os

class TableauLogger:
    """
    Custom logger for Tableau application using configuration from config.json.
    Supports both file and console logging with different log levels.
    """

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize logger with custom configuration.
        If config_path is not provided, uses default path.
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent.parent / 'config' / 'config.json'
        self.config_path = config_path
        self.config = self._load_config()
        self.logger = self._setup_logger()

    def _load_config(self) -> Dict[str, Any]:
        """
        Load and return the logging configuration from JSON.
        Returns a minimal disabled config on failure.
        """
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            return config.get('logger', {})
        except Exception as e:
            print(f"Error loading logger configuration: {str(e)}")
            # Return a minimal config to prevent crash
            return {
                "logger_name": "tableau_logger",
                "log_level": "INFO",
                "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                "log_dir": "logs",
                "log_file_name": "app.log",
                "console_log_level": "INFO",
                "file_log_level": "ERROR",
                "logging_enabled": False
            }

    def _setup_logger(self) -> logging.Logger:
        """
        Setup and configure the logger with file handler only.
        """
        # Create logger
        logger = logging.getLogger(self.config.get('logger_name', 'tableau_logger'))
        logger.setLevel(self.config.get('log_level', 'INFO'))
        
        # Remove any existing handlers
        logger.handlers = []
        
        # Create formatter
        formatter = logging.Formatter(self.config.get('log_format'))
        
        # Setup file handler if logging is enabled
        if self.config.get('logging_enabled', False):
            # Create logs directory
            log_dir = Path(__file__).parent.parent.parent / self.config.get('log_dir', 'logs')
            log_dir.mkdir(parents=True, exist_ok=True)
            
            # Setup file handler with rotation
            log_file = log_dir / self.config.get('log_file_name', 'app.log')
            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=10*1024*1024,  # 10MB
                backupCount=5
            )
            file_handler.setLevel(self.config.get('file_log_level', 'ERROR'))
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
            
            logger.info("Logger initialized with file logging only")
        else:
            logger.disabled = True
            
        return logger

    def reload_config(self) -> None:
        """
        Reload the logging configuration from file and reinitialize the logger.
        """
        self.config = self._load_config()
        self.logger = self._setup_logger()

    def is_enabled(self) -> bool:
        """
        Check if logging is enabled.
        """
        return not self.logger.disabled

    def enable_logging(self) -> None:
        """
        Enable logging and update the config file.
        """
        self.config['logging_enabled'] = True
        self._update_config()
        self.logger = self._setup_logger()

    def disable_logging(self) -> None:
        """
        Disable logging and update the config file.
        """
        self.config['logging_enabled'] = False
        self._update_config()
        self.logger = self._setup_logger()

    def _update_config(self) -> None:
        """
        Update the configuration file (JSON).
        """
        try:
            # Read the entire config file
            with open(self.config_path, 'r') as f:
                full_config = json.load(f)
            
            # Update the logger section
            full_config['logger'] = self.config
            
            # Write back the entire config
            with open(self.config_path, 'w') as f:
                json.dump(full_config, f, indent=2)
        except Exception as e:
            print(f"Error updating logger configuration: {str(e)}")

    def info(self, message: str) -> None:
        """Log info message if logging is enabled."""
        if not self.logger.disabled:
            self.logger.info(message)

    def warning(self, message: str) -> None:
        """Log warning message if logging is enabled."""
        if not self.logger.disabled:
            self.logger.warning(message)

    def error(self, message: str) -> None:
        """Log error message if logging is enabled."""
        if not self.logger.disabled:
            self.logger.error(message)

    def debug(self, message: str) -> None:
        """Log debug message if logging is enabled."""
        if not self.logger.disabled:
            self.logger.debug(message)

    def critical(self, message: str) -> None:
        """Log critical message if logging is enabled."""
        if not self.logger.disabled:
            self.logger.critical(message)

# Create the singleton logger instance
app_logger = TableauLogger()

# Example usage:
if __name__ == "__main__":
    # Test the logger
    app_logger.info("Logger initialized")
    app_logger.debug("This is a debug message")
    app_logger.info("This is an info message")
    app_logger.warning("This is a warning message")
    app_logger.error("This is an error message")
    
    # Test enabling/disabling
    app_logger.disable_logging()
    app_logger.info("This won't be logged")
    
    app_logger.enable_logging()
    app_logger.info("This will be logged again")
