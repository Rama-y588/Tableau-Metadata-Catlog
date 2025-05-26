import logging
import logging.config
from pathlib import Path
import json
from typing import Optional, Dict, Any
from dotenv import load_dotenv
import os

class TableauLogger:
    """
    Custom logger for Tableau application using configuration from logger_config.json.
    Supports dynamic enabling/disabling and runtime config reloading.
    """

    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize logger with custom configuration.
        If config_path is not provided, uses default path.
        """
        if config_path is None:
            config_path = root_dir / 'config' / 'logger_config.json'
        self.config_path = config_path
        self.config = self._load_config()
        self._setup_logging()

    def _load_config(self) -> Dict[str, Any]:
        """
        Load and return the logging configuration from JSON.
        Returns a minimal disabled config on failure.
        """
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            return config
        except Exception as e:
            log = logging.getLogger("fallback")
            log.error(f"Error loading logger configuration: {str(e)}")
            # Return a minimal config to prevent crash
            return {
                "version": 1,
                "disable_existing_loggers": False,
                "logging_enabled": False,
                "handlers": {},
                "loggers": {}
            }

    def _setup_logging(self) -> None:
        """
        Setup logging using the configuration file.
        If logging is disabled, disables the logger.
        """
        try:
            if not self.config.get('logging_enabled', False):
                self.logger = logging.getLogger('tableau')
                self.logger.disabled = True
                return

            # Ensure logs directory exists
            log_dir = Path(__file__).resolve().parent.parent.parent / 'logs'
            log_dir.mkdir(parents=True, exist_ok=True)

            # Update log file path for file handler if present
            handlers = self.config.get('handlers', {})
            if 'file' in handlers and 'filename' in handlers['file']:
                handlers['file']['filename'] = str(log_dir / Path(handlers['file']['filename']).name)

            # Configure logging
            logging.config.dictConfig(self.config)

            # Get logger and set enabled/disabled state
            logger_name = next(iter(self.config.get('loggers', {'tableau': {}})))
            self.logger = logging.getLogger(logger_name)
            self.logger.disabled = not self.config.get('loggers', {}).get(logger_name, {}).get('enabled', True)

            if not self.logger.disabled:
                self.logger.info("Logger initialized with custom configuration")
        except Exception as e:
            log = logging.getLogger("fallback")
            log.error(f"Error setting up logger: {str(e)}")
            self.logger = logging.getLogger('tableau')
            self.logger.disabled = True

    def reload_config(self) -> None:
        """
        Reload the logging configuration from file and reinitialize the logger.
        """
        self.config = self._load_config()
        self._setup_logging()

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
        self._setup_logging()

    def disable_logging(self) -> None:
        """
        Disable logging and update the config file.
        """
        self.config['logging_enabled'] = False
        self._update_config()
        self._setup_logging()

    def _update_config(self) -> None:
        """
        Update the configuration file (JSON).
        """
        try:
            with open(self.config_path, 'w') as f:
                json.dump(self.config, f, indent=2)
        except Exception as e:
            log = logging.getLogger("fallback")
            log.error(f"Error updating logger configuration: {str(e)}")

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

# ---------------------- Singleton Instance Setup ----------------------

# Load environment variables from .env (three levels up)
currentdir = Path(__file__).resolve()
root_dir = currentdir.parent.parent.parent
log = logging.getLogger("fallback")

# Load .env from root_dir
dotenv_path = root_dir / '.env'
load_dotenv(dotenv_path=dotenv_path)

# Determine config file path from environment or use default
config_file_path_env = os.getenv('CONFIG_FILE_PATH')
if config_file_path_env:
    config_relative_path = Path(config_file_path_env)
else:
    log.warning("Warning: 'CONFIG_FILE_PATH' environment variable not found. Using default relative path 'config/logger_config.json'.")
    config_relative_path = Path("config") / "logger_config.json"

configfilepath = root_dir / config_relative_path

# Create the logger instance
app_logger = TableauLogger(config_path=configfilepath)


# Example usage:
"""
from src.utils.custom_logger import app_logger

# Check if logging is enabled
if app_logger.is_enabled():
    app_logger.info("Logging is enabled")

# Enable/disable logging
app_logger.disable_logging()
app_logger.enable_logging()

# Reload configuration from file
app_logger.reload_config()

# Log messages (will only log if enabled)
app_logger.info("This is an info message")
app_logger.warning("This is a warning message")
app_logger.error("This is an error message")
"""
