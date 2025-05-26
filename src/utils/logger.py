import logging
import yaml
from pathlib import Path
from logging.handlers import RotatingFileHandler
from typing import Dict, Any, Optional

class TableauLogger:
    """
    A minimalist and efficient logging utility. It configures a logger instance
    based on a YAML file or uses a default setup if the file is unavailable
    or invalid.
    """
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initializes the logger. It attempts to load configuration from the
        specified YAML file or falls back to sensible defaults.
        """
        self.logger = logging.getLogger("TableauLogger")
        self.logger.propagate = False # Prevent messages from bubbling to the root logger

        # --- REMOVED: Initial temporary console handler ---
        # The philosophy here is to assume the config will dictate all logging behavior,
        # even for setup messages. This means if config fails, initial setup messages
        # might not appear, which could make debugging harder.
        # if not self.logger.handlers:
        #     self.logger.addHandler(logging.StreamHandler())
        #     self.logger.setLevel(logging.INFO)

        # Determine config file path; assumes script is in <project_root>/src/utils
        self.config_path = config_path or Path(__file__).resolve().parents[2] / 'config' / 'logger_config.yaml'

        # Load config or use defaults, then apply it
        # Note: If _load_or_default_config itself logs using self.logger, and
        # no handlers are yet present, those messages won't appear.
        self.config = self._load_or_default_config()
        self._apply_config()

    def _load_or_default_config(self) -> Dict[str, Any]:
        """
        Attempts to load logging configuration from the YAML file.
        If loading fails (file not found, invalid YAML, or missing 'logger' key),
        it returns a default configuration.
        """
        # Note: These informational messages will only appear if a handler capable
        # of handling INFO level logs is already attached to self.logger.
        # With the initial StreamHandler removed, these might not be visible
        # if config loading fails.
        self.logger.info(f"Attempting to load configuration from: '{self.config_path}'.")
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                full_config = yaml.safe_load(f)

            if isinstance(full_config, dict) and 'logger' in full_config and isinstance(full_config['logger'], dict):
                self.logger.info("Configuration loaded successfully from file.")
                return full_config['logger']
            else:
                self.logger.warning(f"Config file '{self.config_path}' is invalid or missing 'logger' section. Using default config.")
                return self._default_config_settings()

        except FileNotFoundError:
            self.logger.warning(f"Configuration file not found at '{self.config_path}'. Using default config.")
            return self._default_config_settings()
        except yaml.YAMLError as e:
            self.logger.error(f"Failed to parse YAML configuration '{self.config_path}': {e}. Using default config.", exc_info=True)
            return self._default_config_settings()
        except Exception as e:
            self.logger.error(f"An unexpected error occurred loading config from '{self.config_path}': {e}. Using default config.", exc_info=True)
            return self._default_config_settings()

    def _default_config_settings(self) -> Dict[str, Any]:
        """Returns a sensible default logging configuration."""
        self.logger.info("Applying default logger configuration.")
        return {
            "logger_name": "tableau_logger",
            "log_level": "INFO",
            "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
            "log_dir": "logs",
            "log_file_name": "app.log",
            "file_log_level": "ERROR",
            "console_log_level": "INFO",
            "logging_enabled": True,
            "console_logging": "disabled" # Ensure this is set to disabled in your YAML too!
        }

    def _get_log_level(self, level_name: str) -> int:
        """Converts a string log level to its integer constant. Defaults to INFO if unrecognized."""
        level = getattr(logging, level_name.upper(), None)
        if level is None:
            self.logger.warning(f"Unknown log level '{level_name}' in config. Defaulting to INFO.")
            return logging.INFO
        return level

    def _apply_config(self) -> None:
        """
        Applies the current configuration to the logger instance. This involves
        setting the overall level, clearing old handlers, and adding new
        file and console handlers based on the loaded configuration.
        """
        self.logger.setLevel(self._get_log_level(self.config.get('log_level', 'INFO')))

        # Clear existing handlers (will be empty if initial one was removed)
        for handler in self.logger.handlers[:]:
            self.logger.removeHandler(handler)

        if not self.config.get('logging_enabled', False):
            self.logger.disabled = True
            # This message will only appear if a handler is added later, or if this
            # state is checked by an external system.
            self.logger.info("Logger explicitly disabled by configuration.")
            return

        formatter = logging.Formatter(self.config.get('log_format'))

        # Configure and add File Handler
        try:
            log_dir = Path(__file__).resolve().parents[2] / self.config.get('log_dir', 'logs')
            log_dir.mkdir(parents=True, exist_ok=True)
            log_file = log_dir / self.config.get('log_file_name', 'app.log')

            file_handler = RotatingFileHandler(
                log_file,
                maxBytes=10 * 1024 * 1024,
                backupCount=5,
                encoding='utf-8'
            )
            file_handler.setLevel(self._get_log_level(self.config.get('file_log_level', 'ERROR')))
            file_handler.setFormatter(formatter)
            self.logger.addHandler(file_handler)
            self.logger.debug(f"File logging enabled: '{log_file}' (Level: {self.config.get('file_log_level', 'ERROR')}).")
        except Exception as e:
            self.logger.error(f"Failed to create file handler: {e}. File logging disabled.", exc_info=True)

        # Configure and add Console Handler (ONLY if enabled in config)
        if self.config.get('console_logging', 'enabled').lower() == 'enabled':
            console_handler = logging.StreamHandler()
            console_handler.setLevel(self._get_log_level(self.config.get('console_log_level', 'INFO')))
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
            self.logger.debug(f"Console logging enabled (Level: {self.config.get('console_log_level', 'INFO')}).")
        else:
            # This message itself will only appear if a file handler is active and its level permits.
            self.logger.info("Console logging disabled by configuration.")

        self.logger.disabled = False
        self.logger.info(f"Logger '{self.config.get('logger_name')}' initialized successfully with overall level {self.config.get('log_level', 'INFO')}.")

    def debug(self, message: str, *args, **kwargs): self._log(logging.DEBUG, message, *args, **kwargs)
    def info(self, message: str, *args, **kwargs): self._log(logging.INFO, message, *args, **kwargs)
    def warning(self, message: str, *args, **kwargs): self._log(logging.WARNING, message, *args, **kwargs)
    def error(self, message: str, *args, **kwargs): self._log(logging.ERROR, message, *args, **kwargs)
    def critical(self, message: str, *args, **kwargs): self._log(logging.CRITICAL, message, *args, **kwargs)
    def exception(self, message: str, *args, **kwargs): self._log(logging.ERROR, message, exc_info=True, *args, **kwargs)

    def _log(self, level: int, message: str, *args, **kwargs):
        try:
            if not self.logger.disabled:
                self.logger.log(level, message, *args, **kwargs)
        except Exception as e:
            print(f"[LOGGER CRITICAL FAILURE] Unable to log message '{message}' (Level: {logging.getLevelName(level)}): {e}")

app_logger = TableauLogger()

if __name__ == '__main__':
    app_logger.info("--- Starting TableauLogger Test (Post-initialization) ---")
    app_logger.info("This INFO message should now only go to the file if console_logging is disabled.")
    app_logger.debug("This DEBUG message should also go only to the file if configured.")
    app_logger.warning("A warning message.")
    app_logger.error("An error message.")
    app_logger.critical("A critical message.")

    try:
        result = 1 / 0
    except ZeroDivisionError:
        app_logger.exception("An exception occurred while trying to divide by zero!")

    app_logger.info("--- TableauLogger testing complete ---")