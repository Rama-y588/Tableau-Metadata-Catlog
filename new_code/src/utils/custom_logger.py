import json
import logging
import os
from logging.handlers import RotatingFileHandler
from pathlib import Path

class CustomLogger:
    _instance = None
    
    def __new__(cls, config_path: str = None):
        """
        Create a singleton instance of CustomLogger
        
        Args:
            config_path (str): Path to the logger configuration JSON file
        """
        if cls._instance is None:
            cls._instance = super(CustomLogger, cls).__new__(cls)
            cls._instance._initialized = False
        return cls._instance
    
    def __init__(self, config_path: str = None):
        """
        Initialize the custom logger with configuration from JSON file
        
        Args:
            config_path (str): Path to the logger configuration JSON file
        """
        if self._initialized:
            return
            
        if config_path is None:
            # Default path relative to this file
            config_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 
                                     'config', 'logger_comfig.json')
        
        self.config = self._load_config(config_path)
        self.logger = self._setup_logger()
        self._logging_enabled = self.config.get('logging_enabled', True)
        self._initialized = True
        
        if self.config['log_file_path'] is None:
            # Take log_dir from environment variable (or default to 'logs')
            log_dir = os.environ.get('LOG_DIR', 'logs')
            self.log_file = Path(__file__).parent.parent / log_dir / 'app.log'
        else:
            self.log_file = Path(self.config['log_file_path'])
        self.log_file.parent.mkdir(parents=True, exist_ok=True)
    
    def _load_config(self, config_path: str) -> dict:
        """Load logger configuration from JSON file"""
        try:
            with open(config_path, 'r') as f:
                return json.load(f)['logger']
        except Exception as e:
            raise Exception(f"Failed to load logger configuration: {str(e)}")
    
    def _setup_logger(self) -> logging.Logger:
        """Setup and configure the logger with file and console handlers"""
        # Create logger
        logger = logging.getLogger(self.config['logger_name'])
        logger.setLevel(self.config['log_level'])
        
        # Create formatter
        formatter = logging.Formatter(self.config['log_format'])
        
        # Setup file handler
        log_dir = os.path.dirname(self.config['log_file_path'])
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
            
        file_handler = RotatingFileHandler(
            self.config['log_file_path'],
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5
        )
        file_handler.setLevel(self.config['file_log_level'])
        file_handler.setFormatter(formatter)
        
        # Setup console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(self.config['console_log_level'])
        console_handler.setFormatter(formatter)
        
        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        
        return logger

    def enable_logging(self):
        """Enable logging"""
        self._logging_enabled = True
        self.logger.info("Logging has been enabled")
    
    def disable_logging(self):
        """Disable logging"""
        self._logging_enabled = False
        # We can't log this message since logging is disabled
        print("Logging has been disabled")
    
    def is_logging_enabled(self) -> bool:
        """Check if logging is currently enabled"""
        return self._logging_enabled
    
    def _should_log(self) -> bool:
        """Internal method to check if logging should proceed"""
        return self._logging_enabled
    
    def debug(self, message: str):
        """Log debug message if logging is enabled"""
        if self._should_log():
            self.logger.debug(message)
    
    def info(self, message: str):
        """Log info message if logging is enabled"""
        if self._should_log():
            self.logger.info(message)
    
    def warning(self, message: str):
        """Log warning message if logging is enabled"""
        if self._should_log():
            self.logger.warning(message)
    
    def error(self, message: str):
        """Log error message if logging is enabled"""
        if self._should_log():
            self.logger.error(message)
    
    def critical(self, message: str):
        """Log critical message if logging is enabled"""
        if self._should_log():
            self.logger.critical(message)

# Create a singleton instance that can be imported
app_logger = CustomLogger()

# Example usage
if __name__ == "__main__":
    # Test the singleton logger
    print("\nTesting singleton logger:")
    app_logger.debug("This is a debug message")
    app_logger.info("This is an info message")
    
    # Create another instance (will be the same instance)
    another_logger = CustomLogger()
    print(f"\nAre both loggers the same instance? {app_logger is another_logger}")
    
    # Test logging control
    app_logger.disable_logging()
    app_logger.info("This won't be logged")
    
    app_logger.enable_logging()
    app_logger.warning("This will be logged again") 