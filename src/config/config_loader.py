import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from src.utils.logger import app_logger as logger


def load_csv_exporter_config(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Load configuration from csv_exporter_config.yaml file.
    
    Args:
        config_path (Optional[Path]): Path to the config file. If None, uses default path.
        
    Returns:
        Dict[str, Any]: Configuration dictionary
    """
    try:
        # If no path provided, use default path
        if config_path is None:
            current_file = Path(__file__).resolve()
            config_path = current_file.parent / "csv_exporter_config.yaml"
        
        if not config_path.exists():
            logger.error(f"Config file not found: {config_path}")
            return {}
            
        with open(config_path, 'r', encoding='utf-8') as yamlfile:
            config = yaml.safe_load(yamlfile)
            
        logger.info(f"Successfully loaded configuration from {config_path}")
        return config
        
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        return {}


def get_project_file_path(config_path: Optional[Path] = None) -> Optional[Path]:
    """
    Get the project file path from the configuration.
    
    Args:
        config_path (Optional[Path]): Path to the config file. If None, uses default path.
        
    Returns:
        Optional[Path]: Path to the project file, or None if not found
    """
    config = load_csv_exporter_config(config_path)
    
    # Get the project file path from config
    project_path = config.get('project_file_path')
    if not project_path:
        logger.error("Project file path not found in configuration")
        return None
        
    # Convert to Path object
    try:
        project_path = Path(project_path)
        if not project_path.exists():
            logger.error(f"Project file not found at: {project_path}")
            return None
            
        logger.info(f"Found project file at: {project_path}")
        return project_path
        
    except Exception as e:
        logger.error(f"Invalid project file path: {str(e)}")
        return None


if __name__ == "__main__":
    # Test loading configuration
    config = load_csv_exporter_config()
    if config:
        logger.info("Configuration loaded successfully:")
        for key, value in config.items():
            logger.info(f"{key}: {value}")
            
    # Test getting project file path
    project_path = get_project_file_path()
    if project_path:
        logger.info(f"Project file path: {project_path}") 