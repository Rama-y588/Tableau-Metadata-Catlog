from pathlib import Path
import yaml
from src.utils.logger import app_logger as logger

# --- Configuration Loading Function ---
def load_YAML_config(config_path: Path) -> dict | None:
    """
    Loads configuration settings from a YAML file.

    Args:
        config_path (Path): The Path object pointing to the YAML configuration file.

    Returns:
        dict | None: A dictionary containing the configuration settings, or None if
                     the file is not found or cannot be parsed.
    """
    logger.info(f"Attempting to load configuration from: {config_path}")
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        logger.info("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found at '{config_path}'. Please verify the path and file existence.")
        return None
    except yaml.YAMLError as e:
        logger.critical(f"Error parsing configuration file '{config_path}': {e}. Please check YAML syntax.")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading config from '{config_path}': {e}", exc_info=True)
        return None