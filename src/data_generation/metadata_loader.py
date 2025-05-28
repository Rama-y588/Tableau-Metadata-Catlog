import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config

# Define paths
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
config_file_path = project_root / 'config' / 'csv_exporter.yaml'
# Define module name for logging
MODULE_NAME = current_file.name

def load_metadata() -> Optional[Dict[str, Any]]:
    """
    Loads the metadata from the JSON file and passes it to transform_all.
    
    Returns:
        Optional[Dict[str, Any]]: The loaded metadata if successful, None if failed
    """
    logger.info(f"[{MODULE_NAME}] Starting metadata loading process")
    
    try:
        # Load config
        logger.debug(f"[{MODULE_NAME}] Loading configuration from: {config_file_path}")
        config = load_YAML_config(config_file_path)
        
        if not config:
            logger.error(f"[{MODULE_NAME}] Failed to load configuration file")
            return None
            
        file_settings = config.get('file_settings', {})
        if not file_settings:
            logger.error(f"[{MODULE_NAME}] No file settings found in configuration")
            return None
        
        # Get metadata file path from config
        data_folder_path = file_settings.get('data_folder_path')
        metadata_folder_name = file_settings.get('metadata_folder_name')
        metadata_json_filename = file_settings.get('metadata_json_filename')
        
        # Validate required config values
        missing_settings = []
        if not data_folder_path:
            missing_settings.append('data_folder_path')
        if not metadata_folder_name:
            missing_settings.append('metadata_folder_name')
        if not metadata_json_filename:
            missing_settings.append('metadata_json_filename')
            
        if missing_settings:
            logger.error(f"[{MODULE_NAME}] Missing required configuration settings: {', '.join(missing_settings)}")
            return None
        
        # Construct metadata file path
        metadata_file_path = project_root / data_folder_path / metadata_folder_name / metadata_json_filename
        logger.debug(f"[{MODULE_NAME}] Constructed metadata file path: {metadata_file_path}")
        
        # Check if metadata file exists
        if not metadata_file_path.exists():
            logger.error(f"[{MODULE_NAME}] Metadata file not found at: {metadata_file_path}")
            return None
        
        # Load metadata
        logger.info(f"[{MODULE_NAME}] Loading metadata from: {metadata_file_path}")
        try:
            with open(metadata_file_path, 'r', encoding='utf-8') as f:
                metadata = json.load(f)
        except json.JSONDecodeError as e:
            logger.error(f"[{MODULE_NAME}] Invalid JSON format in metadata file: {str(e)}")
            return None
        except UnicodeDecodeError as e:
            logger.error(f"[{MODULE_NAME}] Encoding error while reading metadata file: {str(e)}")
            return None
        
        # Validate metadata structure
        if not isinstance(metadata, dict):
            logger.error(f"[{MODULE_NAME}] Invalid metadata format: expected dictionary, got {type(metadata)}")
            return None
            
        logger.info(f"[{MODULE_NAME}] Successfully loaded metadata with {len(metadata)} top-level keys")
        return metadata
        
    except FileNotFoundError as e:
        logger.error(f"[{MODULE_NAME}] File not found: {str(e)}")
        return None
    except PermissionError as e:
        logger.error(f"[{MODULE_NAME}] Permission denied: {str(e)}")
        return None
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Unexpected error during metadata loading: {str(e)}", exc_info=True)
        return None

# --- Testing ---
if __name__ == "__main__":
    logger.info(f"[{MODULE_NAME}] Starting metadata loader test")
    
    try:
        metadata = load_metadata()
        
        if metadata:
            logger.info(f"[{MODULE_NAME}] Successfully loaded metadata")
            logger.debug(f"[{MODULE_NAME}] Metadata keys: {list(metadata.keys())}")
        else:
            logger.error(f"[{MODULE_NAME}] Failed to load metadata")
            
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Error during test execution: {str(e)}", exc_info=True)
    finally:
        logger.info(f"[{MODULE_NAME}] Metadata loader test completed")