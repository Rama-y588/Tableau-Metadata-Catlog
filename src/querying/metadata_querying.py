import json
import sys
from pathlib import Path
from typing import Optional, Dict, Any
from src.utils.logger import app_logger as logger
from src.utils.graphql.execute_graphql_query import execute_graphql_query
from src.querying.graphql_query_loader import get_query_by_name
from src.utils.helper import load_YAML_config

# Get current file information for logging
CURRENT_FILE = Path(__file__).resolve()
FILE_NAME = CURRENT_FILE.name
PROJECT_ROOT = CURRENT_FILE.parents[2]
CONFIG_FILE_PATH = PROJECT_ROOT / "config" / "csv_exporter.yaml"

def get_output_path() -> Optional[Path]:
    """
    Get the output path from configuration
    
    Returns:
        Optional[Path]: Path object for the output file, or None if config is invalid
    """
    try:
        config = load_YAML_config(CONFIG_FILE_PATH)
        if not config:
            logger.error(f"[{FILE_NAME}] Failed to load configuration from {CONFIG_FILE_PATH}")
            return None
        
        file_settings = config.get('file_settings', {})
        data_folder_path_str = file_settings.get('data_folder_path')
        temp_subfolder_name = file_settings.get('temp_subfolder_name')
        metadata_json_filename = file_settings.get('metadata_json_filename')
        
        if not all([data_folder_path_str, temp_subfolder_name, metadata_json_filename]):
            logger.error(f"[{FILE_NAME}] Missing required configuration settings")
            return None
            
        return PROJECT_ROOT / data_folder_path_str / temp_subfolder_name / metadata_json_filename
        
    except Exception as e:
        logger.error(f"[{FILE_NAME}] Error getting output path: {str(e)}")
        return None

def fetch_and_save_metadata(query: str, output_file: Path) -> bool:
    """
    Executes the provided GraphQL query and saves the result to a JSON file.

    Args:
        query (str): The GraphQL query string to execute.
        output_file (Path): The path to the output metadata.json file.

    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"[{FILE_NAME}] Executing GraphQL query...")
        response = execute_graphql_query(query)
        
        if not response:
            logger.error(f"[{FILE_NAME}] Empty response from GraphQL query")
            return False
            
        logger.info(f"[{FILE_NAME}] Query executed successfully. Saving metadata...")

        # Create parent directory if it doesn't exist
        output_file.parent.mkdir(parents=True, exist_ok=True)

        # Write response to metadata.json
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(response, f, indent=4)

        logger.info(f"[{FILE_NAME}] Metadata saved to: {output_file}")
        return True

    except FileNotFoundError as e:
        logger.error(f"[{FILE_NAME}] File not found error: {str(e)}")
        return False
    except json.JSONDecodeError as e:
        logger.error(f"[{FILE_NAME}] JSON encoding error: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"[{FILE_NAME}] Failed to fetch and save metadata: {str(e)}")
        return False

def save_metadata_from_query(query: str) -> bool:
    """
    Execute a query and save the results to metadata.json
    
    Args:
        query (str): The GraphQL query to execute
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        logger.info(f"[{FILE_NAME}] Starting metadata save process for query")
        
        # Get output path from config
        output_file = get_output_path()
        if not output_file:
            return False
        
        # Fetch and save metadata
        success = fetch_and_save_metadata(query, output_file)
        
        if success:
            logger.info(f"[{FILE_NAME}] Successfully saved metadata from query")
        else:
            logger.error(f"[{FILE_NAME}] Failed to save metadata from query")
            
        return success
        
    except Exception as e:
        logger.error(f"[{FILE_NAME}] Unexpected error in save_metadata_from_query: {str(e)}")
        return False

