import csv
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime
import json

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_workbooks import get_workbooks

# --- Module Constants ---
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'
MODULE_NAME = current_file.name

def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validate the configuration dictionary for required settings.
    
    Args:
        config (Dict[str, Any]): Configuration dictionary
        
    Returns:
        bool: True if valid, False otherwise
    """
    required_settings = ['data_folder_path', 'temp_subfolder_name', 'workbooks_csv_filename']
    file_settings = config.get('file_settings', {})
    
    missing_settings = [setting for setting in required_settings if not file_settings.get(setting)]
    if missing_settings:
        logger.error(f"[{MODULE_NAME}] Missing required settings: {', '.join(missing_settings)}")
        return False
    return True

def create_output_directory(directory: Path) -> bool:
    """
    Create output directory if it doesn't exist.
    
    Args:
        directory (Path): Directory path to create
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        os.makedirs(directory, exist_ok=True)
        logger.info(f"[{MODULE_NAME}] Created/verified output directory: {directory}")
        return True
    except OSError as e:
        logger.critical(f"[{MODULE_NAME}] Failed to create output directory '{directory}': {str(e)}")
        return False

def process_workbook_data(workbook: Dict[str, Any]) -> Dict[str, str]:
    """
    Process and validate workbook data for CSV output.
    
    Args:
        workbook (Dict[str, Any]): Raw workbook data
        
    Returns:
        Dict[str, str]: Processed workbook data
    """
    return {
        'workbook_id': str(workbook.get('id', '')),
        'workbook_name': str(workbook.get('name', '')),
        'project_id': str(workbook.get('project_id', '')),
        'project_name': str(workbook.get('project_name', '')),
        'owner_id': str(workbook.get('owner_id', '')),
        'description': str(workbook.get('description', '')),
        'created_at': str(workbook.get('created_at', '')),
        'updated_at': str(workbook.get('updated_at', '')),
        'url': str(workbook.get('url', ''))
    }

def generate_workbooks_csv_from_config(workbooks_data: List[Dict[str, Any]]) -> bool:
    """
    Generates a CSV file from workbook data based on configuration settings.
    
    Args:
        workbooks_data (List[Dict[str, Any]]): List of workbook data dictionaries
        
    Returns:
        bool: True if successful, False otherwise
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting workbooks CSV generation process")

    # Load and validate configuration
    config = load_YAML_config(CONFIG_FILE_PATH)
    if not config or not validate_config(config):
        logger.error(f"[{MODULE_NAME}] Invalid configuration. Aborting CSV generation.")
        return False

    file_settings = config['file_settings']
    output_directory = project_root / file_settings['data_folder_path'] / file_settings['temp_subfolder_name']
    csv_filepath = output_directory / file_settings['workbooks_csv_filename']

    # Create output directory
    if not create_output_directory(output_directory):
        return False

    # Validate input data
    if not workbooks_data:
        logger.warning(f"[{MODULE_NAME}] No workbooks data provided. CSV will not be generated.")
        return False

    logger.info(f"[{MODULE_NAME}] Processing {len(workbooks_data)} workbooks")

    try:
        # Sort workbooks by name
        sorted_workbooks = sorted(workbooks_data, key=lambda x: x.get('name', '').lower())
        
        # Define CSV headers
        headers = [
            'workbook_id', 'workbook_name', 'project_id', 'project_name',
            'owner_id', 'description', 'created_at', 'updated_at', 'url'
        ]

        # Process workbooks
        processed_workbooks = [process_workbook_data(wb) for wb in sorted_workbooks]

        # Write to CSV
        logger.info(f"[{MODULE_NAME}] Writing CSV to: {csv_filepath}")
        with open(csv_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(processed_workbooks)

        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{MODULE_NAME}] Successfully generated CSV with {len(processed_workbooks)} workbooks in {processing_time:.2f} seconds")
        return True

    except IOError as e:
        logger.critical(f"[{MODULE_NAME}] IOError while writing CSV: {str(e)}")
        return False
    except Exception as e:
        logger.critical(f"[{MODULE_NAME}] Unexpected error during CSV generation: {str(e)}")
        return False

def main() -> None:
    """Main function for testing the module."""
    logger.info(f"[{MODULE_NAME}] Starting script execution")
    
    try:
        test_data_path = project_root / "sample_data" / "data_test.json"
        logger.info(f"[{MODULE_NAME}] Loading test data from: {test_data_path}")
        
        with open(test_data_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        workbooks_data = get_workbooks(raw_data=raw_data)
        success = generate_workbooks_csv_from_config(workbooks_data)
        
        if success:
            logger.info(f"[{MODULE_NAME}] Script completed successfully")
        else:
            logger.error(f"[{MODULE_NAME}] Script completed with errors")
            
    except FileNotFoundError:
        logger.error(f"[{MODULE_NAME}] Test data file not found: {test_data_path}")
    except json.JSONDecodeError:
        logger.error(f"[{MODULE_NAME}] Invalid JSON in test data file")
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Unexpected error during script execution: {str(e)}")

if __name__ == "__main__":
    main()