import csv
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
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

# Define CSV headers as a constant
CSV_HEADERS = [
    'workbook_id', 'workbook_name', 'workbook_project_id',
    'workbook_owner_id', 'workbook_description', 'workbook_created_at', 
    'workbook_updated_at', 'workbook_url'
]

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
        'workbook_project_id': str(workbook.get('project_id', '')),
        'workbook_owner_id': str(workbook.get('owner_id', '')),
        'workbook_description': str(workbook.get('description', '')),
        'workbook_created_at': str(workbook.get('created_at', '')),
        'workbook_updated_at': str(workbook.get('updated_at', '')),
        'workbook_url': str(workbook.get('url', ''))
    }

def write_workbook_to_csv(csv_filepath: Path, workbook_data: Dict[str, str], is_new_file: bool = False) -> bool:
    """
    Write a single workbook record to the CSV file.
    
    Args:
        csv_filepath (Path): Path to the CSV file
        workbook_data (Dict[str, str]): Workbook data to write
        is_new_file (bool): Whether this is a new file (needs headers)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        mode = 'w' if is_new_file else 'a'
        with open(csv_filepath, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
            if is_new_file:
                writer.writeheader()
            writer.writerow(workbook_data)
        return True
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Error writing to CSV: {str(e)}")
        return False

def generate_workbooks_csv_from_config(workbooks_data: List[Dict[str, Any]]) -> str:
    """
    Generates a CSV file from workbook data based on configuration settings.
    If the file exists, it will append new records. If not, it will create a new file.
    
    Args:
        workbooks_data (List[Dict[str, Any]]): List of workbook data dictionaries
        
    Returns:
        str: Status of the operation ("Success" or "Failed")
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting workbooks CSV generation process")

    # Load and validate configuration
    config = load_YAML_config(CONFIG_FILE_PATH)
    if not config or not validate_config(config):
        logger.error(f"[{MODULE_NAME}] Invalid configuration. Aborting CSV generation.")
        return "Failed"

    file_settings = config['file_settings']
    output_directory = project_root / file_settings['data_folder_path'] / file_settings['temp_subfolder_name']
    csv_filepath = output_directory / file_settings['workbooks_csv_filename']

    # Create output directory
    if not create_output_directory(output_directory):
        return "Failed"

    # Validate input data
    if not workbooks_data:
        logger.warning(f"[{MODULE_NAME}] No workbooks data provided. CSV will not be generated.")
        return "Success"

    logger.info(f"[{MODULE_NAME}] Processing {len(workbooks_data)} workbooks")

    try:
        # Sort workbooks by name
        sorted_workbooks = sorted(workbooks_data, key=lambda x: x.get('name', '').lower())
        
        # Check if file exists
        file_exists = csv_filepath.exists()
        
        # Process and write workbooks one by one
        successful_writes = 0
        for workbook in sorted_workbooks:
            processed_data = process_workbook_data(workbook)
            if write_workbook_to_csv(csv_filepath, processed_data, not file_exists):
                successful_writes += 1
                file_exists = True  # After first write, we're appending
            else:
                logger.error(f"[{MODULE_NAME}] Failed to write workbook: {workbook.get('name', 'Unknown')}")

        processing_time = (datetime.now() - start_time).total_seconds()
        
        if successful_writes == len(sorted_workbooks):
            logger.info(f"[{MODULE_NAME}] Successfully processed all {successful_writes} workbooks in {processing_time:.2f} seconds")
            return "Success"
        else:
            logger.warning(f"[{MODULE_NAME}] Partially successful: {successful_writes}/{len(sorted_workbooks)} workbooks processed")
            return "Partial"

    except Exception as e:
        logger.critical(f"[{MODULE_NAME}] Unexpected error during CSV generation: {str(e)}")
        return "Failed"

def main() -> None:
    """Main function for testing the module."""
    logger.info(f"[{MODULE_NAME}] Starting script execution")
    
    try:
        test_data_path = project_root / "sample_data" / "data_test.json"
        logger.info(f"[{MODULE_NAME}] Loading test data from: {test_data_path}")
        
        with open(test_data_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        workbooks_data = get_workbooks(raw_data=raw_data)
        status = generate_workbooks_csv_from_config(workbooks_data)
        
        if status == "Success":
            logger.info(f"[{MODULE_NAME}] Script completed successfully")
        elif status == "Partial":
            logger.warning(f"[{MODULE_NAME}] Script completed with partial success")
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