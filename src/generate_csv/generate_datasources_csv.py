import csv
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Set, Tuple
from enum import Enum

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_datasources import get_datasources

# --- Define root folder for standalone execution ---
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
config_path = project_root / 'config' / 'csv_exporter.yaml'

# Define module name for logging
MODULE_NAME = "generate_datasources_csv"

class CSVGenerationStatus(Enum):
    SUCCESS = "Success"
    PARTIAL = "Partial"
    FAILED = "Failed"

def read_existing_datasources(csv_filepath: Path) -> Set[str]:
    """
    Read existing datasources from CSV file to avoid duplicates.
    
    Args:
        csv_filepath (Path): Path to the CSV file
        
    Returns:
        Set[str]: Set of existing datasource IDs
    """
    existing_datasources = set()
    if csv_filepath.exists():
        try:
            with open(csv_filepath, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    datasource_id = row.get('datasource_id', '').strip()
                    if datasource_id:  # Only add if ID is present
                        existing_datasources.add(datasource_id)
            logger.info(f"[{MODULE_NAME}] Found {len(existing_datasources)} existing datasources in CSV")
        except Exception as e:
            logger.error(f"[{MODULE_NAME}] Error reading existing datasources: {str(e)}")
    return existing_datasources

def generate_datasources_csv_from_config(datasources_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate CSV file from datasources data with duplicate prevention.
    
    Args:
        datasources_data (List[Dict[str, Any]]): List of datasource dictionaries
        
    Returns:
        Dict[str, Any]: Status dictionary containing:
            - status: Success/Partial/Failed
            - processed_count: Number of items processed
            - skipped_count: Number of items skipped
            - processing_time: Time taken to process
            - file_path: Path to the generated CSV
            - error_message: Error message if any
    """
    start_time = datetime.now()
    status = {
        "status": CSVGenerationStatus.FAILED.value,
        "processed_count": 0,
        "skipped_count": 0,
        "processing_time": 0,
        "file_path": None,
        "error_message": None
    }
    
    # Load configuration
    try:
        config = load_YAML_config(config_path)
        if not config:
            status["error_message"] = "Failed to load configuration"
            return status

        file_settings = config.get('csv_paths', {})  # Changed from csv_paths to file_settings
        data_folder_path_str = file_settings.get('data_folder_path')
        temp_subfolder_name = file_settings.get('temp_subfolder_name')
        datasource_csv_filename = file_settings.get('datasources_csv_filename')
        
        if not all([data_folder_path_str, temp_subfolder_name, datasource_csv_filename]):
            status["error_message"] = "Missing required file settings in config"
            return status
            
        csv_filepath = project_root / data_folder_path_str / temp_subfolder_name / datasource_csv_filename
        status["file_path"] = str(csv_filepath)
        logger.info(f"[{MODULE_NAME}] CSV file path: {csv_filepath}")
        
    except Exception as e:
        status["error_message"] = f"Error loading configuration: {str(e)}"
        logger.error(f"[{MODULE_NAME}] {status['error_message']}")
        return status

    # Define headers
    headers = [
        'datasource_id',
        'datasource_name',
        'datasource_type',
        'datasource_has_extracts',
        'datasource_last_extract_refresh',
        'datasource_creation_date',
        'datasource_last_modified_date'
    ]

    # Get existing datasources to prevent duplicates
    existing_datasources = read_existing_datasources(csv_filepath)

    # Sort datasources by ID for consistent processing
    sorted_datasources = sorted(datasources_data, key=lambda x: str(x.get('id', '')).lower())

    # Process datasources with meaningful field names
    processed_datasources = []
    seen_datasources = set()  # Track datasources we've seen in this batch

    for ds in sorted_datasources:
        try:
            datasource_id = str(ds.get('id', '')).strip()
            if not datasource_id:  # Skip if no ID
                continue

            # Skip if datasource already exists in CSV or in current batch
            if datasource_id in existing_datasources or datasource_id in seen_datasources:
                status["skipped_count"] += 1
                logger.debug(f"[{MODULE_NAME}] Skipping duplicate datasource: {datasource_id}")
                continue

            processed_datasource = {
                'datasource_id': datasource_id,
                'datasource_name': str(ds.get('name', '')).strip(),
                'datasource_type': str(ds.get('type', '')).strip(),
                'datasource_has_extracts': str(ds.get('has_extracts', '')).strip(),
                'datasource_last_extract_refresh': str(ds.get('extract_last_refresh_time', '')).strip(),
                'datasource_creation_date': str(ds.get('created_at', '')).strip(),
                'datasource_last_modified_date': str(ds.get('updated_at', '')).strip()
            }
            processed_datasources.append(processed_datasource)
            seen_datasources.add(datasource_id)
            status["processed_count"] += 1

        except Exception as e:
            logger.error(f"[{MODULE_NAME}] Error processing datasource {ds.get('id', 'unknown')}: {str(e)}")
            continue

    if not processed_datasources:
        status["status"] = CSVGenerationStatus.SUCCESS.value
        status["error_message"] = "No new datasources to write to CSV"
        status["processing_time"] = (datetime.now() - start_time).total_seconds()
        return status

    # Check if file exists to determine mode
    file_exists = csv_filepath.exists()
    mode = 'a' if file_exists else 'w'
    
    logger.info(f"[{MODULE_NAME}] Writing {len(processed_datasources)} new datasources to CSV file: {csv_filepath} (mode: {mode})")
    try:
        with open(csv_filepath, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            writer.writerows(processed_datasources)
        
        status["processing_time"] = (datetime.now() - start_time).total_seconds()
        status["status"] = CSVGenerationStatus.SUCCESS.value
        
        logger.info(f"[{MODULE_NAME}] Successfully processed datasources in {status['processing_time']:.2f} seconds")
        logger.info(f"[{MODULE_NAME}] Datasources CSV file updated successfully at: {csv_filepath}")
        logger.info(f"[{MODULE_NAME}] Skipped {status['skipped_count']} duplicate datasources")
        
    except IOError as e:
        status["error_message"] = f"IOError while writing CSV file: {str(e)}"
        logger.critical(f"[{MODULE_NAME}] {status['error_message']}", exc_info=True)
    except Exception as e:
        status["error_message"] = f"Unexpected error during CSV writing: {str(e)}"
        logger.critical(f"[{MODULE_NAME}] {status['error_message']}", exc_info=True)

    return status

if __name__ == "__main__":
    logger.info(f"[{MODULE_NAME}] Script execution started.")
    import json
    root_folder = Path(__file__).resolve().parents[2]
    
    test_data_path = root_folder / "sample_data" / "data_test.json"
    with open(test_data_path, 'r') as f:
        raw_data = json.load(f)

    # Get datasources data using get_datasources
    datasources_data = get_datasources(raw_data=raw_data)
    
    # Generate CSV and get status
    status = generate_datasources_csv_from_config(datasources_data)
    logger.info(f"[{MODULE_NAME}] CSV generation status: {status['status']}")
    logger.info(f"[{MODULE_NAME}] Processed: {status['processed_count']}")
    logger.info(f"[{MODULE_NAME}] Skipped: {status['skipped_count']}")
    if status['error_message']:
        logger.info(f"[{MODULE_NAME}] Message: {status['error_message']}")
    
    logger.info(f"[{MODULE_NAME}] Script execution finished.")