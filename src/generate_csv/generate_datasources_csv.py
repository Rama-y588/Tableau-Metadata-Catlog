import csv
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from enum import Enum

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_datasources import get_datasources

# --- Define root folder for standalone execution ---
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'

# Define module name for logging
MODULE_NAME = "generate_datasources_csv"

class CSVGenerationStatus(Enum):
    SUCCESS = "Success"
    PARTIAL = "Partial"
    FAILED = "Failed"

def generate_datasources_csv_from_config(datasources_data) -> str:
    """
    Generates a CSV file from datasource data based on configuration settings.
    Appends all datasources to the CSV file with simplified fields.
    
    Args:
        datasources_data (List[Dict[str, Any]]): List of datasource data dictionaries
        
    Returns:
        str: Status of the operation ("Success", "Partial", or "Failed")
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting datasources CSV generation process")

    config = load_YAML_config(CONFIG_FILE_PATH)
    if not config:
        logger.error(f"[{MODULE_NAME}] Failed to load configuration. Aborting datasources CSV generation.")
        return CSVGenerationStatus.FAILED.value

    file_settings = config.get('file_settings', {})
    data_folder_path_str = file_settings.get('data_folder_path')
    temp_subfolder_name = file_settings.get('temp_subfolder_name')
    datasources_csv_filename = file_settings.get('datasources_csv_filename')

    if not all([data_folder_path_str, temp_subfolder_name, datasources_csv_filename]):
        logger.error(
            f"[{MODULE_NAME}] Missing one or more required 'file_settings' keys "
            "('data_folder_path', 'temp_subfolder_name', 'datasources_csv_filename') in config."
        )
        return CSVGenerationStatus.FAILED.value

    output_directory = project_root / data_folder_path_str / temp_subfolder_name
    csv_filepath = output_directory / datasources_csv_filename

    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"[{MODULE_NAME}] Ensured output directory exists: {output_directory}")
    except OSError as e:
        logger.critical(f"[{MODULE_NAME}] Error creating output directory '{output_directory}': {e}", exc_info=True)
        return CSVGenerationStatus.FAILED.value

    if not datasources_data:
        logger.info(f"[{MODULE_NAME}] No datasources data retrieved. Datasources CSV will not be generated.")
        return CSVGenerationStatus.SUCCESS.value

    logger.info(f"[{MODULE_NAME}] Total datasources found: {len(datasources_data)}")

    # Sort datasources by 'name'
    sorted_datasources = sorted(datasources_data, key=lambda x: x.get('name', ''))

    # Meaningful headers for datasources
    headers = [
        'datasource_id',              # Unique identifier for the datasource
        'datasource_name',            # Name of the datasource
        'datasource_path',            # Full path to the datasource
        'datasource_type',            # Type of datasource (upstream/embedded)
        'datasource_has_extracts',               # Whether the datasource has extracts
        'datasource_last_extract_refresh',       # Last time the extract was refreshed
        'datasource_creation_date',              # When the datasource was created
        'datasource_last_modified_date'          # When the datasource was last modified
    ]

    # Process datasources with meaningful field names
    processed_datasources = []
    for ds in sorted_datasources:
        processed_datasources.append({
            'datasource_id': ds.get('id', "null"),
            'datasource_name': ds.get('name', "null"),
            'datasource_path': ds.get('path', "null"),
            'datasource_type': ds.get('type', "null"),
            'datasource_has_extracts': ds.get('has_extracts', "null"),
            'datasource_last_extract_refresh': ds.get('extract_last_refresh_time', "null"),
            'datasource_creation_date': ds.get('created_at', "null"),
            'datasource_last_modified_date': ds.get('updated_at', "null")
        })

    # Check if file exists to determine mode
    file_exists = csv_filepath.exists()
    mode = 'a' if file_exists else 'w'
    
    logger.info(f"[{MODULE_NAME}] Writing {len(processed_datasources)} datasources to CSV file: {csv_filepath} (mode: {mode})")
    try:
        with open(csv_filepath, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            writer.writerows(processed_datasources)
        
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{MODULE_NAME}] Successfully processed datasources in {processing_time:.2f} seconds")
        logger.info(f"[{MODULE_NAME}] Datasources CSV file updated successfully at: {csv_filepath}")
        return CSVGenerationStatus.SUCCESS.value
        
    except IOError as e:
        logger.critical(f"[{MODULE_NAME}] IOError while writing CSV file '{csv_filepath}': {e}", exc_info=True)
        return CSVGenerationStatus.FAILED.value
    except Exception as e:
        logger.critical(f"[{MODULE_NAME}] Unexpected error during CSV writing: {e}", exc_info=True)
        return CSVGenerationStatus.FAILED.value


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
    logger.info(f"[{MODULE_NAME}] CSV generation status: {status}")
    
    logger.info(f"[{MODULE_NAME}] Script execution finished.")