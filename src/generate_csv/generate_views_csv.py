import csv
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any
from enum import Enum

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_views import get_views

# --- Define root folder for standalone execution ---
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'

# Define module name for logging
MODULE_NAME = "generate_views_csv"

class CSVGenerationStatus(Enum):
    SUCCESS = "Success"
    PARTIAL = "Partial"
    FAILED = "Failed"

def generate_views_csv_from_config(views_data) -> str:
    """
    Generates a CSV file from view data based on configuration settings.
    Appends all views to the CSV file with simplified fields.
    
    Args:
        views_data (List[Dict[str, Any]]): List of view data dictionaries
        
    Returns:
        str: Status of the operation ("Success", "Partial", or "Failed")
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting views CSV generation process")

    config = load_YAML_config(CONFIG_FILE_PATH)
    if not config:
        logger.error(f"[{MODULE_NAME}] Failed to load configuration. Aborting views CSV generation.")
        return CSVGenerationStatus.FAILED.value

    file_settings = config.get('file_settings', {})
    data_folder_path_str = file_settings.get('data_folder_path')
    temp_subfolder_name = file_settings.get('temp_subfolder_name')
    views_csv_filename = file_settings.get('views_csv_filename')

    if not all([data_folder_path_str, temp_subfolder_name, views_csv_filename]):
        logger.error(
            f"[{MODULE_NAME}] Missing one or more required 'file_settings' keys "
            "('data_folder_path', 'temp_subfolder_name', 'views_csv_filename') in config."
        )
        return CSVGenerationStatus.FAILED.value

    output_directory = project_root / data_folder_path_str / temp_subfolder_name
    csv_filepath = output_directory / views_csv_filename

    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"[{MODULE_NAME}] Ensured output directory exists: {output_directory}")
    except OSError as e:
        logger.critical(f"[{MODULE_NAME}] Error creating output directory '{output_directory}': {e}", exc_info=True)
        return CSVGenerationStatus.FAILED.value

    if not views_data:
        logger.info(f"[{MODULE_NAME}] No views data retrieved. Views CSV will not be generated.")
        return CSVGenerationStatus.SUCCESS.value

    logger.info(f"[{MODULE_NAME}] Total views found: {len(views_data)}")

    # Sort views by 'name'
    sorted_views = sorted(views_data, key=lambda x: x.get('name', ''))

    # Simplified headers with only required fields
    headers = [
        'viewid',
        'view_name',
        'path',
        'view_type',
        'created_at',
        'updated_at'
    ]

    # Process views with only required fields
    processed_views = []
    for view in sorted_views:
        processed_views.append({
            'viewid': view.get('id', "null"),
            'view_name': view.get('name', "null"),
            'path': view.get('path', "null"),
            'view_type': view.get('type', "null"),
            'created_at': view.get('created_at', "null"),
            'updated_at': view.get('updated_at', "null")
        })

    # Check if file exists to determine mode
    file_exists = csv_filepath.exists()
    mode = 'a' if file_exists else 'w'
    
    logger.info(f"[{MODULE_NAME}] Writing {len(processed_views)} views to CSV file: {csv_filepath} (mode: {mode})")
    try:
        with open(csv_filepath, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            writer.writerows(processed_views)
        
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{MODULE_NAME}] Successfully processed views in {processing_time:.2f} seconds")
        logger.info(f"[{MODULE_NAME}] Views CSV file updated successfully at: {csv_filepath}")
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

    # Get views data using get_views
    views_data = get_views(raw_data=raw_data, site_name="tableau.example.com")
    
    # Generate CSV and get status
    status = generate_views_csv_from_config(views_data)
    logger.info(f"[{MODULE_NAME}] CSV generation status: {status}")
    
    logger.info(f"[{MODULE_NAME}] Script execution finished.")