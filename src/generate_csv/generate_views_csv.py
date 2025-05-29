import csv
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from enum import Enum

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_views import get_views
from src.Auth import server_connection

# --- Define root folder for standalone execution ---
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'

# Define module name for logging (using file name)
MODULE_NAME = "generate_views_csv.py"

class CSVGenerationStatus(Enum):
    SUCCESS = "Success"
    PARTIAL = "Partial"
    FAILED = "Failed"

def construct_view_url(view_path: str) -> str:
    """
    Constructs a Tableau view URL using the view path.
    
    Args:
        view_path (str): Path of the view
        
    Returns:
        str: Constructed view URL
    """
    try:
        profile_name = server_connection._default_profile_name
        if not profile_name:
            logger.error(f"[{MODULE_NAME}] No default profile name found")
            return ""

        server_config = server_connection._profiles.get(profile_name)
        if not server_config:
            logger.error(f"[{MODULE_NAME}] No server configuration found for profile: {profile_name}")
            return ""

        base_url = server_config.get('url', '').strip()
        site_name = server_config.get('site_name', '').strip()
        
        if not base_url or not site_name:
            logger.error(f"[{MODULE_NAME}] Missing required server configuration: url or site_name")
            return ""
            
        # Remove any leading/trailing slashes from view_path
        view_path = view_path.strip('/')
        
        # Construct the URL properly
        view_url = f"https://{base_url}/#/site/{site_name}/views/{view_path}"
        
        logger.debug(f"[{MODULE_NAME}] Constructed view URL: {view_url}")
        return view_url
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Error constructing view URL: {str(e)}", exc_info=True)
        return ""

def format_datetime(dt: Optional[datetime]) -> str:
    """
    Formats datetime object to ISO format string.
    
    Args:
        dt (Optional[datetime]): Datetime object to format
        
    Returns:
        str: Formatted datetime string or empty string if None
    """
    if isinstance(dt, datetime):
        return dt.isoformat()
    return ""

def generate_views_csv_from_config(views_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generates a CSV file from view data based on configuration settings.
    
    Args:
        views_data (List[Dict[str, Any]]): List of view data dictionaries
        
    Returns:
        Dict[str, Any]: Status dictionary containing:
            - status: "Success", "Partial", or "Failed"
            - total_views: Total number of views processed
            - processed_views: Number of views successfully written
            - error_message: Error message if any
            - file_path: Path to the generated CSV file
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting views CSV generation process")
    
    status = {
        "status": CSVGenerationStatus.SUCCESS.value,
        "total_views": 0,
        "processed_views": 0,
        "error_message": None,
        "file_path": None
    }

    # Load configuration
    config = load_YAML_config(CONFIG_FILE_PATH)
    if not config:
        error_msg = "Failed to load configuration"
        logger.error(f"[{MODULE_NAME}] {error_msg}")
        status["error_message"] = error_msg
        return status

    # Validate file settings
    file_settings = config.get('file_settings', {})
    required_settings = ['data_folder_path', 'temp_subfolder_name', 'views_csv_filename']
    missing_settings = [setting for setting in required_settings if not file_settings.get(setting)]
    
    if missing_settings:
        error_msg = f"Missing required 'file_settings' keys: {', '.join(missing_settings)}"
        logger.error(f"[{MODULE_NAME}] {error_msg}")
        status["error_message"] = error_msg
        return status

    # Setup file paths
    output_directory = project_root / file_settings['data_folder_path'] / file_settings['temp_subfolder_name']
    csv_filepath = output_directory / file_settings['views_csv_filename']
    status["file_path"] = str(csv_filepath)

    # Create output directory
    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"[{MODULE_NAME}] Ensured output directory exists: {output_directory}")
    except OSError as e:
        error_msg = f"Error creating output directory: {str(e)}"
        logger.critical(f"[{MODULE_NAME}] {error_msg}", exc_info=True)
        status["error_message"] = error_msg
        return status

    # Validate views data
    if not views_data:
        logger.info(f"[{MODULE_NAME}] No views data retrieved. Views CSV will not be generated.")
        status["status"] = CSVGenerationStatus.SUCCESS.value
        return status

    status["total_views"] = len(views_data)
    logger.info(f"[{MODULE_NAME}] Total views found: {status['total_views']}")

    # Sort views by name
    sorted_views = sorted(views_data, key=lambda x: x.get('name', ''))

    # Define headers
    headers = [
        'view_id',
        'view_name',
        'view_type',
        'view_created_at',
        'view_updated_at',
        'view_url'
    ]

    # Process views
    processed_views = []
    for view in sorted_views:
        try:
            processed_view = {
                'view_id': str(view.get('id', '')),
                'view_name': str(view.get('name', '')),
                'view_type': str(view.get('type', '')),
                'view_created_at': format_datetime(view.get('created_at')),
                'view_updated_at': format_datetime(view.get('updated_at')),
                'view_url': construct_view_url(view.get('path', ''))
            }
            processed_views.append(processed_view)
        except Exception as e:
            logger.error(f"[{MODULE_NAME}] Error processing view {view.get('id', 'unknown')}: {str(e)}")
            continue

    # Write to CSV
    file_exists = csv_filepath.exists()
    mode = 'a' if file_exists else 'w'
    
    logger.info(f"[{MODULE_NAME}] Writing {len(processed_views)} views to CSV file: {csv_filepath} (mode: {mode})")
    try:
        with open(csv_filepath, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            writer.writerows(processed_views)
        
        status["processed_views"] = len(processed_views)
        status["status"] = CSVGenerationStatus.SUCCESS.value
        
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{MODULE_NAME}] Successfully processed views in {processing_time:.2f} seconds")
        logger.info(f"[{MODULE_NAME}] Views CSV file updated successfully at: {csv_filepath}")
        
    except IOError as e:
        error_msg = f"IOError while writing CSV file: {str(e)}"
        logger.critical(f"[{MODULE_NAME}] {error_msg}", exc_info=True)
        status["error_message"] = error_msg
        status["status"] = CSVGenerationStatus.FAILED.value
    except Exception as e:
        error_msg = f"Unexpected error during CSV writing: {str(e)}"
        logger.critical(f"[{MODULE_NAME}] {error_msg}", exc_info=True)
        status["error_message"] = error_msg
        status["status"] = CSVGenerationStatus.FAILED.value

    return status

if __name__ == "__main__":
    logger.info(f"[{MODULE_NAME}] Script execution started.")
    import json
    root_folder = Path(__file__).resolve().parents[2]
    
    test_data_path = root_folder / "sample_data" / "data_test.json"
    with open(test_data_path, 'r') as f:
        raw_data = json.load(f)

    # Get views data using get_views
    views_data = get_views(raw_data=raw_data)
    
    # Generate CSV and get status
    status = generate_views_csv_from_config(views_data)
    
    # Log detailed status
    logger.info(f"[{MODULE_NAME}] CSV Generation Status:")
    logger.info(f"[{MODULE_NAME}]   Status: {status['status']}")
    logger.info(f"[{MODULE_NAME}]   Total Views: {status['total_views']}")
    logger.info(f"[{MODULE_NAME}]   Processed Views: {status['processed_views']}")
    if status['error_message']:
        logger.error(f"[{MODULE_NAME}]   Error: {status['error_message']}")
    logger.info(f"[{MODULE_NAME}]   File Path: {status['file_path']}")
    
    logger.info(f"[{MODULE_NAME}] Script execution finished.")