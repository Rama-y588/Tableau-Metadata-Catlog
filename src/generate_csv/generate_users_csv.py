import csv
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import json

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_users import get_users

# --- Module Constants ---
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'
MODULE_NAME = current_file.name

# Define CSV headers as a constant
CSV_HEADERS = [
    'user_id', 'user_name', 'user_username', 'user_email'
]

def validate_config(config: Dict[str, Any]) -> bool:
    """
    Validate the configuration dictionary for required settings.
    
    Args:
        config (Dict[str, Any]): Configuration dictionary
        
    Returns:
        bool: True if valid, False otherwise
    """
    required_settings = ['data_folder_path', 'temp_subfolder_name', 'users_csv_filename']
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

def process_user_data(user: Dict[str, Any]) -> Dict[str, str]:
    """
    Process and validate user data for CSV output.
    
    Args:
        user (Dict[str, Any]): Raw user data
        
    Returns:
        Dict[str, str]: Processed user data
    """
    return {
        'user_id': str(user.get('id', '')),
        'user_name': str(user.get('name', '')),
        'user_username': str(user.get('username', '')),
        'user_email': str(user.get('email', ''))
    }


def write_users_to_csv(csv_filepath: Path, users_data: List[Dict[str, str]], is_new_file: bool = False) -> bool:
    """
    Write multiple user records to the CSV file.
    
    Args:
        csv_filepath (Path): Path to the CSV file
        users_data (List[Dict[str, str]]): List of user data to write
        is_new_file (bool): Whether this is a new file (needs headers)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        mode = 'w' if is_new_file else 'a'
        logger.info(f"[{MODULE_NAME}] Writing users to CSV file: {csv_filepath}")
        logger.info(f"[{MODULE_NAME}] File mode: {'Create new file' if is_new_file else 'Append to existing file'}")
        
        with open(csv_filepath, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=CSV_HEADERS)
            if is_new_file:
                logger.info(f"[{MODULE_NAME}] Writing headers: {CSV_HEADERS}")
                writer.writeheader()
            
            logger.info(f"[{MODULE_NAME}] Writing {len(users_data)} user records")
            writer.writerows(users_data)
            
            # Log sample of data being written
            if users_data:
                sample_user = users_data[0]
                logger.info(f"[{MODULE_NAME}] Sample user data being written: {sample_user}")
        
        logger.info(f"[{MODULE_NAME}] Successfully wrote data to CSV file")
        return True
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Error writing to CSV: {str(e)}")
        return False

def generate_users_csv_from_config(users_data: List[Dict[str, Any]]) -> str:
    """
    Generates a CSV file from user data based on configuration settings.
    If the file exists, it will append new records. If not, it will create a new file.
    
    Args:
        users_data (List[Dict[str, Any]]): List of user data dictionaries
        
    Returns:
        str: Status of the operation ("Success", "Partial", or "Failed")
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting users CSV generation process")

    # Load and validate configuration
    config = load_YAML_config(CONFIG_FILE_PATH)
    if not config or not validate_config(config):
        logger.error(f"[{MODULE_NAME}] Invalid configuration. Aborting CSV generation.")
        return "Failed"

    file_settings = config['file_settings']
    output_directory = project_root / file_settings['data_folder_path'] / file_settings['temp_subfolder_name']
    csv_filepath = output_directory / file_settings['users_csv_filename']

    logger.info(f"[{MODULE_NAME}] Output directory: {output_directory}")
    logger.info(f"[{MODULE_NAME}] CSV file path: {csv_filepath}")

    # Create output directory
    if not create_output_directory(output_directory):
        return "Failed"

    # Validate input data
    if not users_data:
        logger.warning(f"[{MODULE_NAME}] No users data provided. CSV will not be generated.")
        return "Success"

    logger.info(f"[{MODULE_NAME}] Processing {len(users_data)} users")

    try:
        # Sort users by name
        sorted_users = sorted(users_data, key=lambda x: x.get('name', '').lower())
        logger.info(f"[{MODULE_NAME}] Sorted users by name")
        
        # Process all users at once
        processed_users = [process_user_data(user) for user in sorted_users]
        logger.info(f"[{MODULE_NAME}] Processed {len(processed_users)} users")
        
        # Check if file exists
        file_exists = csv_filepath.exists()
        logger.info(f"[{MODULE_NAME}] CSV file exists: {file_exists}")
        
        # Write all users to CSV
        if write_users_to_csv(csv_filepath, processed_users, not file_exists):
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"[{MODULE_NAME}] Successfully processed {len(processed_users)} users in {processing_time:.2f} seconds")
            logger.info(f"[{MODULE_NAME}] CSV file generated successfully at: {csv_filepath}")
            return "Success"
        else:
            logger.error(f"[{MODULE_NAME}] Failed to write users to CSV file: {csv_filepath}")
            return "Failed"

    except Exception as e:
        logger.critical(f"[{MODULE_NAME}] Unexpected error during CSV generation: {str(e)}")
        return "Failed"

def main() -> None:
    """Main function for testing the module."""
    logger.info(f"[{MODULE_NAME}] Starting script execution")
    
    try:
        # Load sample data
        sample_data_path = project_root / 'sample_data' / 'data_test.json'
        logger.info(f"[{MODULE_NAME}] Loading test data from: {sample_data_path}")
        
        with open(sample_data_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        # Get users from raw data
        users_data = get_users(raw_data=raw_data)
        
        if not users_data:
            logger.error(f"[{MODULE_NAME}] No users data found in the sample data")
            return
            
        # Generate CSV
        status = generate_users_csv_from_config(users_data)
        
        # Log final status
        if status == "Success":
            logger.info(f"[{MODULE_NAME}] Script completed successfully - CSV file generated")
        elif status == "Partial":
            logger.warning(f"[{MODULE_NAME}] Script completed with partial success - Some users may not have been processed")
        else:
            logger.error(f"[{MODULE_NAME}] Script completed with errors - CSV generation failed")
            
    except FileNotFoundError:
        logger.error(f"[{MODULE_NAME}] Test data file not found: {sample_data_path}")
    except json.JSONDecodeError:
        logger.error(f"[{MODULE_NAME}] Invalid JSON in test data file")
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Unexpected error during script execution: {str(e)}")

if __name__ == "__main__":
    main()