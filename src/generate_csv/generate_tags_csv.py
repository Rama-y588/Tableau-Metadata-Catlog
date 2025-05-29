import csv
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Set, Tuple
from enum import Enum

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_tags import get_tags

# --- Define root folder for standalone execution ---
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'

# Define module name for logging (using file name)
MODULE_NAME = "generate_tags_csv.py"

class CSVGenerationStatus(Enum):
    SUCCESS = "Success"
    PARTIAL = "Partial"
    FAILED = "Failed"

def read_existing_tags(csv_filepath: Path) -> Set[str]:
    """
    Read existing tags from CSV file to avoid duplicates.
    
    Args:
        csv_filepath (Path): Path to the CSV file
        
    Returns:
        Set[str]: Set of existing tag names
    """
    existing_tags = set()
    if csv_filepath.exists():
        try:
            with open(csv_filepath, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    tag_name = row.get('tag_name', '').strip()
                    if tag_name:  # Only add if tag name is present
                        existing_tags.add(tag_name)
            logger.info(f"[{MODULE_NAME}] Found {len(existing_tags)} existing tags in CSV")
        except Exception as e:
            logger.error(f"[{MODULE_NAME}] Error reading existing tags: {str(e)}")
    return existing_tags

def generate_tags_csv_from_config(tags_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generates a CSV file from tag data based on configuration settings.
    
    Args:
        tags_data (List[Dict[str, Any]]): List of tag data dictionaries
        
    Returns:
        Dict[str, Any]: Status dictionary containing:
            - status: "Success", "Partial", or "Failed"
            - total_tags: Total number of tags processed
            - processed_tags: Number of tags successfully written
            - error_message: Error message if any
            - file_path: Path to the generated CSV file
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting tags CSV generation process")
    
    status = {
        "status": CSVGenerationStatus.FAILED.value,
        "total_tags": 0,
        "processed_tags": 0,
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
    file_settings = config.get('csv_paths', {})
    required_settings = ['data_folder_path', 'temp_subfolder_name', 'tags_csv_filename']
    missing_settings = [setting for setting in required_settings if not file_settings.get(setting)]
    
    if missing_settings:
        error_msg = f"Missing required 'file_settings' keys: {', '.join(missing_settings)}"
        logger.error(f"[{MODULE_NAME}] {error_msg}")
        status["error_message"] = error_msg
        return status

    # Setup file paths
    output_directory = project_root / file_settings['data_folder_path'] / file_settings['temp_subfolder_name']
    csv_filepath = output_directory / file_settings['tags_csv_filename']
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

    # Read existing tags
    existing_tags = read_existing_tags(csv_filepath)

    # Validate tags data
    if not tags_data:
        logger.info(f"[{MODULE_NAME}] No tags data retrieved. Tags CSV will not be generated.")
        status["status"] = CSVGenerationStatus.SUCCESS.value
        return status

    status["total_tags"] = len(tags_data)
    logger.info(f"[{MODULE_NAME}] Total tags found: {status['total_tags']}")

    # Sort tags by name
    sorted_tags = sorted(tags_data, key=lambda x: x.get('name', ''))

    # Define headers - only id and name
    headers = [
        'tag_id',
        'tag_name'
    ]

    # Process tags
    processed_tags = []
    skipped_tags = 0
    seen_tags = set()  # Track tags we've seen in this batch

    for tag in sorted_tags:
        try:
            tag_name = str(tag.get('name', '')).strip()

            # Skip if tag already exists in CSV or in current batch
            if tag_name in existing_tags or tag_name in seen_tags:
                skipped_tags += 1
                logger.debug(f"[{MODULE_NAME}] Skipping duplicate tag: {tag_name}")
                continue

            processed_tag = {
                'tag_id': str(tag.get('id', '')),
                'tag_name': tag_name
            }
            processed_tags.append(processed_tag)
            seen_tags.add(tag_name)

        except Exception as e:
            logger.error(f"[{MODULE_NAME}] Error processing tag {tag.get('id', 'unknown')}: {str(e)}")
            continue

    # Write to CSV
    file_exists = csv_filepath.exists()
    mode = 'a' if file_exists else 'w'
    
    logger.info(f"[{MODULE_NAME}] Writing {len(processed_tags)} new tags to CSV file: {csv_filepath} (mode: {mode})")
    try:
        with open(csv_filepath, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            writer.writerows(processed_tags)
        
        status["processed_tags"] = len(processed_tags)
        status["status"] = CSVGenerationStatus.SUCCESS.value
        
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{MODULE_NAME}] Successfully processed tags in {processing_time:.2f} seconds")
        logger.info(f"[{MODULE_NAME}] Tags CSV file updated successfully at: {csv_filepath}")
        logger.info(f"[{MODULE_NAME}] Skipped {skipped_tags} duplicate tags")
        
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

    # Get tags data using get_tags
    tags_data = get_tags(raw_data=raw_data)
    
    # Generate CSV and get status
    status = generate_tags_csv_from_config(tags_data)
    
    # Log detailed status
    logger.info(f"[{MODULE_NAME}] CSV Generation Status:")
    logger.info(f"[{MODULE_NAME}]   Status: {status['status']}")
    logger.info(f"[{MODULE_NAME}]   Total Tags: {status['total_tags']}")
    logger.info(f"[{MODULE_NAME}]   Processed Tags: {status['processed_tags']}")
    if status['error_message']:
        logger.error(f"[{MODULE_NAME}]   Error: {status['error_message']}")
    logger.info(f"[{MODULE_NAME}]   File Path: {status['file_path']}")
    
    logger.info(f"[{MODULE_NAME}] Script execution finished.")