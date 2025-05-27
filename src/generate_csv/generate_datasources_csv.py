import csv
import os
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_datasources import get_datasources

current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'


def format_timestamp(timestamp: Optional[Any]) -> str:
    """
    Formats timestamp to ISO format string or returns 'null'.
    
    Args:
        timestamp: Timestamp value to format
    
    Returns:
        str: Formatted timestamp or 'null'
    """
    if not timestamp:
        return "null"
    try:
        if isinstance(timestamp, str):
            return timestamp
        if isinstance(timestamp, datetime):
            return timestamp.isoformat()
        return str(timestamp)
    except Exception:
        return "null"


def generate_datasources_csv_from_config(datasources_data: List[Dict[str, Any]]) -> None:
    """
    Generates a CSV file from datasource data based on configuration settings.
    
    Args:
        datasources_data (List[Dict[str, Any]]): List of datasource data dictionaries
    """
    logger.info("Starting datasources CSV generation process.")

    # Load and validate configuration
    config = load_YAML_config(CONFIG_FILE_PATH)
    if not config:
        logger.error("Failed to load configuration. Aborting datasources CSV generation.")
        return

    file_settings = config.get('file_settings', {})
    data_folder_path_str = file_settings.get('data_folder_path')
    temp_subfolder_name = file_settings.get('temp_subfolder_name')
    datasources_csv_filename = file_settings.get('datasources_csv_filename')

    if not all([data_folder_path_str, temp_subfolder_name, datasources_csv_filename]):
        logger.error(
            "Missing one or more required 'file_settings' keys "
            "('data_folder_path', 'temp_subfolder_name', 'datasources_csv_filename') in config."
        )
        return

    # Setup output directory
    output_directory = project_root / data_folder_path_str / temp_subfolder_name
    csv_filepath = output_directory / datasources_csv_filename

    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Ensured output directory exists: {output_directory}")
    except OSError as e:
        logger.critical(f"Error creating output directory '{output_directory}': {e}", exc_info=True)
        return

    # Validate input data
    if not datasources_data:
        logger.info("No datasources data retrieved. Datasources CSV will not be generated.")
        return

    logger.info(f"Total datasources found: {len(datasources_data)}")

    # Sort datasources by name
    sorted_datasources = sorted(datasources_data, key=lambda x: x.get('name', ''))

    # Log sample of sorted datasources
    logger.debug("Sample of sorted datasource details:")
    for item in sorted_datasources[:5]:  # Log first 5 items
        logger.debug(
            f"ID: {item.get('id', 'N/A')}, "
            f"Name: {item.get('name', 'N/A')}"
        )

    # Define CSV headers with meaningful names
    headers = [
        'Datasource ID',
        'Datasource Name',
        'Resource URI',
        'Has Extracts',
        'Last Extract Refresh Time',
        'Datasource Type',
        'Creation Date',
        'Last Modified Date'
    ]

    # Process datasources
    processed_datasources = []
    for datasource in sorted_datasources:
        processed_datasource = {
            'Datasource ID': datasource.get('id', "null"),
            'Datasource Name': datasource.get('name', "null"),
            'Resource URI': datasource.get('uri', "null"),
            'Has Extracts': str(datasource.get('has_extracts', False)).lower(),
            'Last Extract Refresh Time': format_timestamp(datasource.get('extract_last_refresh_time')),
            'Datasource Type': datasource.get('type', "null"),
            'Creation Date': format_timestamp(datasource.get('created_at')),
            'Last Modified Date': format_timestamp(datasource.get('updated_at'))
        }
        processed_datasources.append(processed_datasource)

    # Write to CSV
    logger.info(f"Writing datasources CSV to: {csv_filepath}")
    try:
        with open(csv_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(processed_datasources)
        logger.info(f"Datasources CSV file '{datasources_csv_filename}' generated successfully at {csv_filepath}")
    except IOError as e:
        logger.critical(f"IOError while writing CSV file '{csv_filepath}': {e}", exc_info=True)
    except Exception as e:
        logger.critical(f"Unexpected error during CSV writing: {e}", exc_info=True)


if __name__ == "__main__":
    logger.info("Script execution started.")
    import json
    root_folder = Path(__file__).resolve().parents[2]
    
    test_data_path = root_folder / "sample_data" / "data_test.json"
    with open(test_data_path, 'r') as f:
        raw_data = json.load(f)

    raw_data = get_datasources(raw_data=raw_data)
    generate_datasources_csv_from_config(raw_data)
    logger.info("Script execution finished.")
