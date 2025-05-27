import csv
import os
from pathlib import Path
from typing import List, Dict, Any

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_datasources_connections import get_datasource_connections

current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'


def generate_datasource_connections_csv_from_config(datasource_connections: List[Dict[str, Any]]) -> None:
    """
    Generates a CSV file showing the relationship between datasources and their connections.
    Only includes datasource ID and connection ID.
    
    Args:
        datasource_connections (List[Dict[str, Any]]): List of datasource-connection relationships
    """
    logger.info("Starting datasource-connections relationship CSV generation process.")

    # Load and validate configuration
    config = load_YAML_config(CONFIG_FILE_PATH)
    if not config:
        logger.error("Failed to load configuration. Aborting CSV generation.")
        return

    file_settings = config.get('file_settings', {})
    data_folder_path_str = file_settings.get('data_folder_path')
    temp_subfolder_name = file_settings.get('temp_subfolder_name')
    relationship_csv_filename = file_settings.get('datasource_connections_csv_filename')

    if not all([data_folder_path_str, temp_subfolder_name, relationship_csv_filename]):
        logger.error(
            "Missing one or more required 'file_settings' keys "
            "('data_folder_path', 'temp_subfolder_name', 'datasource_connections_csv_filename') in config."
        )
        return

    # Setup output directory
    output_directory = project_root / data_folder_path_str / temp_subfolder_name
    csv_filepath = output_directory / relationship_csv_filename

    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Ensured output directory exists: {output_directory}")
    except OSError as e:
        logger.critical(f"Error creating output directory '{output_directory}': {e}", exc_info=True)
        return

    if not datasource_connections:
        logger.info("No datasource-connection relationships found. CSV will not be generated.")
        return

    logger.info(f"Total datasource-connection relationships found: {len(datasource_connections)}")

    # Define CSV headers to match the data field names
    headers = [
        'datasource_id',
        'connection_id'
    ]

    # Sort relationships by datasource ID
    sorted_relationships = sorted(datasource_connections, key=lambda x: x['datasource_id'])

    # Write to CSV
    logger.info(f"Writing datasource-connections relationship CSV to: {csv_filepath}")
    try:
        with open(csv_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(sorted_relationships)
        logger.info(f"Relationship CSV file '{relationship_csv_filename}' generated successfully at {csv_filepath}")
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

    # Get datasource-connection relationships using existing transformations
    datasource_connections = get_datasource_connections(raw_data=raw_data)
    
    # Generate the CSV
    generate_datasource_connections_csv_from_config(datasource_connections)
    logger.info("Script execution finished.") 