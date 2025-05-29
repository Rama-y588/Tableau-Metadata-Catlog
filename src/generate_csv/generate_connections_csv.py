import csv
import os
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Set, Tuple
from enum import Enum

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_connection import get_connections

# --- Define root folder for standalone execution ---
current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'

# Define module name for logging (using file name)
MODULE_NAME = "generate_connections_csv.py"

class CSVGenerationStatus(Enum):
    SUCCESS = "Success"
    PARTIAL = "Partial"
    FAILED = "Failed"
def read_existing_connections(csv_filepath: Path) -> Set[Tuple[str, str, str]]:
    """
    Read existing connections from CSV file to avoid duplicates.
    
    Args:
        csv_filepath (Path): Path to the CSV file
        
    Returns:
        Set[Tuple[str, str, str]]: Set of existing connection keys (name, type, connects_to)
    """
    existing_connections = set()
    if csv_filepath.exists():
        try:
            with open(csv_filepath, 'r', newline='', encoding='utf-8') as csvfile:
                reader = csv.DictReader(csvfile)
                for row in reader:
                    # Use the new header names to match the CSV structure
                    connection_key = (
                        row.get('Connection_Name', '').strip(),
                        row.get('Connection_Type', '').strip(),
                        row.get('Connection_Target', '').strip()
                    )
                    if all(connection_key):  # Only add if all fields are present
                        existing_connections.add(connection_key)
            logger.info(f"[{MODULE_NAME}] Found {len(existing_connections)} existing connections in CSV")
        except Exception as e:
            logger.error(f"[{MODULE_NAME}] Error reading existing connections: {str(e)}")
    return existing_connections
def generate_connection_csv_from_config(connections_data: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generates a CSV file from connection data based on configuration settings.
    
    Args:
        connections_data (List[Dict[str, Any]]): List of connection data dictionaries
        
    Returns:
        Dict[str, Any]: Status dictionary containing:
            - status: "Success", "Partial", or "Failed"
            - total_connections: Total number of connections processed
            - processed_connections: Number of connections successfully written
            - error_message: Error message if any
            - file_path: Path to the generated CSV file
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting connections CSV generation process")
    
    status = {
        "status": CSVGenerationStatus.FAILED.value,
        "total_connections": 0,
        "processed_connections": 0,
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
    required_settings = ['data_folder_path', 'temp_subfolder_name', 'connections_csv_filename']
    missing_settings = [setting for setting in required_settings if not file_settings.get(setting)]
    
    if missing_settings:
        error_msg = f"Missing required 'file_settings' keys: {', '.join(missing_settings)}"
        logger.error(f"[{MODULE_NAME}] {error_msg}")
        status["error_message"] = error_msg
        return status

    # Setup file paths
    output_directory = project_root / file_settings['data_folder_path'] / file_settings['temp_subfolder_name']
    csv_filepath = output_directory / file_settings['connections_csv_filename']
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

    # Read existing connections
    existing_connections = read_existing_connections(csv_filepath)

    # Validate connections data
    if not connections_data:
        logger.info(f"[{MODULE_NAME}] No connections data retrieved. Connections CSV will not be generated.")
        status["status"] = CSVGenerationStatus.SUCCESS.value
        return status

    status["total_connections"] = len(connections_data)
    logger.info(f"[{MODULE_NAME}] Total connections found: {status['total_connections']}")

    # Define headers with meaningful names
    headers = [
        'connection_id',           # Unique identifier for the connection
        'connection_name',         # Name of the connection
        'connection_type',         # Type of connection (e.g., PostgreSQL, MySQL)
        'connection_to'        # What the connection connects to
    ]

    # Process connections
    processed_connections = []
    skipped_connections = 0
    seen_connections = set()  # Track connections we've seen in this batch

    # Sort connections by name before processing
    sorted_connections = sorted(connections_data, key=lambda x: str(x.get('name', '')).lower())

    for conn in sorted_connections:
        try:
            # Create connection key for duplicate checking
            connection_key = (
                str(conn.get('name', '')).strip(),
                str(conn.get('connection_type', '')).strip(),
                str(conn.get('connects_to', '')).strip()
            )

            # Skip if connection already exists in CSV or in current batch
            if connection_key in existing_connections or connection_key in seen_connections:
                skipped_connections += 1
                logger.debug(f"[{MODULE_NAME}] Skipping duplicate connection: {connection_key[0]}")
                continue

            # Map the processed connection with new header names
            processed_connection = {
                'connection_id': str(conn.get('id', '')),
                'connection_name': str(conn.get('name', '')),
                'connection_type': str(conn.get('connection_type', '')),
                'connection_to': str(conn.get('connects_to', ''))
            }
            processed_connections.append(processed_connection)
            seen_connections.add(connection_key)  # Add to seen set to prevent duplicates in current batch

        except Exception as e:
            logger.error(f"[{MODULE_NAME}] Error processing connection {conn.get('id', 'unknown')}: {str(e)}")
            continue

    # Write to CSV
    file_exists = csv_filepath.exists()
    mode = 'a' if file_exists else 'w'
    
    logger.info(f"[{MODULE_NAME}] Writing {len(processed_connections)} new connections to CSV file: {csv_filepath} (mode: {mode})")
    try:
        with open(csv_filepath, mode, newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            if not file_exists:
                writer.writeheader()
            writer.writerows(processed_connections)
        
        status["processed_connections"] = len(processed_connections)
        status["status"] = CSVGenerationStatus.SUCCESS.value
        
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{MODULE_NAME}] Successfully processed connections in {processing_time:.2f} seconds")
        logger.info(f"[{MODULE_NAME}] Connections CSV file updated successfully at: {csv_filepath}")
        logger.info(f"[{MODULE_NAME}] Skipped {skipped_connections} duplicate connections")
        
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

    # Get connections data using get_connections
    connections_data = get_connections(raw_data=raw_data)
    
    # Generate CSV and get status
    status = generate_connection_csv_from_config(connections_data)
    
    # Log detailed status
    logger.info(f"[{MODULE_NAME}] CSV Generation Status:")
    logger.info(f"[{MODULE_NAME}]   Status: {status['status']}")
    logger.info(f"[{MODULE_NAME}]   Total Connections: {status['total_connections']}")
    logger.info(f"[{MODULE_NAME}]   Processed Connections: {status['processed_connections']}")
    if status['error_message']:
        logger.error(f"[{MODULE_NAME}]   Error: {status['error_message']}")
    logger.info(f"[{MODULE_NAME}]   File Path: {status['file_path']}")
    
    logger.info(f"[{MODULE_NAME}] Script execution finished.")