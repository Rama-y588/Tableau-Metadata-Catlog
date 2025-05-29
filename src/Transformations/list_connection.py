from datetime import datetime
from typing import List, Dict, Any, Set, Tuple
from pathlib import Path
import logging
from src.utils.logger import app_logger as logger

# Define module name for logging (using file name)
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]
FILE_NAME = "list_connection.py"

def get_connections(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and transform connection data from Tableau workbook data.
    Processes both upstream and embedded datasources.

    Args:
        raw_data (Dict[str, Any]): Raw JSON object containing Tableau workbook data.

    Returns:
        List[Dict[str, Any]]: A list of unique connection metadata dictionaries.
    """
    try:
        # Use a set to track unique connections
        unique_connections: Set[Tuple[str, str, str]] = set()
        connections: List[Dict[str, Any]] = []

        workbooks = raw_data.get('data', {}).get('workbooks', [])
        logger.info(f"[{FILE_NAME}] Processing connections for {len(workbooks)} workbook(s)...")

        # Process both upstream and embedded datasources
        for workbook in workbooks:
            # Process upstream datasources
            for ds in workbook.get('upstreamDatasources', []):
                for db in ds.get('upstreamDatabases', []):
                    try:
                        # Extract and clean connection properties
                        db_name = db.get('name', '').strip()
                        connection_type = db.get('connectionType', '').strip()
                        connects_to = db.get('__typename', '').strip()

                        if not all([db_name, connection_type, connects_to]):
                            logger.warning(f"[{FILE_NAME}] Skipping incomplete connection data: {db}")
                            continue

                        # Create a unique key for the connection
                        connection_key = (db_name, connection_type, connects_to)
                        
                        # Only add if this is a unique connection
                        if connection_key not in unique_connections:
                            unique_connections.add(connection_key)
                            
                            connections.append({
                                'id': f"conn_{len(connections)}",
                                'name': db_name,
                                'connection_type': connection_type,
                                'connects_to': connects_to
                            })
                            logger.debug(f"[{FILE_NAME}] Added unique connection: {db_name}")

                    except Exception as e:
                        logger.error(f"[{FILE_NAME}] Error processing database connection: {str(e)}")
                        continue

            # Process embedded datasources
            for ds in workbook.get('embeddedDatasources', []):
                for db in ds.get('upstreamDatabases', []):
                    try:
                        # Extract and clean connection properties
                        db_name = db.get('name', '').strip()
                        connection_type = db.get('connectionType', '').strip()
                        connects_to = db.get('__typename', '').strip()

                        if not all([db_name, connection_type, connects_to]):
                            logger.warning(f"[{FILE_NAME}] Skipping incomplete connection data: {db}")
                            continue

                        # Create a unique key for the connection
                        connection_key = (db_name, connection_type, connects_to)
                        
                        # Only add if this is a unique connection
                        if connection_key not in unique_connections:
                            unique_connections.add(connection_key)
                            
                            connections.append({
                                'id': f"conn_{len(connections)}",
                                'name': db_name,
                                'connection_type': connection_type,
                                'connects_to': connects_to
                            })
                            logger.debug(f"[{FILE_NAME}] Added unique connection: {db_name}")

                    except Exception as e:
                        logger.error(f"[{FILE_NAME}] Error processing database connection: {str(e)}")
                        continue

        # Sort connections by name
        connections.sort(key=lambda x: x.get('name', ''))
        
        logger.info(f"[{FILE_NAME}] Successfully extracted {len(connections)} unique connections")
        return connections

    except Exception as e:
        logger.error(f"[{FILE_NAME}] Failed to extract connections: {str(e)}", exc_info=True)
        raise

def _validate_connection_data(db: Dict[str, Any]) -> bool:
    """
    Validates if the connection data has all required fields.
    
    Args:
        db (Dict[str, Any]): Database connection data
        
    Returns:
        bool: True if data is valid, False otherwise
    """
    required_fields = ['name', 'connectionType', '__typename']
    return all(db.get(field) for field in required_fields)

if __name__ == "__main__":
    import json
    logger.info(f"[{FILE_NAME}] --- Extracting Connections from Raw Data ---")

    try:
        # Determine the path to the sample data file
        test_data_path = root_folder / "sample_data" / "data_test.json"

        # Ensure the test data file exists
        if not test_data_path.exists():
            logger.error(f"[{FILE_NAME}] Test data file not found at {test_data_path}")
            exit(1)

        # Load the test data
        with open(test_data_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        # Get connections
        connections = get_connections(raw_data)

        # Log the extracted connections in a formatted way
        for conn in connections:
            logger.info(f"[{FILE_NAME}] Connection - ID: {conn['id']}, Name: {conn['name']}, "
                       f"Type: {conn['connection_type']}, Connects to: {conn['connects_to']}")

    except Exception as e:
        logger.error(f"[{FILE_NAME}] Error during test execution: {str(e)}", exc_info=True)