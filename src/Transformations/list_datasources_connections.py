from typing import List, Dict, Any
from src.utils.logger import app_logger as logger
from src.Transformations.list_datasources import get_datasources
from src.Transformations.list_connection import get_connections

def get_datasource_connections(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and transform datasource-connection relationships using existing transformations.
    Uses get_datasources() and get_connections() to maintain consistency.

    Args:
        raw_data (Dict[str, Any]): Raw JSON object containing Tableau workbook data.

    Returns:
        List[Dict[str, Any]]: A list of datasource-connection relationship dictionaries.
    """
    try:
        # Get datasources and connections using existing transformations
        datasources = get_datasources(raw_data)
        connections = get_connections(raw_data)
        
        logger.info(f"Processing relationships between {len(datasources)} datasources and {len(connections)} connections...")

        # Create a mapping of datasource IDs to their connections
        datasource_connections = []
        connection_index = 0

        for datasource in datasources:
            datasource_id = datasource.get('id')
            if not datasource_id:
                continue

            # Each datasource gets a connection
            if connection_index < len(connections):
                connection_id = connections[connection_index]['id']
                datasource_connections.append({
                    'datasource_id': datasource_id,
                    'connection_id': connection_id
                })
                connection_index += 1

        logger.info(f"Created {len(datasource_connections)} datasource-connection relationships.")
        return datasource_connections

    except Exception as e:
        logger.exception(f"Failed to create datasource-connection relationships: {e}")
        raise


if __name__ == "__main__":
    import json
    from pathlib import Path

    logger.info("\n--- Extracting Datasource-Connection Relationships from Raw Data ---")

    try:
        root_folder = Path(__file__).resolve().parents[2]
        test_data_path = root_folder / "sample_data" / "data_test.json"

        if not test_data_path.exists():
            logger.error(f"Test data file not found at {test_data_path}")
            exit(1)

        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)

        relationships = get_datasource_connections(raw_data)

        for rel in relationships:
            print(rel)

    except Exception as e:
        logger.exception(f"Error during test execution: {e}")
