from datetime import datetime
from typing import List, Dict, Any

def get_connections(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and transform connection data from Tableau workbook data.

    Args:
        raw_data (Dict[str, Any]): Raw JSON object containing Tableau workbook data.

    Returns:
        List[Dict[str, Any]]: A list of connection metadata dictionaries.
    """
    connections = []
    connection_id_map = {}  # To avoid duplicates

    workbooks = raw_data.get('data', {}).get('workbooks', [])

    for workbook in workbooks:
        # Upstream Datasource connections
        for ds in workbook.get('upstreamDatasources', []):
            for db in ds.get('upstreamDatabases', []):
                db_key = (
                    db.get('name'),
                    db.get('connectionType'),
                    db.get('__typename')
                )
                if db_key not in connection_id_map:
                    connection_id = f"conn_{len(connections)}"
                    connection_id_map[db_key] = connection_id
                    connections.append({
                        'id': connection_id,
                        'name': db.get('name'),
                        'connection_type': db.get('connectionType'),
                        'connects_to': db.get('__typename'),
                        'created_at': datetime.now(),
                        'updated_at': datetime.now()
                    })

        # Embedded Datasource connections
        for ds in workbook.get('embeddedDatasources', []):
            for db in ds.get('upstreamDatabases', []):
                db_key = (
                    db.get('name'),
                    db.get('connectionType'),
                    db.get('__typename')
                )
                if db_key not in connection_id_map:
                    connection_id = f"conn_{len(connections)}"
                    connection_id_map[db_key] = connection_id
                    connections.append({
                        'id': connection_id,
                        'name': db.get('name'),
                        'connection_type': db.get('connectionType'),
                        'connects_to': db.get('__typename'),
                        'created_at': datetime.now(),
                        'updated_at': datetime.now()
                    })

    return connections



if __name__ == "__main__":
    import json
    from pathlib import Path
    from src.utils.logger import app_logger as logger
    

    logger.info("\n--- Extracting Connections from Raw Data ---")

    try:
        # Determine the path to the sample data file
        root_folder = Path(__file__).resolve().parents[2]
        test_data_path = root_folder / "sample_data" / "data_test.json"
        print(test_data_path)

        # Ensure the test data file exists
        if not test_data_path.exists():
            logger.error(f"Test data file not found at {test_data_path}")
            exit(1)

        # Load the test data
        with open(test_data_path, "r", encoding="utf-8") as f:
            raw_data = json.load(f)

        # Get connections
        connections = get_connections(raw_data)

        # Print the extracted connections
        for conn in connections:
            print(conn)

    except Exception as e:
        logger.exception("Error during test execution")
