import csv
import os
from pathlib import Path
from typing import List, Dict, Any, Tuple

from src.utils.logger import app_logger as logger


def generate_connections_csv_from_config(connections: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """
    Generate CSV file for connections data.
    
    Args:
        connections (List[Dict[str, Any]]): List of connection data
        
    Returns:
        Tuple[bool, str]: Success status and message
    """
    try:
        # Get project root and create output directory
        current_file = Path(__file__).resolve()
        project_root = current_file.parents[1]
        output_dir = project_root / "output" / "csv"
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Define CSV file path
        csv_file = output_dir / "connections.csv"
        
        # Define CSV headers
        headers = [
            'Connection ID',
            'Connection Name',
            'Connection Type',
            'Connects To',
            'Creation Date',
            'Last Modified Date'
        ]
        
        # Write to CSV
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            # Sort connections by name
            sorted_connections = sorted(connections, key=lambda x: x.get('name', ''))
            
            for connection in sorted_connections:
                writer.writerow({
                    'Connection ID': connection.get('id', ''),
                    'Connection Name': connection.get('name', ''),
                    'Connection Type': connection.get('type', ''),
                    'Connects To': connection.get('connectsTo', ''),
                    'Creation Date': connection.get('createdAt', ''),
                    'Last Modified Date': connection.get('updatedAt', '')
                })
        
        logger.info(f"Successfully generated connections CSV at: {csv_file}")
        return True, f"Generated connections CSV with {len(connections)} entries"
        
    except Exception as e:
        error_msg = f"Failed to generate connections CSV: {str(e)}"
        logger.error(error_msg)
        return False, error_msg


if __name__ == "__main__":
    logger.info("Script execution started.")
    import json
    root_folder = Path(__file__).resolve().parents[2]
    
    test_data_path = root_folder / "sample_data" / "data_test.json"
    with open(test_data_path, 'r') as f:
        raw_data = json.load(f)

    raw_data = get_connections(raw_data=raw_data)
    status, message = generate_connections_csv_from_config(raw_data)
    print(status, message)
    logger.info("Script execution finished.") 