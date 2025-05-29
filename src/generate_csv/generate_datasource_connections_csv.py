import json
import csv
import os
from pathlib import Path
from typing import List, Dict, Any, Set
from datetime import datetime

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_datasources_connections import get_datasource_connections

current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]
config_path = root_folder / "config" / "csv_exporter.yaml"
file_name = current_file.name

def read_existing_relationships(csv_filepath: Path) -> Set[str]:
    """
    Read existing relationships from CSV file to prevent duplicates.
    
    Args:
        csv_filepath (Path): Path to the CSV file
        
    Returns:
        Set[str]: Set of unique relationship keys
    """
    existing_relationships = set()
    if csv_filepath.exists():
        try:
            with open(csv_filepath, 'r', newline='', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    relationship_key = f"{row['datasource_id']}:{row['connection_id']}"
                    existing_relationships.add(relationship_key)
            logger.info(f"{file_name} Read {len(existing_relationships)} existing relationships")
        except Exception as e:
            logger.error(f"{file_name} Error reading existing relationships: {e}")
    return existing_relationships

def generate_datasource_connections_csv_from_config(datasource_connections: List[Dict[str, Any]]) -> Dict[str, str]:
    """
    Generate CSV file for datasource-connection relationships with duplicate handling.
    
    Args:
        datasource_connections (List[Dict[str, Any]]): List of datasource-connection relationships
        config_path (Path): Path to configuration file
        
    Returns:
        Dict[str, str]: Status dictionary with 'status' and 'message' keys
    """
    start_time = datetime.now()
    logger.info(f"{file_name} Starting datasource-connections relationship CSV generation process.")
    
    try:
        config = load_YAML_config(config_path)
        if not config:
            return {"status": "Failed", "message": "Failed to load configuration"}

        file_settings = config.get('csv_paths', {})
        data_folder_path_str = file_settings.get('data_folder_path')
        temp_subfolder_name = file_settings.get('temp_subfolder_name')
        relationship_csv_filename = file_settings.get('datasource_connections_csv_filename')

        if not all([data_folder_path_str, temp_subfolder_name, relationship_csv_filename]):
            return {"status": "Failed", "message": "Missing required file settings in config"}

        output_directory = Path(__file__).resolve().parents[2] / data_folder_path_str / temp_subfolder_name
        csv_filepath = output_directory / relationship_csv_filename

        # Create output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"{file_name} Output directory ready: {output_directory}")

        # Read existing relationships
        existing_relationships = read_existing_relationships(csv_filepath)
        
        if not datasource_connections:
            return {"status": "Success", "message": "No new relationships to write"}

        # Filter out duplicates
        new_relationships = []
        skipped_count = 0
        for rel in datasource_connections:
            relationship_key = f"{rel['datasource_id']}:{rel['connection_id']}"
            if relationship_key not in existing_relationships:
                new_relationships.append(rel)
                existing_relationships.add(relationship_key)
            else:
                skipped_count += 1

        if not new_relationships:
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"{file_name} No new relationships to write. Processing time: {processing_time:.2f} seconds")
            return {"status": "Success", "message": "All relationships already exist"}

        # Prepare headers and sort data
        headers = ['datasource_id', 'connection_id']
        sorted_data = sorted(new_relationships, key=lambda x: x['datasource_id'])

        # Write to CSV (append mode if file exists)
        mode = 'a' if csv_filepath.exists() else 'w'
        logger.info(f"{file_name} Writing to CSV in {mode} mode: {csv_filepath}")
        
        try:
            with open(csv_filepath, mode, newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if mode == 'w':
                    writer.writeheader()
                writer.writerows(sorted_data)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"{file_name} Successfully wrote {len(new_relationships)} new relationships")
            logger.info(f"{file_name} Skipped {skipped_count} duplicate relationships")
            logger.info(f"{file_name} Processing time: {processing_time:.2f} seconds")
            
            return {
                "status": "Success",
                "message": f"Wrote {len(new_relationships)} new relationships, skipped {skipped_count} duplicates"
            }

        except Exception as e:
            error_msg = f"Error writing CSV: {str(e)}"
            logger.critical(f"{file_name} {error_msg}", exc_info=True)
            return {"status": "Failed", "message": error_msg}

    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.exception(f"{file_name} {error_msg}")
        return {"status": "Failed", "message": error_msg}

if __name__ == "__main__":
    logger.info(f"{file_name} Execution started")
    
    try:
        # Load test data
        test_data_path = root_folder / "sample_data" / "data_test.json"
        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)
        logger.debug(f"{file_name} Test data loaded")

        # Get relationships and generate CSV
        datasource_connections = get_datasource_connections(raw_data)
        result = generate_datasource_connections_csv_from_config(datasource_connections)
        
        logger.info(f"{file_name} CSV generation status: {result['status']}")
        logger.info(f"{file_name} Message: {result['message']}")

    except FileNotFoundError as e:
        logger.error(f"{file_name} File not found: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"{file_name} Invalid JSON format: {e}")
    except Exception as e:
        logger.exception(f"{file_name} Unexpected error occurred: {e}")

    logger.info(f"{file_name} Execution completed")