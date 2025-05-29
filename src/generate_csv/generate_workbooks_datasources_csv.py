import json
import csv
import os
from pathlib import Path
from typing import List, Dict, Any, Set
from datetime import datetime
from enum import Enum

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_workbook_datasources import get_workbook_datasources

current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]
config_path = root_folder / "config" / "csv_exporter.yaml"
file_name = current_file.name

class CSVGenerationStatus(Enum):
    SUCCESS = "Success"
    PARTIAL = "Partial"
    FAILED = "Failed"

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
                    relationship_key = f"{row['workbook_id']}:{row['datasource_id']}"
                    existing_relationships.add(relationship_key)
            logger.info(f"[{file_name}] Read {len(existing_relationships)} existing relationships")
        except Exception as e:
            logger.error(f"[{file_name}] Error reading existing relationships: {e}")
    return existing_relationships

def generate_workbook_datasources_csv_from_config(workbook_datasources: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Generate CSV file for workbook-datasource relationships with duplicate handling.
    
    Args:
        workbook_datasources (List[Dict[str, Any]]): List of workbook-datasource relationships
        config_path (Path): Path to configuration file
        
    Returns:
        Dict[str, Any]: Status dictionary with detailed information
    """
    start_time = datetime.now()
    logger.info(f"[{file_name}] Starting workbook-datasource relationship CSV generation process.")
    
    # Initialize status dictionary
    status = {
        "status": CSVGenerationStatus.FAILED.value,
        "total_relationships": len(workbook_datasources) if workbook_datasources else 0,
        "processed_relationships": 0,
        "skipped_relationships": 0,
        "error_message": None,
        "file_path": None
    }
    
    try:
        config = load_YAML_config(config_path)
        if not config:
            status["error_message"] = "Failed to load configuration"
            return status

        file_settings = config.get('csv_paths', {})
        data_folder_path_str = file_settings.get('data_folder_path')
        temp_subfolder_name = file_settings.get('temp_subfolder_name')
        relationship_csv_filename = file_settings.get('workbook_datasources_csv_filename')

        if not all([data_folder_path_str, temp_subfolder_name, relationship_csv_filename]):
            status["error_message"] = "Missing required file settings in config"
            return status

        output_directory = Path(__file__).resolve().parents[2] / data_folder_path_str / temp_subfolder_name
        csv_filepath = output_directory / relationship_csv_filename
        status["file_path"] = str(csv_filepath)

        # Create output directory if it doesn't exist
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"[{file_name}] Output directory ready: {output_directory}")

        # Read existing relationships
        existing_relationships = read_existing_relationships(csv_filepath)
        
        if not workbook_datasources:
            status["status"] = CSVGenerationStatus.SUCCESS.value
            status["error_message"] = "No relationships to process"
            return status

        # Filter out duplicates
        new_relationships = []
        skipped_count = 0
        for rel in workbook_datasources:
            relationship_key = f"{rel['workbook_id']}:{rel['datasource_id']}"
            if relationship_key not in existing_relationships:
                new_relationships.append(rel)
                existing_relationships.add(relationship_key)
            else:
                skipped_count += 1

        status["skipped_relationships"] = skipped_count

        if not new_relationships:
            processing_time = (datetime.now() - start_time).total_seconds()
            logger.info(f"[{file_name}] No new relationships to write. Processing time: {processing_time:.2f} seconds")
            status["status"] = CSVGenerationStatus.SUCCESS.value
            status["error_message"] = "All relationships already exist"
            return status

        # Prepare headers and sort data
        headers = ['workbook_id', 'datasource_id']
        sorted_data = sorted(new_relationships, key=lambda x: x['workbook_id'])

        # Write to CSV (append mode if file exists)
        mode = 'a' if csv_filepath.exists() else 'w'
        logger.info(f"[{file_name}] Writing to CSV in {mode} mode: {csv_filepath}")
        
        try:
            with open(csv_filepath, mode, newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=headers)
                if mode == 'w':
                    writer.writeheader()
                writer.writerows(sorted_data)
            
            processing_time = (datetime.now() - start_time).total_seconds()
            status["processed_relationships"] = len(new_relationships)
            status["status"] = CSVGenerationStatus.SUCCESS.value
            
            logger.info(f"[{file_name}] Successfully wrote {len(new_relationships)} new relationships")
            logger.info(f"[{file_name}] Skipped {skipped_count} duplicate relationships")
            logger.info(f"[{file_name}] Processing time: {processing_time:.2f} seconds")

        except Exception as e:
            error_msg = f"Error writing CSV: {str(e)}"
            logger.critical(f"[{file_name}] {error_msg}", exc_info=True)
            status["status"] = CSVGenerationStatus.FAILED.value
            status["error_message"] = error_msg

    except Exception as e:
        error_msg = f"Unexpected error: {str(e)}"
        logger.exception(f"[{file_name}] {error_msg}")
        status["status"] = CSVGenerationStatus.FAILED.value
        status["error_message"] = error_msg

    return status

if __name__ == "__main__":
    logger.info(f"[{file_name}] Execution started")
    
    try:
        # Load test data
        test_data_path = root_folder / "sample_data" / "data_test.json"
        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)
        logger.debug(f"[{file_name}] Test data loaded")

        # Get relationships and generate CSV
        workbook_datasources = get_workbook_datasources(raw_data)
        result = generate_workbook_datasources_csv_from_config(workbook_datasources)
        
        logger.info(f"[{file_name}] CSV generation status: {result['status']}")
        logger.info(f"[{file_name}] Total relationships: {result['total_relationships']}")
        logger.info(f"[{file_name}] Processed relationships: {result['processed_relationships']}")
        logger.info(f"[{file_name}] Skipped relationships: {result['skipped_relationships']}")
        if result['error_message']:
            logger.info(f"[{file_name}] Message: {result['error_message']}")

    except FileNotFoundError as e:
        logger.error(f"[{file_name}] File not found: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"[{file_name}] Invalid JSON format: {e}")
    except Exception as e:
        logger.exception(f"[{file_name}] Unexpected error occurred: {e}")

    logger.info(f"[{file_name}] Execution completed")