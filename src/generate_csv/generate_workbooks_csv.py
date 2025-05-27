import csv
import os
from pathlib import Path
from typing import List, Dict, Any

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_workbooks import get_workbooks

current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'


def generate_workbooks_csv_from_config(workbooks_data) -> None:
    """
    Generates a CSV file from workbook data based on configuration settings.
    
    Args:
        workbooks_data (List[Dict[str, Any]]): List of workbook data dictionaries
    """
    logger.info("Starting workbooks CSV generation process.")

    config = load_YAML_config(CONFIG_FILE_PATH)
    if not config:
        logger.error("Failed to load configuration. Aborting workbooks CSV generation.")
        return

    file_settings = config.get('file_settings', {})
    data_folder_path_str = file_settings.get('data_folder_path')
    temp_subfolder_name = file_settings.get('temp_subfolder_name')
    workbooks_csv_filename = file_settings.get('workbooks_csv_filename')

    if not all([data_folder_path_str, temp_subfolder_name, workbooks_csv_filename]):
        logger.error(
            "Missing one or more required 'file_settings' keys "
            "('data_folder_path', 'temp_subfolder_name', 'workbooks_csv_filename') in config."
        )
        return

    output_directory = project_root / data_folder_path_str / temp_subfolder_name
    csv_filepath = output_directory / workbooks_csv_filename

    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Ensured output directory exists: {output_directory}")
    except OSError as e:
        logger.critical(f"Error creating output directory '{output_directory}': {e}", exc_info=True)
        return

    if not workbooks_data:
        logger.info("No workbooks data retrieved. Workbooks CSV will not be generated.")
        return

    logger.info(f"Total workbooks found: {len(workbooks_data)}")

    # Sort workbooks by 'name'
    sorted_workbooks = sorted(workbooks_data, key=lambda x: x.get('name', ''))

    logger.debug("Sorted workbook details:")
    for item in sorted_workbooks:
        logger.debug(f"ID: {item.get('id', 'N/A')}, Name: {item.get('name', 'N/A')}, Project: {item.get('project_name', 'N/A')}")

    headers = [
        'workbook_id',
        'workbook_name',
        'project_id',
        'project_name',
        'owner_id',
        'owner_name',
        'description',
        'created_at',
        'updated_at'
    ]

    processed_workbooks = []
    for workbook in sorted_workbooks:
        processed_workbooks.append({
            'workbook_id': workbook.get('id', "null"),
            'workbook_name': workbook.get('name', "null"),
            'project_id': workbook.get('project_id', "null"),
            'project_name': workbook.get('project_name', "null"),
            'owner_id': workbook.get('owner_id', "null"),
            'description': workbook.get('description', "null"),
            'created_at': workbook.get('created_at', "null"),
            'updated_at': workbook.get('updated_at', "null")
        })

    logger.info(f"Writing workbooks CSV to: {csv_filepath}")
    try:
        with open(csv_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(processed_workbooks)
        logger.info(f"Workbooks CSV file '{workbooks_csv_filename}' generated successfully at {csv_filepath}")
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

    raw_data = get_workbooks(raw_data=raw_data)
    generate_workbooks_csv_from_config(raw_data)
    logger.info("Script execution finished.") 