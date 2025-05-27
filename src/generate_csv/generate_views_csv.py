import csv
import os
from pathlib import Path
from typing import List, Dict, Any

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_views import get_dashboard_views

current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'


def generate_views_csv_from_config(views_data) -> None:
    """
    Retrieves view data by calling get_dashboard_views()
    and generates a CSV file based on configuration settings.
    """
    logger.info("Starting views CSV generation process.")

    config = load_YAML_config(CONFIG_FILE_PATH)
    if not config:
        logger.error("Failed to load configuration. Aborting views CSV generation.")
        return

    file_settings = config.get('file_settings', {})
    data_folder_path_str = file_settings.get('data_folder_path')
    temp_subfolder_name = file_settings.get('temp_subfolder_name')
    views_csv_filename = file_settings.get('views_csv_filename')

    if not all([data_folder_path_str, temp_subfolder_name, views_csv_filename]):
        logger.error(
            "Missing one or more required 'file_settings' keys "
            "('data_folder_path', 'temp_subfolder_name', 'views_csv_filename') in config."
        )
        return

    output_directory = project_root / data_folder_path_str / temp_subfolder_name
    csv_filepath = output_directory / views_csv_filename

    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Ensured output directory exists: {output_directory}")
    except OSError as e:
        logger.critical(f"Error creating output directory '{output_directory}': {e}", exc_info=True)
        return


    if not views_data:
        logger.info("No views data retrieved. Views CSV will not be generated.")
        return

    logger.info(f"Total views found: {len(views_data)}")

    # Sort views by 'name'
    sorted_views = sorted(views_data, key=lambda x: x.get('name', ''))

    logger.debug("Sorted view details:")
    for item in sorted_views:
        logger.debug(f"ID: {item.get('id', 'N/A')}, Name: {item.get('name', 'N/A')}, Workbook ID: {item.get('workbook_id', 'N/A')}")

    headers = [
        'view_id',
        'view_name',
        'workbook_id',
        'view_path',
        'view_type',
        'view_created_at',
        'view_updated_at',
    ]

    processed_views = []
    for view in sorted_views:
        processed_views.append({
            'view_id': view.get('id', "null"),
            'view_name': view.get('name', "null"),
            'workbook_id': view.get('workbook_id', "null"),
            'view_path': view.get('path', "null"),
            'view_type': view.get('type', "null"),
            'view_created_at': view.get('created_at', "null"),
            'view_updated_at': view.get('updated_at', "null"),
        })

    logger.info(f"Writing views CSV to: {csv_filepath}")
    try:
        with open(csv_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(processed_views)
        logger.info(f"Views CSV file '{views_csv_filename}' generated successfully at {csv_filepath}")
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

    raw_data = get_dashboard_views(raw_data= raw_data)
    generate_views_csv_from_config(raw_data)
    logger.info("Script execution finished.")
