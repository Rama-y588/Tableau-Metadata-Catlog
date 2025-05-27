import csv
import os
import yaml
from pathlib import Path

from src.utils.logger import app_logger as logger # Assuming this import is correct
from src.Transformations.list_projects import get_unique_workbook_projects

# --- Path Configuration ---
# Resolves the absolute path of the current file (data_exporter.py)
current_file_path = Path(__file__).resolve()

# Based on your print output, 'D:\Tableau_application\src\new_code' is your desired project root.
# If your script is directly in 'D:\Tableau_application\src\new_code', then .parent is correct.
project_root = current_file_path.parent.parent.parent # This makes D:\Tableau_application\src\new_code the project root
# Define the absolute path to the configuration file
CONFIG_FILE_PATH = project_root / "config" / "csv_exporter.yaml"

# --- Data Simulation Function ---
def get_unique_workbook_projects() -> list[dict]:
    """
    Simulates fetching unique Tableau project data.
    In a production environment, this function would connect to a database
    (e.g., PostgreSQL, Tableau Server's internal repository) or a Tableau API
    to retrieve actual project information.

    Returns:
        list[dict]: A list of dictionaries, where each dictionary represents
                    a unique Tableau project with 'project_id' and 'project_name'.
    """
    logger.info("Simulating data retrieval for unique workbook projects.")
    # Sample data mimicking what your database query might return
    # raw_data = [
    #     {"project_id": "proj_001", "project_name": "Sales Dashboards"},
    #     {"project_id": "proj_002", "project_name": "Marketing Analytics"},
    #     {"project_id": "proj_003", "project_name": "Finance Reports"},
    #     {"project_id": "proj_001", "project_name": "Sales Dashboards"}, # Duplicate for testing uniqueness
    #     {"project_id": "proj_004", "project_name": "HR Metrics"},
    #     {"project_id": "proj_003", "project_name": "Finance Reports"}, # Duplicate for testing uniqueness
    #     {"project_id": "proj_005", "project_name": "Executive Overviews"},
    # ]

    raw_data = get_unique_workbook_projects()

    # Process raw data to get unique projects
    unique_projects_set = set()
    processed_data = []
    for item in raw_data:
        # Use a tuple of (id, name) to ensure uniqueness based on both fields
        project_tuple = (item['project_id'], item['project_name'])
        if project_tuple not in unique_projects_set:
            unique_projects_set.add(project_tuple)
            processed_data.append(item)
    
    logger.info(f"Retrieved {len(raw_data)} raw project entries, found {len(processed_data)} unique projects.")
    return processed_data

# --- Configuration Loading Function ---
def load_YAML_config(config_path: Path) -> dict | None:
    """
    Loads configuration settings from a YAML file.

    Args:
        config_path (Path): The Path object pointing to the YAML configuration file.

    Returns:
        dict | None: A dictionary containing the configuration settings, or None if
                     the file is not found or cannot be parsed.
    """
    logger.info(f"Attempting to load configuration from: {config_path}")
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = yaml.safe_load(file)
        logger.info("Configuration loaded successfully.")
        return config
    except FileNotFoundError:
        logger.error(f"Configuration file not found at '{config_path}'. Please verify the path and file existence.")
        return None
    except yaml.YAMLError as e:
        logger.critical(f"Error parsing configuration file '{config_path}': {e}. Please check YAML syntax.")
        return None
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading config from '{config_path}': {e}", exc_info=True)
        return None

# --- CSV Generation Function ---
def generate_project_csv_from_config():
    """
    Retrieves unique workbook project data and generates a CSV file
    based on the paths and settings defined in the configuration file
    (`config/csv_exporter.yaml`).
    """
    logger.info("Starting CSV generation process.")

    config = load_YAML_config(CONFIG_FILE_PATH)
    if not config:
        logger.error("Failed to load configuration. Aborting CSV generation.")
        return

    # Extract file settings from the loaded configuration
    file_settings = config.get('file_settings', {})
    data_folder_path_str = file_settings.get('data_folder_path')
    temp_subfolder_name = file_settings.get('temp_subfolder_name')
    project_csv_filename = file_settings.get('project_csv_filename')

    # Validate that all necessary config values were found
    if not all([data_folder_path_str, temp_subfolder_name, project_csv_filename]):
        logger.error(
            "Missing one or more required 'file_settings' "
            "(data_folder_path, temp_subfolder_name, project_csv_filename) in your config file. "
            "Please check config/csv_exporter.yaml."
        )
        return


    
    # Always join relative paths to the project_root
    output_directory = project_root / Path(data_folder_path_str) / temp_subfolder_name
    
        
    csv_filepath = output_directory / project_csv_filename

    # Ensure the output directory exists. Create it if it doesn't.
    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Ensured output directory exists: {output_directory}")
    except OSError as e:
        logger.critical(f"Error creating output directory '{output_directory}': {e}. Permissions issue or invalid path?", exc_info=True)
        return

    # --- Data Retrieval and Processing ---
    project_data = get_unique_workbook_projects()

    if not project_data:
        logger.info("No unique workbook project data retrieved. CSV file will not be generated.")
        return

    logger.info(f"Total unique projects found: {len(project_data)}")
    # Sort the list by project_name for consistent output in CSV and logs
    sorted_projects = sorted(project_data, key=lambda x: x['project_name'])
    logger.debug("Unique Project IDs and Names (Sorted by Name):")
    for item in sorted_projects:
        logger.debug(f"  ID: {item['project_id']}, Name: {item['project_name']}")

    # Define the headers for the CSV file. These must exactly match the dictionary keys.
    headers = ['project_id', 'project_name']

    # --- CSV File Writing ---
    logger.info(f"Attempting to write CSV file to: {csv_filepath}")
    try:
        # 'w' mode for writing, newline='' for consistent CSV, utf-8 encoding for broad character support
        with open(csv_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)

            # Write the header row
            writer.writeheader()

            # Write all processed data rows
            writer.writerows(sorted_projects)

        logger.info(f"CSV file '{project_csv_filename}' generated successfully at: {csv_filepath}")
    except IOError as e:
        logger.critical(f"IOError while writing CSV file to '{csv_filepath}': {e}. Check disk space or permissions.", exc_info=True)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during CSV writing to '{csv_filepath}': {e}", exc_info=True)

# --- Main Execution Block ---
if __name__ == "__main__":
    logger.info("Script execution started.")
    generate_project_csv_from_config()
    logger.info("Script execution finished.")