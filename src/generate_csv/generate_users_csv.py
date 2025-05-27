import csv
import os
import yaml
from src.utils.logger import app_logger as logger
from pathlib import Path
from src.utils.helper import load_YAML_config
from src.Transformations.list_users import get_unique_users

current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'

def generate_user_csv_from_config(user_data):
    """
    Retrieves unique user data (users of workbooks) and generates a CSV file
    based on the paths and settings defined in the configuration file.
    """
    logger.info("Starting user CSV generation process.")

    config = load_YAML_config(CONFIG_FILE_PATH)
    if not config:
        logger.error("Failed to load configuration. Aborting user CSV generation.")
        return

    file_settings = config.get('file_settings', {})
    data_folder_path_str = file_settings.get('data_folder_path')
    temp_subfolder_name = file_settings.get('temp_subfolder_name')
    user_csv_filename = file_settings.get('user_csv_filename')

    if not all([data_folder_path_str, temp_subfolder_name, user_csv_filename]):
        logger.error(
            "Missing one or more required 'user_file_settings' "
            "in your config file. Please check config/csv_exporter.yaml."
        )
        return

    output_directory = project_root / Path(data_folder_path_str) / temp_subfolder_name
    csv_filepath = output_directory / user_csv_filename

    try:
        os.makedirs(output_directory, exist_ok=True)
        logger.info(f"Ensured output directory exists for users: {output_directory}")
    except OSError as e:
        logger.critical(f"Error creating output directory '{output_directory}' for users: {e}", exc_info=True)
        return

    user_data = get_unique_users() # This calls the function you provided

    if not user_data:
        logger.info("No unique user data retrieved. User CSV file will not be generated.")
        return

    logger.info(f"Total unique users found: {len(user_data)}")
    sorted_users = sorted(user_data, key=lambda x: x.get('username', '') or x.get('email', '') or '')
    logger.debug("Unique User Details (Sorted by Username/Email):")
    for item in sorted_users:
        logger.debug(f"  ID: {item.get('id', 'N/A')}, Username: {item.get('username', 'N/A')}, Email: {item.get('email', 'N/A')}")

    # Define the headers for the CSV file with meaningful names
    headers = [
        'user_id',
        'user_name',
        'user_username',
        'user_email',
        'user_created_at',
        'user_updated_at'
    ]

    # Create a list of dictionaries with the new header names
    # This step maps the generic keys from `get_unique_users` to the desired CSV header names.
    processed_user_data = []
    for user_dict in sorted_users:
        processed_user_data.append({
            'user_id': user_dict.get('id',"null"),
            'user_name': user_dict.get('name',"null"),
            'user_username': user_dict.get('username',"null"),
            'user_email': user_dict.get('email',"null"),
            'user_created_at': user_dict.get('created_at',"null"),
            'user_updated_at': user_dict.get('updated_at',"null")
        })

    logger.info(f"Attempting to write user CSV file to: {csv_filepath}")
    try:
        with open(csv_filepath, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(processed_user_data) # Write the processed data
        logger.info(f"User CSV file '{user_csv_filename}' generated successfully at: {csv_filepath}")
    except IOError as e:
        logger.critical(f"IOError while writing user CSV file to '{csv_filepath}': {e}", exc_info=True)
    except Exception as e:
        logger.critical(f"An unexpected error occurred during user CSV writing to '{csv_filepath}': {e}", exc_info=True)


# --- Main Execution Block ---
if __name__ == "__main__":
    logger.info("Script execution started.")
    
    # Call the function to generate the users CSV
    raw_data = get_unique_users() 
    generate_user_csv_from_config(raw_data) 
    
    logger.info("Script execution finished.")