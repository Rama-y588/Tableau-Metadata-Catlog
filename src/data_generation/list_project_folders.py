import csv
from pathlib import Path
from typing import List, Dict, Any, Optional
from src.utils.logger import app_logger as logger
from src.generate_csv.generate_projects_csv import generate_project_csv_from_config
from src.config.config_loader import get_project_file_path
import yaml
generate_project_csv_from_config()

current_file = Path(__file__).resolve()
root_folder = current_file.parents[3]

configuration_path = root_folder / "config" / "tableau.yaml"
config = []
with open(configuration_path,"r") as f:
    congig = yaml.safe_load(f)

    file_settings = config.get('file_settings', {})
    data_folder_path_str = file_settings.get('data_folder_path')
    temp_subfolder_name = file_settings.get('temp_subfolder_name')
    datasources_csv_filename = file_settings.get('project_csv_filename')
    print(file_settings,data_folder_path_str,temp_subfolder_name,datasources_csv_filename)

