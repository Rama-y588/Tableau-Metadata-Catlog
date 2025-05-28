import json
import logging
from pathlib import Path
from typing import Dict, Any

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.transform_all import transform_all

# Define module name for logging
MODULE_NAME = "metadata_loader"

def load_metadata() -> Dict[str, Any]:
    """
    Loads the metadata from the JSON file and passes it to transform_all.
    """
    # Define paths
    current_file = Path(__file__).resolve()
    project_root = current_file.parents[2]
    config_file_path = project_root / 'config' / 'csv_exporter.yaml'
    
    # Load config
    config = load_YAML_config(config_file_path)
    file_settings = config.get('file_settings', {})
    
    # Get metadata file path from config
    data_folder_path = file_settings.get('data_folder_path')
    metadata_folder_name = file_settings.get('metadata_folder_name')
    metadata_json_filename = file_settings.get('metadata_json_filename')
    
    metadata_file_path = project_root / data_folder_path / metadata_folder_name / metadata_json_filename
    
    # Load metadata
    with open(metadata_file_path, 'r', encoding='utf-8') as f:
        metadata = json.load(f)
    
    return metadata

# --- Testing ---
if __name__ == "__main__":
    load_metadata()