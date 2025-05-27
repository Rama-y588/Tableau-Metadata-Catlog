import csv
import os
from pathlib import Path
from typing import List, Dict, Any, Tuple

from src.utils.logger import app_logger as logger
from src.utils.helper import load_YAML_config
from src.Transformations.list_tags import get_tags

current_file = Path(__file__).resolve()
project_root = current_file.parents[2]
CONFIG_FILE_PATH = project_root / 'config' / 'csv_exporter.yaml'


def generate_tags_csv_from_config(tags: List[Dict[str, Any]]) -> Tuple[bool, str]:
    """
    Generate CSV file for tags data.
    
    Args:
        tags (List[Dict[str, Any]]): List of tag data
        
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
        csv_file = output_dir / "tags.csv"
        
        # Define CSV headers
        headers = [
            'Tag ID',
            'Tag Name',
            'Creation Date',
            'Last Modified Date'
        ]
        
        # Write to CSV
        with open(csv_file, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=headers)
            writer.writeheader()
            
            # Sort tags by name
            sorted_tags = sorted(tags, key=lambda x: x.get('name', ''))
            
            for tag in sorted_tags:
                writer.writerow({
                    'Tag ID': tag.get('id', ''),
                    'Tag Name': tag.get('name', ''),
                    'Creation Date': tag.get('createdAt', ''),
                    'Last Modified Date': tag.get('updatedAt', '')
                })
        
        logger.info(f"Successfully generated tags CSV at: {csv_file}")
        return True, f"Generated tags CSV with {len(tags)} entries"
        
    except Exception as e:
        error_msg = f"Failed to generate tags CSV: {str(e)}"
        logger.error(error_msg)
        return False, error_msg


if __name__ == "__main__":
    logger.info("Script execution started.")
    import json
    root_folder = Path(__file__).resolve().parents[2]
    
    test_data_path = root_folder / "sample_data" / "data_test.json"
    with open(test_data_path, 'r') as f:
        raw_data = json.load(f)

    # Get tags using existing transformations
    tags = get_tags(raw_data=raw_data)
    
    # Generate the CSV
    generate_tags_csv_from_config(tags)
    logger.info("Script execution finished.") 