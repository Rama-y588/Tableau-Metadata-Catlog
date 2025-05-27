"""
Tableau Workbook Listing Module

This module provides functionality to list workbooks from Tableau Server and
generate CSV reports. It includes both live server and dummy data functionality.
"""

import json
from pathlib import Path
from typing import List, Dict, Any

from src.utils.logger import app_logger as logger

# --- Module Constants ---
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]


def get_workbooks(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extracts workbook information from the provided raw JSON data.

    Args:
        raw_data (Dict[str, Any]): Dictionary containing workbook data.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing workbook information:
            - id: Workbook's unique identifier
            - name: Workbook's name
            - description: Workbook's description (empty string if not available)
            - owner_id: Workbook owner's id
            - project_id: Project's name (from dummy data)
            - created_at: Creation timestamp
            - updated_at: Last update timestamp
            - url : Workbook's URL

    Raises:
        Exception: For unexpected errors during parsing.
    """
    try:
        workbooks_list = []
        all_workbooks_raw = raw_data.get('data', {}).get('workbooks', [])

        for workbook_raw in all_workbooks_raw:
            workbook_info = {
                'id': workbook_raw.get('id', ''),
                'name': workbook_raw.get('name', ''),
                'description': workbook_raw.get('description', ''),
                'owner_id': workbook_raw.get('owner', {}).get('id', ''),
                'project_id': workbook_raw.get('projectName', ''),
                'created_at': workbook_raw.get('createdAt', ''),
                'updated_at': workbook_raw.get('updatedAt', ''),
                'url': workbook_raw.get('uri', '')
            }
            workbooks_list.append(workbook_info)
            logger.debug(f"Found dummy workbook: {workbook_info['name']} in project {workbook_info['project_id']}")

        logger.info(f"Successfully retrieved {len(workbooks_list)} workbooks from dummy data.")
        return workbooks_list

    except Exception as e:
        logger.error(f"An unexpected error occurred while fetching workbooks from dummy data: {str(e)}")
        raise


# --- Example Usage (for testing) ---
if __name__ == "__main__":
    raw_data_path_for_test = root_folder / "sample_data" / "data_test.json"

    print("\n--- Fetching workbooks from dummy data only ---")
    try:
        with open(raw_data_path_for_test, 'r') as f:
            raw_data = json.load(f)

        dummy_workbooks = get_workbooks(raw_data=raw_data)

        for wb in dummy_workbooks:
            print(f"ID: {wb['id']}, Name: {wb['name']}, Project: {wb['project_id']}, URL: {wb['url']}")
    except FileNotFoundError:
        print(f"Dummy data file not found at {raw_data_path_for_test}")
    except json.JSONDecodeError:
        print(f"Error decoding JSON from {raw_data_path_for_test}")
    except Exception as e:
        print(f"Error fetching dummy data: {e}")
