"""
Tableau Workbook Listing Module

This module provides functionality to list workbooks from Tableau Server and
generate CSV reports. It includes both live server and dummy data functionality.
"""

import json
from pathlib import Path
from typing import List, Dict, Any, Optional
from functools import lru_cache
import logging
from datetime import datetime

from src.utils.logger import app_logger as logger
from src.Auth import server_connection

# --- Module Constants ---
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]
MODULE_NAME = current_file.name


# Cache server config to avoid repeated lookups
@lru_cache(maxsize=1)
def get_server_config() -> Dict[str, Any]:
    """Get cached server configuration."""
    try:
        default_profile = server_connection._default_profile_name
        config = server_connection._profiles.get(default_profile, {})
        if not config:
            logger.error(f"[{MODULE_NAME}] Server configuration not found")
            raise ValueError("Server configuration is empty")
        return config
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Failed to get server configuration: {str(e)}")
        raise

def construct_workbook_url(workbook_uri: str) -> str:
    """
    Construct the full workbook URL from the URI.
    
    Args:
        workbook_uri (str): The workbook URI from the raw data
        
    Returns:
        str: Complete workbook URL
    """
    try:
        if not workbook_uri:
            logger.warning(f"[{MODULE_NAME}] Empty workbook URI provided")
            return ""
            
        server_config = get_server_config()
        workbook_id = str(workbook_uri).split("/")[3]
        return f"{server_config['url']}/#/site/{server_config['site_name']}/workbooks/{workbook_id}/views"
    except (IndexError, KeyError) as e:
        logger.error(f"[{MODULE_NAME}] Failed to construct workbook URL: {str(e)}")
        return ""

def validate_workbook_data(workbook_info: Dict[str, Any]) -> bool:
    """
    Validate workbook data for required fields.
    
    Args:
        workbook_info (Dict[str, Any]): Workbook information dictionary
        
    Returns:
        bool: True if valid, False otherwise
    """
    required_fields = ['id', 'name']
    return all(workbook_info.get(field) for field in required_fields)

def get_workbooks(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extracts workbook information from the provided raw JSON data.

    Args:
        raw_data (Dict[str, Any]): Dictionary containing workbook data.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries containing workbook information

    Raises:
        ValueError: If raw_data is None or empty
        KeyError: If required data structure is missing
        Exception: For unexpected errors during parsing
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting workbook extraction process")

    if not raw_data:
        logger.error(f"[{MODULE_NAME}] Raw data is empty or None")
        raise ValueError("Raw data cannot be empty or None")

    try:
        workbooks_list = []
        all_workbooks_raw = raw_data.get('data', {}).get('workbooks', [])
        
        if not all_workbooks_raw:
            logger.warning(f"[{MODULE_NAME}] No workbooks found in raw data")
            return workbooks_list

        for workbook_raw in all_workbooks_raw:
            try:
                workbook_info = {
                    'id': workbook_raw.get('id', ''),
                    'name': workbook_raw.get('name', ''),
                    'description': workbook_raw.get('description', ''),
                    'owner_id': workbook_raw.get('owner', {}).get('id', ''),
                    'project_id': workbook_raw.get('projectName', ''),
                    'created_at': workbook_raw.get('createdAt', ''),
                    'updated_at': workbook_raw.get('updatedAt', ''),
                    'url': construct_workbook_url(workbook_raw.get('uri', ''))
                }
                
                if not validate_workbook_data(workbook_info):
                    logger.warning(f"[{MODULE_NAME}] Skipping workbook with missing required fields: {workbook_info}")
                    continue
                    
                workbooks_list.append(workbook_info)
                logger.debug(f"[{MODULE_NAME}] Found workbook: {workbook_info['name']} in project {workbook_info['project_id']}")

            except Exception as e:
                logger.error(f"[{MODULE_NAME}] Error processing workbook: {str(e)}")
                continue

        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{MODULE_NAME}] Successfully processed {len(workbooks_list)} workbooks in {processing_time:.2f} seconds")
        return workbooks_list

    except Exception as e:
        logger.error(f"[{MODULE_NAME}] An unexpected error occurred while fetching workbooks: {str(e)}")
        raise

    

if __name__ == "__main__":
    raw_data_path_for_test = root_folder / "sample_data" / "data_test.json"
    logger.info(f"[{MODULE_NAME}] Starting test execution with file: {raw_data_path_for_test}")

    try:
        with open(raw_data_path_for_test, 'r') as f:
            raw_data = json.load(f)

        dummy_workbooks = get_workbooks(raw_data=raw_data)

        for wb in dummy_workbooks:
            logger.info(f"[{MODULE_NAME}] Workbook found - ID: {wb['id']}, Name: {wb['name']}, Project: {wb['project_id']}, URL: {wb['url']}")

    except FileNotFoundError:
        logger.error(f"[{MODULE_NAME}] Dummy data file not found at {raw_data_path_for_test}")
    except json.JSONDecodeError:
        logger.error(f"[{MODULE_NAME}] Error decoding JSON from {raw_data_path_for_test}")
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Error fetching dummy data: {str(e)}")