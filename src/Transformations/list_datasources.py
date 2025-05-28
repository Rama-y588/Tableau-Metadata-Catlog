import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

from src.utils.logger import app_logger as logger

# --- Define root folder for standalone execution ---
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]

# Define module name for logging using current file name
MODULE_NAME = current_file.name  # This will be 'list_datasources.py'

def _parse_datetime(dt_string: Optional[str]) -> Optional[datetime]:
    """
    Parses ISO datetime strings with support for 'Z' (UTC).
    Returns None if parsing fails.
    """
    if dt_string:
        try:
            return datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
        except ValueError:
            logger.error(f"[{MODULE_NAME}] Invalid datetime format: '{dt_string}'")
    return None

def get_datasources(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extracts datasource information from Tableau raw workbook JSON.

    Args:
        raw_data (Dict[str, Any]): JSON-like dictionary containing 'data' > 'workbooks'.

    Returns:
        List[Dict[str, Any]]: List of datasources with metadata.

    Raises:
        ValueError: If input structure is malformed.
        Exception: For unexpected runtime errors.
    """
    logger.info(f"[{MODULE_NAME}] Starting datasources extraction process")
    datasources: List[Dict[str, Any]] = []

    try:
        data_root = raw_data.get('data', {})
        workbooks = data_root.get('workbooks', [])

        if not isinstance(workbooks, list):
            raise ValueError("'workbooks' should be a list.")

        logger.info(f"[{MODULE_NAME}] Processing {len(workbooks)} workbooks")
        
        for workbook in workbooks:
            workbook_id = workbook.get('id')
            logger.debug(f"[{MODULE_NAME}] Processing workbook ID: {workbook_id}")

            # Process upstream datasources
            for ds in workbook.get('upstreamDatasources', []):
                datasource_id = ds.get('id')
                datasource_name = ds.get('name')
                
                datasources.append({
                    'id': datasource_id,
                    'name': datasource_name,
                    'workbook_id': workbook_id,
                    'path': ds.get('path', ''),
                    'type': 'upstream',
                    'has_extracts': ds.get('hasExtracts', False),
                    'extract_last_refresh_time': _parse_datetime(ds.get('extractLastRefreshTime')),
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                })
                logger.debug(f"[{MODULE_NAME}] Found Upstream Datasource: '{datasource_name}'")

            # Process embedded datasources
            for ds in workbook.get('embeddedDatasources', []):
                datasource_id = ds.get('id')
                datasource_name = ds.get('name')
                
                datasources.append({
                    'id': datasource_id,
                    'name': datasource_name,
                    'workbook_id': workbook_id,
                    'path': ds.get('path', ''),
                    'type': 'embedded',
                    'has_extracts': ds.get('hasExtracts', False),
                    'extract_last_refresh_time': _parse_datetime(ds.get('extractLastRefreshTime')),
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                })
                logger.debug(f"[{MODULE_NAME}] Found Embedded Datasource: '{datasource_name}'")

        logger.info(f"[{MODULE_NAME}] Successfully extracted {len(datasources)} datasources")
        return datasources

    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Error extracting datasources: {str(e)}", exc_info=True)
        if isinstance(e, (AttributeError, TypeError)):
            raise ValueError("Malformed input: expected dictionary with nested 'data' > 'workbooks'.") from e
        raise

# --- Testing ---
if __name__ == "__main__":
    logger.info(f"[{MODULE_NAME}] Starting datasources module test")
    
    try:
        test_data_path = root_folder / "sample_data" / "data_test.json"
        logger.debug(f"[{MODULE_NAME}] Loading test data from: {test_data_path}")
        
        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)

        # Get datasources data
        datasources_data = get_datasources(raw_data)
        logger.info(f"[{MODULE_NAME}] Found {len(datasources_data)} datasources")
        
        # Log sample of datasources
        for ds in datasources_data[:3]:
            logger.info(f"[{MODULE_NAME}] Sample Datasource:")
            logger.info(f"[{MODULE_NAME}]   ID: {ds['id']}")
            logger.info(f"[{MODULE_NAME}]   Name: {ds['name']}")
            logger.info(f"[{MODULE_NAME}]   Type: {ds['type']}")
            logger.info(f"[{MODULE_NAME}]   Has Extracts: {ds['has_extracts']}")
            logger.info(f"[{MODULE_NAME}]   Last Refresh: {ds['extract_last_refresh_time']}")
            
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Error during test execution: {str(e)}", exc_info=True)
    finally:
        logger.info(f"[{MODULE_NAME}] Datasources module test completed")