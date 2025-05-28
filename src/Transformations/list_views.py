import json
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional
from urllib.parse import quote
from src.utils.logger import app_logger as logger

# --- Define root folder for standalone execution ---
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]

# Define module name for logging
MODULE_NAME = current_file.name

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

def construct_view_url(view_path: str, site_name: str) -> str:
    """
    Constructs a Tableau view URL using the view path and site name.
    
    Args:
        view_path (str): Path of the view
        site_name (str): Name of the Tableau site
        
    Returns:
        str: Constructed view URL
    """
    try:
        # Construct the URL
        view_url = f"https://{site_name}/#/views/{view_path}"
        
        logger.debug(f"[{MODULE_NAME}] Constructed view URL: {view_url}")
        return view_url
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Error constructing view URL: {str(e)}", exc_info=True)
        return ""

def get_views(raw_data: Dict[str, Any], site_name: str) -> List[Dict[str, Any]]:
    """
    Extracts only 'Dashboard' type views from Tableau raw workbook JSON.

    Args:
        raw_data (Dict[str, Any]): JSON-like dictionary containing 'data' > 'workbooks'.
        site_name (str): Name of the Tableau site for URL construction

    Returns:
        List[Dict[str, Any]]: List of dashboard views with metadata.

    Raises:
        ValueError: If input structure is malformed.
        Exception: For unexpected runtime errors.
    """
    logger.info(f"[{MODULE_NAME}] Starting dashboard views extraction process")
    dashboard_views: List[Dict[str, Any]] = []

    try:
        data_root = raw_data.get('data', {})
        workbooks = data_root.get('workbooks', [])

        if not isinstance(workbooks, list):
            raise ValueError("'workbooks' should be a list.")

        logger.info(f"[{MODULE_NAME}] Processing {len(workbooks)} workbooks")
        
        for workbook in workbooks:
            workbook_id = workbook.get('id')
            workbook_name = workbook.get('name')
            logger.debug(f"[{MODULE_NAME}] Processing workbook: {workbook_name}")

            for view in workbook.get('views', []):
                if view.get('__typename') == 'Dashboard':
                    view_name = view.get('name')
                    view_path = view.get('path', '')
                    view_url = construct_view_url(view_path, site_name)
                    
                    dashboard_views.append({
                        'id': view.get('id'),
                        'name': view_name,
                        'workbook_id': workbook_id,
                        'workbook_name': workbook_name,
                        'path': view_path,
                        'type': 'Dashboard',
                        'created_at': _parse_datetime(view.get('createdAt')),
                        'updated_at': _parse_datetime(view.get('updatedAt')),
                        'url': view_url
                    })
                    logger.debug(f"[{MODULE_NAME}] Found Dashboard: '{view_name}' in workbook: '{workbook_name}'")

        logger.info(f"[{MODULE_NAME}] Successfully extracted {len(dashboard_views)} dashboard views")
        return dashboard_views

    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Error extracting dashboard views: {str(e)}", exc_info=True)
        if isinstance(e, (AttributeError, TypeError)):
            raise ValueError("Malformed input: expected dictionary with nested 'data' > 'workbooks'.") from e
        raise

# --- Testing ---
if __name__ == "__main__":
    logger.info(f"[{MODULE_NAME}] Starting views module test")
    
    try:
        test_data_path = root_folder / "sample_data" / "data_test.json"
        logger.debug(f"[{MODULE_NAME}] Loading test data from: {test_data_path}")
        
        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)

        # Test site name
        site_name = "tableau.example.com"  # Replace with your actual site name
        logger.info(f"[{MODULE_NAME}] Using site name: {site_name}")

        # Get dashboard views
        dashboard_views = get_views(raw_data, site_name)
        logger.info(f"[{MODULE_NAME}] Found {len(dashboard_views)} dashboard views")
        
        # Log sample of views
        for view in dashboard_views[:3]:
            logger.info(f"[{MODULE_NAME}] Sample Dashboard:")
            logger.info(f"[{MODULE_NAME}]   ID: {view['id']}")
            logger.info(f"[{MODULE_NAME}]   Name: {view['name']}")
            logger.info(f"[{MODULE_NAME}]   Workbook: {view['workbook_name']}")
            logger.info(f"[{MODULE_NAME}]   URL: {view['url']}")
            logger.info(f"[{MODULE_NAME}]   Created: {view['created_at']}")
            
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Error during test execution: {str(e)}", exc_info=True)
    finally:
        logger.info(f"[{MODULE_NAME}] Views module test completed")