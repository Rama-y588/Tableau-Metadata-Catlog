import json
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any, Optional

# Assuming app_logger is correctly configured and available
from src.utils.logger import app_logger as logger

# --- Define root folder for standalone execution ---
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]

# --- Helper function ---
def _parse_datetime(dt_string: Optional[str]) -> Optional[datetime]:
    """
    Parses ISO datetime strings with support for 'Z' (UTC).
    Returns None if parsing fails.
    """
    if dt_string:
        try:
            return datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
        except ValueError:
            logger.error(f"Invalid datetime format: '{dt_string}'")
    return None

# --- Core function ---
def get_dashboard_views(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extracts 'Dashboard' type views from Tableau raw workbook JSON.

    Args:
        raw_data (Dict[str, Any]): JSON-like dictionary containing 'data' > 'workbooks'.

    Returns:
        List[Dict[str, Any]]: List of dashboard views with metadata.

    Raises:
        ValueError: If input structure is malformed.
        Exception: For unexpected runtime errors.
    """
    logger.info("Extracting dashboard views from raw data...")
    dashboard_views: List[Dict[str, Any]] = []

    try:
        data_root = raw_data.get('data', {})
        workbooks = data_root.get('workbooks', [])

        if not isinstance(workbooks, list):
            raise ValueError("'workbooks' should be a list.")

        for workbook in workbooks:
            workbook_id = workbook.get('id')
            workbook_name = workbook.get('name')

            for view in workbook.get('views', []):
                if view.get('__typename') == 'Dashboard':
                    dashboard_views.append({
                        'id': view.get('id'),
                        'name': view.get('name'),
                        'workbook_id': workbook_id,
                        'workbook_name': workbook_name,
                        'path': view.get('path'),
                        'type': view.get('__typename'),
                        'created_at': _parse_datetime(view.get('createdAt')),
                        'updated_at': _parse_datetime(view.get('updatedAt')),
                    })
                    logger.debug(f"Found Dashboard: '{view.get('name')}' in workbook: '{workbook_name}'")

        logger.info(f"Extracted {len(dashboard_views)} dashboard views.")
        return dashboard_views

    except Exception as e:
        logger.error(f"Error extracting dashboard views: {e}", exc_info=True)
        if isinstance(e, (AttributeError, TypeError)):
            raise ValueError("Malformed input: expected dictionary with nested 'data' > 'workbooks'.") from e
        raise

# --- Testing ---
if __name__ == "__main__":
    print("\n--- Extracting ONLY Dashboard Views from Raw Data ---")
    
    test_data_path = root_folder / "sample_data" / "data_test.json"
    with open(test_data_path, 'r') as f:
        raw_data = json.load(f)

    dashboard_views = get_dashboard_views(raw_data)
    for view in dashboard_views:
        created = view['created_at'].isoformat() if view['created_at'] else "N/A"
        print(f"Dashboard ID: {view['id']}, Name: {view['name']}, Workbook: {view['workbook_name']}, Created: {created}")

    
