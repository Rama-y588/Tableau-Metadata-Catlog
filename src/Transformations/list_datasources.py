from datetime import datetime
from typing import List, Dict, Any, Optional
from src.utils.logger import app_logger as logger

def parse_datetime(date_str: Optional[str]) -> Optional[datetime]:
    """
    Parse ISO 8601 date string into a datetime object.

    Args:
        date_str (Optional[str]): Date string in ISO 8601 format.

    Returns:
        Optional[datetime]: Parsed datetime object or None if invalid.
    """
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except ValueError as e:
        logger.warning(f"Invalid datetime format: {date_str} - {e}")
        return None

def get_datasources(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and transform datasource information from Tableau workbook data.

    Processes both upstream and embedded datasources and returns a unified list
    of datasource metadata including their role in the data pipeline.

    Args:
        raw_data (Dict[str, Any]): Raw JSON object containing Tableau workbook data.

    Returns:
        List[Dict[str, Any]]: A list of datasource metadata dictionaries.
    """
    datasources: List[Dict[str, Any]] = []

    try:
        workbooks = raw_data.get('data', {}).get('workbooks', [])
        logger.info(f"Processing {len(workbooks)} workbook(s)...")

        for wb_index, workbook in enumerate(workbooks):
            logger.debug(f"Processing workbook {wb_index + 1}: {workbook.get('name', 'Unnamed')}")

            for ds in workbook.get('upstreamDatasources', []):
                datasource_id = ds.get('id')
                datasources.append({
                    'id': datasource_id,
                    'name': ds.get('name') or None,
                    'uri': ds.get('uri') or None,
                    'has_extracts': ds.get('hasExtracts', False),
                    'extract_last_refresh_time': parse_datetime(ds.get('extractLastRefreshTime')),
                    'type': 'upstream',
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                })

            for ds in workbook.get('embeddedDatasources', []):
                datasource_id = ds.get('id')
                datasources.append({
                    'id': datasource_id,
                    'name': ds.get('name') or None,
                    'uri': None,
                    'has_extracts': ds.get('hasExtracts', False),
                    'extract_last_refresh_time': parse_datetime(ds.get('extractLastRefreshTime')),
                    'type': 'embedded',
                    'created_at': datetime.now(),
                    'updated_at': datetime.now(),
                })

    except Exception as e:
        logger.exception(f"Failed to extract datasources: {e}")
        raise

    logger.info(f"Extracted {len(datasources)} datasource(s).")
    return datasources


# --- Testing ---
if __name__ == "__main__":
    import json
    from pathlib import Path

    logger.info("\n--- Extracting ONLY Datasources from Raw Data ---")

    try:
        root_folder = Path(__file__).resolve().parent.parent.parent  # Adjust if necessary
        test_data_path = root_folder / "sample_data" / "data_test.json"

        if not test_data_path.exists():
            logger.error(f"Test data file not found at {test_data_path}")
            exit(1)

        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)

        datasources = get_datasources(raw_data)

        for ds in datasources:
            print(ds)

    except Exception as e:
        logger.exception(f"Error during test execution: {e}")

