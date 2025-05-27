from src.utils.logger import app_logger as logger
from typing import List, Dict, Any
import json
from pathlib import Path

def get_workbook_datasource(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extracts intermediary mapping of workbook ID to associated datasource IDs.

    Args:
        raw_data (Dict[str, Any]): Raw JSON object containing Tableau workbook data.

    Returns:
        List[Dict[str, Any]]: A list of workbook-datasource ID mappings.
    """
    workbook_datasources: List[Dict[str, Any]] = []

    try:
        workbooks = raw_data.get('data', {}).get('workbooks', [])
        logger.info(f"Mapping datasources for {len(workbooks)} workbook(s)...")

        for workbook in workbooks:
            workbook_id = workbook.get('id')

            for ds in workbook.get('upstreamDatasources', []):
                ds_id = ds.get('id')
                if workbook_id and ds_id:
                    workbook_datasources.append({
                        'workbook_id': workbook_id,
                        'datasource_id': ds_id
                    })

            for ds in workbook.get('embeddedDatasources', []):
                ds_id = ds.get('id')
                if workbook_id and ds_id:
                    workbook_datasources.append({
                        'workbook_id': workbook_id,
                        'datasource_id': ds_id
                    })

    except Exception as e:
        logger.exception(f"Failed to map workbook to datasources: {e}")
        raise

    logger.info(f"Mapped {len(workbook_datasources)} workbook-datasource relationships.")
    return workbook_datasources

if __name__ == "__main__":
    try:
        root_folder = Path(__file__).resolve().parents[2]
        test_data_path = root_folder / "sample_data" / "data_test.json"
        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)
            
        workbook_datasources = get_workbook_datasource(raw_data)
        for w_d in workbook_datasources:
            print(w_d)
    except Exception as e:
        logger.exception(f"Error: {e}")
