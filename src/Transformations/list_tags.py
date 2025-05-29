from datetime import datetime
from typing import List, Dict, Any
from src.utils.logger import app_logger as logger

# Define module name for logging (using file name)
MODULE_NAME = "list_tags.py"

def get_tags(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and transform tag data from Tableau workbook data.

    Args:
        raw_data (Dict[str, Any]): Raw JSON object containing Tableau workbook data.

    Returns:
        List[Dict[str, Any]]: A list of tag metadata dictionaries with only id and name.
    """
    tags = set()

    try:
        workbooks = raw_data.get('data', {}).get('workbooks', [])
        logger.info(f"[{MODULE_NAME}] Processing tags for {len(workbooks)} workbook(s)...")

        for workbook in workbooks:
            for tag in workbook.get('tags', []):
                tag_id = tag.get('id')
                tag_name = tag.get('name')
                
                if tag_id and tag_name:
                    tags.add((
                        tag_id,
                        tag_name
                    ))

        # Convert set of tuples to list of dictionaries with only id and name
        tag_records = [{
            'id': tag[0],
            'name': tag[1]
        } for tag in tags]

        logger.info(f"[{MODULE_NAME}] Extracted {len(tag_records)} unique tags.")
        return tag_records

    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Failed to extract tags: {str(e)}", exc_info=True)
        raise


if __name__ == "__main__":
    import json
    from pathlib import Path

    logger.info(f"[{MODULE_NAME}] --- Extracting Tags from Raw Data ---")

    try:
        root_folder = Path(__file__).resolve().parents[2]
        test_data_path = root_folder / "sample_data" / "data_test.json"

        if not test_data_path.exists():
            logger.error(f"[{MODULE_NAME}] Test data file not found at {test_data_path}")
            exit(1)

        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)

        tags = get_tags(raw_data)

        # Print tags in a formatted way
        for tag in tags:
            logger.info(f"[{MODULE_NAME}] Tag - ID: {tag['id']}, Name: {tag['name']}")

    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Error during test execution: {str(e)}", exc_info=True)