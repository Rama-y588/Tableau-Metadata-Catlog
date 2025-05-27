from datetime import datetime
from typing import List, Dict, Any
from src.utils.logger import app_logger as logger

def get_tags(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract and transform tag data from Tableau workbook data.

    Args:
        raw_data (Dict[str, Any]): Raw JSON object containing Tableau workbook data.

    Returns:
        List[Dict[str, Any]]: A list of tag metadata dictionaries.
    """
    tags = set()
    workbook_tag_relations = []

    try:
        workbooks = raw_data.get('data', {}).get('workbooks', [])
        logger.info(f"Processing tags for {len(workbooks)} workbook(s)...")

        for workbook in workbooks:
            for tag in workbook.get('tags', []):
                tag_id = tag.get('id')
                tag_name = tag.get('name')
                
                if tag_id and tag_name:
                    tags.add((
                        tag_id,
                        tag_name
                    ))
                    workbook_tag_relations.append((
                        workbook.get('id'),
                        tag_id
                    ))

        # Convert set of tuples to list of dictionaries
        tag_records = [{
            'id': tag[0],
            'name': tag[1],
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        } for tag in tags]

        logger.info(f"Extracted {len(tag_records)} unique tags.")
        return tag_records

    except Exception as e:
        logger.exception(f"Failed to extract tags: {e}")
        raise


if __name__ == "__main__":
    import json
    from pathlib import Path

    logger.info("\n--- Extracting Tags from Raw Data ---")

    try:
        root_folder = Path(__file__).resolve().parents[2]
        test_data_path = root_folder / "sample_data" / "data_test.json"

        if not test_data_path.exists():
            logger.error(f"Test data file not found at {test_data_path}")
            exit(1)

        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)

        tags = get_tags(raw_data)

        for tag in tags:
            print(tag)

    except Exception as e:
        logger.exception(f"Error during test execution: {e}")
