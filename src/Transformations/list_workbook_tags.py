from typing import List, Dict, Any
from datetime import datetime
from src.utils.logger import app_logger as logger
import json
from pathlib import Path


def get_workbook_tags(raw_data: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract unique tags and workbook-tag relationships from self.data.

    Returns:
        List of tag records with id, name, created_at, and updated_at.
    """
    tags = set()
    workbook_tag_relations = []

    workbooks = raw_data.get('data', {}).get('workbooks', [])
    if not isinstance(workbooks, list):
        # Defensive fallback for malformed data
        workbook_tag_relations = []
        return []

    for workbook in workbooks:
        workbook_id = workbook.get('id')
        if not workbook_id:
            continue  # Skip invalid workbook entries

        tags_list = workbook.get('tags', [])
        if not isinstance(tags_list, list):
            continue  # Defensive: skip if tags is not a list

        for tag in tags_list:
            tag_id = tag.get('id')
            workbook_tag_relations.append({
                'workbook_id': workbook_id,
                'tag_id': tag_id
            })


    return workbook_tag_relations


if __name__ == "__main__":
    root_folder = Path(__file__).resolve().parents[2]
    test_data_path = root_folder / "sample_data" / "data_test.json"
    
    with open(test_data_path, 'r') as f:
        raw_data = json.load(f) 
        #print(raw_data)
    workbook_tag_relations = get_workbook_tags(raw_data)
    for w_t in workbook_tag_relations:
        print(w_t)


