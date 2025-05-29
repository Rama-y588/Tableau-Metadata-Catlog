from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path
import json
from src.utils.logger import app_logger as logger

current_file = Path(__file__).resolve()
file_name = current_file.name
MODULE_NAME = "list_workbook_views"

def get_workbook_views(raw_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Extract workbook-view relationships from raw data.
    Only includes workbook_id and view_id pairs.

    Args:
        raw_data (Dict[str, Any]): Raw JSON object containing Tableau workbook data.

    Returns:
        List[Dict[str, str]]: List of dictionaries with workbook_id and view_id pairs.
    """
    start_time = datetime.now()
    logger.info(f"[{file_name}] Starting workbook-view relationship extraction")
    
    try:
        relationships = []
        processed_relationships = set()  # Track unique relationships

        workbooks = raw_data.get("data", {}).get("workbooks", [])
        if not workbooks:
            logger.warning(f"[{file_name}] No workbooks found in data")
            return []

        logger.info(f"[{file_name}] Processing {len(workbooks)} workbooks for view relationships")

        for workbook in workbooks:
            workbook_id = workbook.get("id")
            if not workbook_id:
                logger.warning(f"[{file_name}] Skipping workbook with missing ID")
                continue

            # Process views
            for view in workbook.get("views", []):
                view_id = view.get("id")
                if not view_id:
                    logger.warning(f"[{file_name}] Skipping view with missing ID in workbook {workbook_id}")
                    continue

                # Create unique relationship key
                relationship_key = f"{workbook_id}:{view_id}"
                if relationship_key not in processed_relationships:
                    relationships.append({
                        "workbook_id": workbook_id.strip(),
                        "view_id": view_id.strip()
                    })
                    processed_relationships.add(relationship_key)
                    logger.debug(f"[{file_name}] Created relationship: {workbook_id} -> {view_id}")

        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{file_name}] Created {len(relationships)} workbook-view relationships in {processing_time:.2f} seconds")
            
        return relationships

    except Exception as e:
        logger.exception(f"[{file_name}] Error processing workbook-view relationships: {str(e)}")
        return []

def main() -> None:
    """
    Main execution function for standalone script usage.
    """
    logger.info(f"[{file_name}] Script execution started")
    
    try:
        # Get the root folder path
        root_folder = Path(__file__).resolve().parents[2]
        test_data_path = root_folder / "sample_data" / "data_test.json"

        if not test_data_path.exists():
            raise FileNotFoundError(f"Test data file not found at {test_data_path}")

        # Load test data
        logger.info(f"[{file_name}] Loading test data from: {test_data_path}")
        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)

        # Process relationships
        relationships = get_workbook_views(raw_data)
        
        # Log results
        logger.info(f"[{file_name}] Processing complete. Found {len(relationships)} relationships:")
        for rel in relationships:
            logger.info(f"[{file_name}] {rel}")

    except FileNotFoundError as e:
        logger.error(f"[{file_name}] {str(e)}")
    except json.JSONDecodeError as e:
        logger.error(f"[{file_name}] Invalid JSON in test data file: {str(e)}")
    except Exception as e:
        logger.exception(f"[{file_name}] Unexpected error: {str(e)}")
    finally:
        logger.info(f"[{file_name}] Script execution finished")

if __name__ == "__main__":
    main()