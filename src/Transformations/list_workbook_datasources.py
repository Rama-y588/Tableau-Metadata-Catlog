from typing import List, Dict, Any
from datetime import datetime
from pathlib import Path
import json
from src.utils.logger import app_logger as logger

MODULE_NAME = "list_workbook_datasources"

def get_workbook_datasources(raw_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Extract workbook-datasource relationships from raw data.
    Only includes workbook_id and datasource_id pairs.

    Args:
        raw_data (Dict[str, Any]): Raw JSON object containing Tableau workbook data.

    Returns:
        List[Dict[str, str]]: List of dictionaries with workbook_id and datasource_id pairs.
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting workbook-datasource relationship extraction")
    
    try:
        relationships = []
        processed_relationships = set()  # Track unique relationships

        workbooks = raw_data.get("data", {}).get("workbooks", [])
        if not workbooks:
            logger.warning(f"[{MODULE_NAME}] No workbooks found in data")
            return []

        logger.info(f"[{MODULE_NAME}] Processing {len(workbooks)} workbooks for datasource relationships")

        for workbook in workbooks:
            workbook_id = workbook.get("id")
            if not workbook_id:
                continue

            # Process upstream datasources
            for ds in workbook.get("upstreamDatasources", []):
                datasource_id = ds.get("id")
                if not datasource_id:
                    continue

                # Create unique relationship key
                relationship_key = f"{workbook_id}:{datasource_id}"
                if relationship_key not in processed_relationships:
                    relationships.append({
                        "workbook_id": workbook_id.strip(),
                        "datasource_id": datasource_id.strip()
                    })
                    processed_relationships.add(relationship_key)

            # Process embedded datasources
            for ds in workbook.get("embeddedDatasources", []):
                datasource_id = ds.get("id")
                if not datasource_id:
                    continue

                # Create unique relationship key
                relationship_key = f"{workbook_id}:{datasource_id}"
                if relationship_key not in processed_relationships:
                    relationships.append({
                        "workbook_id": workbook_id.strip(),
                        "datasource_id": datasource_id.strip()
                    })
                    processed_relationships.add(relationship_key)

        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{MODULE_NAME}] Created {len(relationships)} workbook-datasource relationships in {processing_time:.2f} seconds")
            
        return relationships

    except Exception as e:
        logger.exception(f"[{MODULE_NAME}] Error processing workbook-datasource relationships: {str(e)}")
        return []

def main() -> None:
    """
    Main execution function for standalone script usage.
    """
    logger.info(f"[{MODULE_NAME}] Script execution started")
    
    try:
        # Get the root folder path
        root_folder = Path(__file__).resolve().parents[2]
        test_data_path = root_folder / "sample_data" / "data_test.json"

        if not test_data_path.exists():
            raise FileNotFoundError(f"Test data file not found at {test_data_path}")

        # Load test data
        logger.info(f"[{MODULE_NAME}] Loading test data from: {test_data_path}")
        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)

        # Process relationships
        relationships = get_workbook_datasources(raw_data)
        
        # Log results
        logger.info(f"[{MODULE_NAME}] Processing complete. Found {len(relationships)} relationships:")
        for rel in relationships:
            logger.info(f"[{MODULE_NAME}] {rel}")

    except FileNotFoundError as e:
        logger.error(f"[{MODULE_NAME}] {str(e)}")
    except json.JSONDecodeError as e:
        logger.error(f"[{MODULE_NAME}] Invalid JSON in test data file: {str(e)}")
    except Exception as e:
        logger.exception(f"[{MODULE_NAME}] Unexpected error: {str(e)}")
    finally:
        logger.info(f"[{MODULE_NAME}] Script execution finished")

if __name__ == "__main__":
    main()