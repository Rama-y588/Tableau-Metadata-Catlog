import json
import logging
from pathlib import Path
from src.utils.logger import app_logger as logger
from typing import List, Dict, Any

# === Configuration ===
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]
file_name = Path(__file__).stem  # Dynamically gets the filename

def get_datasource_connections(data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Extracts datasource IDs and their associated connection types with detailed logging.
    """
    logger.info(f"{file_name}  Starting extraction of datasource connections")
    datasource_connections = []

    try:
        workbooks = data.get("data", {}).get("workbooks", [])
        logger.info(f"{file_name}  Found {len(workbooks)} workbooks in total")

        for wb_index, workbook in enumerate(workbooks):
            workbook_id = workbook.get("id", "UNKNOWN_ID")
            logger.debug(
                f"{file_name}  Processing workbook #{wb_index + 1} "
                f"(ID: {workbook_id})"
            )

            for ds_type in ["upstreamDatasources", "embeddedDatasources"]:
                datasources = workbook.get(ds_type, [])
                logger.debug(
                    f"{file_name}   - Processing {ds_type} "
                    f"({len(datasources)} datasources)"
                )

                for ds_index, ds in enumerate(datasources):
                    datasource_id = ds.get("id")
                    logger.debug(
                        f"{file_name}    > Datasource #{ds_index + 1} "
                        f"(Type: {ds_type.split('Datasources')[0]}, "
                        f"ID: {datasource_id or 'MISSING_ID'})"
                    )

                    if not datasource_id:
                        logger.warning(
                            f"{file_name}  Missing datasource ID in {ds_type} "
                            f"of workbook {workbook_id}"
                        )
                        continue

                    databases = ds.get("upstreamDatabases", [])
                    logger.debug(
                        f"{file_name}     - Found {len(databases)} "
                        f"upstream databases"
                    )

                    for db_index, db in enumerate(databases):
                        connection_id = db.get("connectionType")
                        logger.debug(
                            f"{file_name}      - Database #{db_index + 1}: "
                            f"Connection type: {connection_id or 'MISSING'}"
                        )

                        if connection_id:
                            datasource_connections.append({
                                "datasource_id": datasource_id,
                                "connection_id": connection_id
                            })
                            logger.info(
                                f"{file_name}   +++ Added connection "
                                f"(DS: {datasource_id}, Conn: {connection_id})"
                            )
                        else:
                            logger.warning(
                                f"{file_name}  Missing connectionType in "
                                f"datasource {datasource_id}, "
                                f"workbook {workbook_id}"
                            )

        logger.info(
            f"{file_name}  Extraction complete. "
            f"Found {len(datasource_connections)} valid connections."
        )
        return datasource_connections

    except Exception as e:
        logger.exception(
            f"{file_name}  Error during extraction: {str(e)}"
        )
        raise

# --- Main Execution ---
if __name__ == "__main__":
    try:
        test_data_path = root_folder / "sample_data" / "data_test.json"
        logger.info(f"{file_name}  Loading test data from: {test_data_path}")

        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)
        
        logger.debug(
            f"{file_name}  Test data structure: {json.dumps(raw_data, indent=2)[:500]}..."
        )
        
        logger.info(f"{file_name}  Starting connection extraction process")
        results = get_datasource_connections(raw_data)

        logger.info(f"{file_name}  Final results:")
        for entry in results:
            logger.info(
                f"{file_name}  - Datasource: {entry['datasource_id']} "
                f"| Connection: {entry['connection_id']}"
            )

    except FileNotFoundError as e:
        logger.critical(f"{file_name}  Critical file error: {e}")
    except json.JSONDecodeError as e:
        logger.error(f"{file_name}  JSON parsing failed: {e.doc[:100]}...")
    except Exception as e:
        logger.exception(f"{file_name}  Fatal error in main execution: {e}")
