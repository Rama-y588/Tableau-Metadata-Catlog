import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from src.Transformations.list_users import get_users
from src.Transformations.list_workbooks import get_workbooks
from src.Transformations.list_views import get_views
from src.Transformations.list_tags import get_tags
from src.Transformations.list_connection import get_connections
from src.Transformations.list_datasources import get_datasources
from src.Transformations.list_workbook_views import get_workbook_views
from src.Transformations.list_workbook_tags import get_workbook_tags
from src.Transformations.list_workbook_datasources import get_workbook_datasources
from src.Transformations.list_datasources_connections import get_datasource_connections
from src.generate_csv.generate_users_csv import generate_users_csv_from_config
from src.generate_csv.generate_workbooks_csv import generate_workbooks_csv_from_config
from src.generate_csv.generate_views_csv import generate_views_csv_from_config
from src.generate_csv.generate_tags_csv import generate_tags_csv_from_config
from src.generate_csv.generate_connections_csv import generate_connection_csv_from_config
from src.generate_csv.generate_datasources_csv import generate_datasources_csv_from_config
from src.generate_csv.generate_workbooks_views_csv import generate_workbook_views_csv_from_config
from src.generate_csv.generate_workbooks_tags_csv import generate_workbook_tags_csv_from_config
from src.generate_csv.generate_workbooks_datasources_csv import generate_workbook_datasources_csv_from_config
from src.generate_csv.generate_datasource_connections_csv import generate_datasource_connections_csv_from_config

from src.utils.helper import load_YAML_config
from src.utils.logger import app_logger as logger
import json

MODULE_NAME = "transform_all"

def transform_all(raw_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Orchestrates the entire transformation process:
    1. Process users data and generate users CSV
    2. Process workbooks data and generate workbooks CSV
    3. Process views data and generate views CSV
    4. Process tags data and generate tags CSV
    5. Process connections data and generate connections CSV
    6. Process datasources data and generate datasources CSV
    7. Process workbook-views relationships and generate CSV
    8. Process workbook-tags relationships and generate CSV
    9. Process workbook-datasources relationships and generate CSV
    
    Args:
        raw_data (Dict[str, Any]): Raw data containing all Tableau information
        
    Returns:
        Dict[str, str]: Status of each transformation step
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting complete transformation process")
    results = {
    "users_status": "Failed",
    "workbooks_status": "Failed",
    "views_status": "Failed",
    "tags_status": "Failed",
    "connections_status": "Failed",
    "datasources_status": "Failed",
    "workbook_views_status": "Failed",
    "workbook_tags_status": "Failed",
    "workbook_datasources_status": "Failed",
    "datasource_connections_status": "Failed",  
    "overall_status": "Failed"
}
    
    try:
        # Step 1: Process Users
        logger.info(f"[{MODULE_NAME}] Starting users transformation")
        users_data = get_users(raw_data)
        if users_data:
            logger.info(f"[{MODULE_NAME}] Found {len(users_data)} users")
            users_status = generate_users_csv_from_config(users_data)
            results["users_status"] = users_status["status"] if isinstance(users_status, dict) else users_status
            logger.info(f"[{MODULE_NAME}] Users transformation completed with status: {results['users_status']}")
        else:
            logger.warning(f"[{MODULE_NAME}] No users data found")
        
        # Step 2: Process Workbooks
        logger.info(f"[{MODULE_NAME}] Starting workbooks transformation")
        workbooks_data = get_workbooks(raw_data)
        if workbooks_data:
            logger.info(f"[{MODULE_NAME}] Found {len(workbooks_data)} workbooks")
            workbooks_status = generate_workbooks_csv_from_config(workbooks_data)
            results["workbooks_status"] = workbooks_status["status"] if isinstance(workbooks_status, dict) else workbooks_status
            logger.info(f"[{MODULE_NAME}] Workbooks transformation completed with status: {results['workbooks_status']}")
        else:
            logger.warning(f"[{MODULE_NAME}] No workbooks data found")

        # Step 3: Process Views
        logger.info(f"[{MODULE_NAME}] Starting views transformation")
        views_data = get_views(raw_data)
        if views_data:
            logger.info(f"[{MODULE_NAME}] Found {len(views_data)} views")
            views_status = generate_views_csv_from_config(views_data)
            results["views_status"] = views_status["status"] if isinstance(views_status, dict) else views_status
            logger.info(f"[{MODULE_NAME}] Views transformation completed with status: {results['views_status']}")
        else:
            logger.warning(f"[{MODULE_NAME}] No views data found")

        # Step 4: Process Tags
        logger.info(f"[{MODULE_NAME}] Starting tags transformation")
        tags_data = get_tags(raw_data)
        if tags_data:
            logger.info(f"[{MODULE_NAME}] Found {len(tags_data)} tags")
            tags_status = generate_tags_csv_from_config(tags_data)
            results["tags_status"] = tags_status["status"] if isinstance(tags_status, dict) else tags_status
            logger.info(f"[{MODULE_NAME}] Tags transformation completed with status: {results['tags_status']}")
        else:
            logger.warning(f"[{MODULE_NAME}] No tags data found")

        # Step 5: Process Connections
        logger.info(f"[{MODULE_NAME}] Starting connections transformation")
        connections_data = get_connections(raw_data)
        if connections_data:
            logger.info(f"[{MODULE_NAME}] Found {len(connections_data)} connections")
            connections_status = generate_connection_csv_from_config(connections_data)
            results["connections_status"] = connections_status["status"] if isinstance(connections_status, dict) else connections_status
            logger.info(f"[{MODULE_NAME}] Connections transformation completed with status: {results['connections_status']}")
        else:
            logger.warning(f"[{MODULE_NAME}] No connections data found")

        # Step 6: Process Datasources
        logger.info(f"[{MODULE_NAME}] Starting datasources transformation")
        datasources_data = get_datasources(raw_data)
        if datasources_data:
            logger.info(f"[{MODULE_NAME}] Found {len(datasources_data)} datasources")
            datasources_status = generate_datasources_csv_from_config(datasources_data)
            results["datasources_status"] = datasources_status["status"] if isinstance(datasources_status, dict) else datasources_status
            logger.info(f"[{MODULE_NAME}] Datasources transformation completed with status: {results['datasources_status']}")
        else:
            logger.warning(f"[{MODULE_NAME}] No datasources data found")

        # Step 7: Process Datasource-Connections Relationships
        logger.info(f"[{MODULE_NAME}] Starting datasource-connections relationships transformation")
        datasource_connections_data = get_datasource_connections(raw_data)
        if datasource_connections_data:
            logger.info(f"[{MODULE_NAME}] Found {len(datasource_connections_data)} datasource-connection relationships")
            datasource_connections_status = generate_datasource_connections_csv_from_config(datasource_connections_data)
            results["datasource_connections_status"] = datasource_connections_status["status"] if isinstance(datasource_connections_status, dict) else datasource_connections_status
            logger.info(f"[{MODULE_NAME}] Datasource-connections relationships transformation completed with status: {results['datasource_connections_status']}")
        else:
            logger.warning(f"[{MODULE_NAME}] No datasource-connection relationships found")

        # Step 7: Process Workbook-Views Relationships
        logger.info(f"[{MODULE_NAME}] Starting workbook-views relationships transformation")
        workbook_views_data = get_workbook_views(raw_data)
        if workbook_views_data:
            logger.info(f"[{MODULE_NAME}] Found {len(workbook_views_data)} workbook-view relationships")
            workbook_views_status = generate_workbook_views_csv_from_config(workbook_views_data)
            results["workbook_views_status"] = workbook_views_status["status"] if isinstance(workbook_views_status, dict) else workbook_views_status
            logger.info(f"[{MODULE_NAME}] Workbook-views relationships transformation completed with status: {results['workbook_views_status']}")
        else:
            logger.warning(f"[{MODULE_NAME}] No workbook-view relationships found")

        # Step 8: Process Workbook-Tags Relationships
        logger.info(f"[{MODULE_NAME}] Starting workbook-tags relationships transformation")
        workbook_tags_data = get_workbook_tags(raw_data)
        if workbook_tags_data:
            logger.info(f"[{MODULE_NAME}] Found {len(workbook_tags_data)} workbook-tag relationships")
            workbook_tags_status = generate_workbook_tags_csv_from_config(workbook_tags_data)
            results["workbook_tags_status"] = workbook_tags_status["status"] if isinstance(workbook_tags_status, dict) else workbook_tags_status
            logger.info(f"[{MODULE_NAME}] Workbook-tags relationships transformation completed with status: {results['workbook_tags_status']}")
        else:
            logger.warning(f"[{MODULE_NAME}] No workbook-tag relationships found")

        # Step 9: Process Workbook-Datasources Relationships
        logger.info(f"[{MODULE_NAME}] Starting workbook-datasources relationships transformation")
        workbook_datasources_data = get_workbook_datasources(raw_data)
        if workbook_datasources_data:
            logger.info(f"[{MODULE_NAME}] Found {len(workbook_datasources_data)} workbook-datasource relationships")
            workbook_datasources_status = generate_workbook_datasources_csv_from_config(workbook_datasources_data)
            results["workbook_datasources_status"] = workbook_datasources_status["status"] if isinstance(workbook_datasources_status, dict) else workbook_datasources_status
            logger.info(f"[{MODULE_NAME}] Workbook-datasources relationships transformation completed with status: {results['workbook_datasources_status']}")
        else:
            logger.warning(f"[{MODULE_NAME}] No workbook-datasource relationships found")
        
        # Determine overall status - Success only if all steps succeeded
        individual_statuses = [
            results["users_status"],
            results["workbooks_status"],
            results["views_status"],
            results["tags_status"],
            results["connections_status"],
            results["datasources_status"],
            results["workbook_views_status"],
            results["workbook_tags_status"],
            results["workbook_datasources_status"],
            results["datasource_connections_status"]

        ]
        
        results["overall_status"] = "Success" if all(status == "Success" for status in individual_statuses) else "Failed"
        
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{MODULE_NAME}] Complete transformation process finished in {processing_time:.2f} seconds")
        logger.info(f"[{MODULE_NAME}] Overall status: {results['overall_status']}")
        
        return results
        
    except Exception as e:
        logger.critical(f"[{MODULE_NAME}] Unexpected error during transformation: {str(e)}")
        return results

if __name__ == "__main__":
    logger.info(f"[{MODULE_NAME}] Script execution started.")
    try:
        # Get the root folder path
        root_folder = Path(__file__).resolve().parents[2]
        
        # Load test data
        test_data_path = root_folder / "sample_data" / "data_test.json"
        logger.info(f"[{MODULE_NAME}] Loading test data from: {test_data_path}")
        
        with open(test_data_path, 'r') as f:
            raw_data = json.load(f)
        
        # Run the transformation
        results = transform_all(raw_data)
        
        # Log the results
        logger.info(f"[{MODULE_NAME}] Transformation Results:")
        for key, value in results.items():
            logger.info(f"[{MODULE_NAME}] {key}: {value}")
        
    except FileNotFoundError:
        logger.error(f"[{MODULE_NAME}] Test data file not found at: {test_data_path}")
    except json.JSONDecodeError:
        logger.error(f"[{MODULE_NAME}] Invalid JSON in test data file: {test_data_path}")
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Unexpected error: {str(e)}", exc_info=True)
    finally:
        logger.info(f"[{MODULE_NAME}] Script execution finished.")