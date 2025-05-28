import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Optional

from src.Transformations.list_users import get_users
from src.Transformations.list_workbooks import get_workbooks
from src.generate_csv.generate_users_csv import generate_users_csv_from_config
from src.generate_csv.generate_workbooks_csv import generate_workbooks_csv_from_config
from src.utils.config_loader import load_YAML_config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

MODULE_NAME = "transform_all"

def transform_all(raw_data: Dict[str, Any]) -> Dict[str, str]:
    """
    Orchestrates the entire transformation process:
    1. Process users data and generate users CSV
    2. Process workbooks data and generate workbooks CSV
    
    Args:
        raw_data (Dict[str, Any]): Raw data containing users and workbooks information
        
    Returns:
        Dict[str, str]: Status of each transformation step
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting complete transformation process")
    
    results = {
        "users_status": "Not Started",
        "workbooks_status": "Not Started",
        "overall_status": "Not Started"
    }
    
    try:
        # Step 1: Process Users
        logger.info(f"[{MODULE_NAME}] Starting users transformation")
        users_data = get_users(raw_data)
        if users_data:
            logger.info(f"[{MODULE_NAME}] Found {len(users_data)} users")
            users_status = generate_users_csv_from_config(users_data)
            results["users_status"] = users_status
            logger.info(f"[{MODULE_NAME}] Users transformation completed with status: {users_status}")
        else:
            logger.warning(f"[{MODULE_NAME}] No users data found")
            results["users_status"] = "Failed"
        
        # Step 2: Process Workbooks
        logger.info(f"[{MODULE_NAME}] Starting workbooks transformation")
        workbooks_data = get_workbooks(raw_data)
        if workbooks_data:
            logger.info(f"[{MODULE_NAME}] Found {len(workbooks_data)} workbooks")
            workbooks_status = generate_workbooks_csv_from_config(workbooks_data)
            results["workbooks_status"] = workbooks_status
            logger.info(f"[{MODULE_NAME}] Workbooks transformation completed with status: {workbooks_status}")
        else:
            logger.warning(f"[{MODULE_NAME}] No workbooks data found")
            results["workbooks_status"] = "Failed"
        
        # Determine overall status
        if results["users_status"] == "Success" and results["workbooks_status"] == "Success":
            results["overall_status"] = "Success"
        elif results["users_status"] == "Failed" and results["workbooks_status"] == "Failed":
            results["overall_status"] = "Failed"
        else:
            results["overall_status"] = "Partial"
        
        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{MODULE_NAME}] Complete transformation process finished in {processing_time:.2f} seconds")
        logger.info(f"[{MODULE_NAME}] Overall status: {results['overall_status']}")
        
        return results
        
    except Exception as e:
        logger.critical(f"[{MODULE_NAME}] Unexpected error during transformation: {str(e)}")
        results["overall_status"] = "Failed"
        return results

def main():
    """
    Main function to demonstrate the transformation process
    """
    try:
        # Load sample data (replace with your actual data loading logic)
        sample_data = {
            "users": [
                {
                    "id": "user1",
                    "name": "John Doe",
                    "email": "john@example.com",
                    "role": "Viewer"
                }
            ],
            "workbooks": [
                {
                    "id": "workbook1",
                    "name": "Sample Workbook",
                    "owner": {"id": "user1"},
                    "created_at": "2024-01-01T00:00:00Z",
                    "updated_at": "2024-01-01T00:00:00Z"
                }
            ]
        }
        
        # Run the transformation
        results = transform_all(sample_data)
        
        # Log the results
        logger.info("Transformation Results:")
        logger.info(f"Users Status: {results['users_status']}")
        logger.info(f"Workbooks Status: {results['workbooks_status']}")
        logger.info(f"Overall Status: {results['overall_status']}")
        
    except Exception as e:
        logger.critical(f"Error in main function: {str(e)}")

if __name__ == "__main__":
    main()