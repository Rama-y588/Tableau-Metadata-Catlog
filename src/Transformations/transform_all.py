import json
from pathlib import Path
from typing import Dict, Any, List, Tuple, Optional

from src.utils.logger import app_logger as logger
from src.utils.status_tracker import status_tracker
from src.api.tableau_metadata_api import get_tableau_metadata
from src.Transformations.list_workbooks import get_workbooks
from src.Transformations.list_views import get_views
from src.Transformations.list_datasources import get_datasources
from src.Transformations.list_connection import get_connections
from src.Transformations.list_tags import get_tags
from src.generate_csv.generate_workbooks_csv import generate_workbooks_csv_from_config
from src.generate_csv.generate_views_csv import generate_views_csv_from_config
from src.generate_csv.generate_datasources_csv import generate_datasources_csv_from_config
from src.generate_csv.generate_connections_csv import generate_connections_csv_from_config
from src.generate_csv.generate_tags_csv import generate_tags_csv_from_config


def transform_all(config_path: Optional[Path] = None, save_raw_data: bool = True) -> Dict[str, Tuple[bool, str]]:
    """
    Transform Tableau metadata and generate all CSV files.
    
    Args:
        config_path (Optional[Path]): Path to the Tableau configuration file
        save_raw_data (bool): Whether to save the raw metadata to a JSON file
        
    Returns:
        Dict[str, Tuple[bool, str]]: Dictionary mapping CSV types to their generation status
            Each status is a tuple of (success, message)
    """
    logger.info("Starting transformation of all data types")
    
    # Get metadata from Tableau
    try:
        raw_data = get_tableau_metadata(config_path)
        logger.info("Successfully retrieved metadata from Tableau")
    except Exception as e:
        error_msg = f"Failed to get metadata from Tableau: {str(e)}"
        logger.error(error_msg)
        return {
            'workbooks': (False, error_msg),
            'views': (False, error_msg),
            'datasources': (False, error_msg),
            'connections': (False, error_msg),
            'tags': (False, error_msg)
        }
    
    # Save raw data if requested
    if save_raw_data:
        try:
            current_file = Path(__file__).resolve()
            project_root = current_file.parents[1]
            output_path = project_root / "sample_data" / "data_test.json"
            
            with open(output_path, 'w') as f:
                json.dump(raw_data, f, indent=2)
            
            logger.info(f"Saved raw metadata to: {output_path}")
        except Exception as e:
            logger.warning(f"Failed to save raw data: {str(e)}")
    
    # Transform data and generate CSVs
    results = {}
    
    # Workbooks
    try:
        workbooks = get_workbooks(raw_data=raw_data)
        results['workbooks'] = generate_workbooks_csv_from_config(workbooks)
    except Exception as e:
        error_msg = f"Failed to process workbooks: {str(e)}"
        logger.error(error_msg)
        results['workbooks'] = (False, error_msg)
    
    # Views
    try:
        views = get_views(raw_data=raw_data)
        results['views'] = generate_views_csv_from_config(views)
    except Exception as e:
        error_msg = f"Failed to process views: {str(e)}"
        logger.error(error_msg)
        results['views'] = (False, error_msg)
    
    # Datasources
    try:
        datasources = get_datasources(raw_data=raw_data)
        results['datasources'] = generate_datasources_csv_from_config(datasources)
    except Exception as e:
        error_msg = f"Failed to process datasources: {str(e)}"
        logger.error(error_msg)
        results['datasources'] = (False, error_msg)
    
    # Connections
    try:
        connections = get_connections(raw_data=raw_data)
        results['connections'] = generate_connections_csv_from_config(connections)
    except Exception as e:
        error_msg = f"Failed to process connections: {str(e)}"
        logger.error(error_msg)
        results['connections'] = (False, error_msg)
    
    # Tags
    try:
        tags = get_tags(raw_data=raw_data)
        results['tags'] = generate_tags_csv_from_config(tags)
    except Exception as e:
        error_msg = f"Failed to process tags: {str(e)}"
        logger.error(error_msg)
        results['tags'] = (False, error_msg)
    
    # Print summary of all operations
    status_tracker.print_summary()
    
    return results


if __name__ == "__main__":
    logger.info("Starting transformation process")
    
    # Run transformations
    results = transform_all()
    
    # Log final results
    logger.info("\nTransformation Results Summary:")
    logger.info("=" * 50)
    for csv_type, (success, message) in results.items():
        logger.info(f"\n{csv_type.title()}:")
        logger.info(f"  Status: {'Success' if success else 'Failure'}")
        logger.info(f"  Message: {message}")
    logger.info("\n" + "=" * 50)
    
    logger.info("Transformation process completed")
