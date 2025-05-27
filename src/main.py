# step 1 is to run the generate_projects_csv that code 

import sys
from pathlib import Path
import argparse
from typing import Dict, Any, List, Tuple, Optional
import json

from src.utils.logger import app_logger as logger
from src.api.tableau_metadata_api import get_tableau_metadata
from src.Transformations.list_workbooks import get_workbooks
from src.Transformations.list_views import get_views
from src.Transformations.list_datasources import get_datasources
from src.Transformations.list_connection import get_connections
from src.Transformations.list_tags import get_tags
from src.generate_csv.generate_projects_csv import generate_projects_csv_from_config
from src.generate_csv.generate_workbooks_csv import generate_workbooks_csv_from_config
from src.generate_csv.generate_views_csv import generate_views_csv_from_config
from src.generate_csv.generate_datasources_csv import generate_datasources_csv_from_config
from src.generate_csv.generate_connections_csv import generate_connections_csv_from_config
from src.generate_csv.generate_tags_csv import generate_tags_csv_from_config


def create_project_structure() -> Tuple[bool, str]:
    """
    Create the necessary project folder structure.
    
    Returns:
        Tuple[bool, str]: Success status and message
    """
    try:
        # Get project root
        current_file = Path(__file__).resolve()
        project_root = current_file.parents[1]
        
        # Define required directories
        directories = [
            project_root / "output" / "csv",        # For CSV files
            project_root / "output" / "logs",       # For log files
            project_root / "sample_data",           # For sample/test data
            project_root / "config",                # For configuration files
            project_root / "api",                   # For API related files
            project_root / "Transformations",       # For transformation modules
            project_root / "generate_csv",          # For CSV generation modules
            project_root / "utils"                  # For utility modules
        ]
        
        # Create directories
        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {directory}")
        
        return True, "Successfully created project structure"
        
    except Exception as e:
        error_msg = f"Failed to create project structure: {str(e)}"
        logger.error(error_msg)
        return False, error_msg


def graphql_to_csv_pipeline(config_path: Path = None, save_raw_data: bool = True) -> Dict[str, Tuple[bool, str]]:
    """
    Executes a GraphQL query and generates corresponding CSV files.

    Args:
        config_path (Path): Path to the Tableau configuration file
        save_raw_data (bool): Whether to save raw metadata to a JSON file

    Returns:
        Dict[str, Tuple[bool, str]]: CSV generation results for each category
    """
    logger.info("Starting GraphQL to CSV pipeline")

    # First, ensure project structure exists
    success, message = create_project_structure()
    if not success:
        logger.error(f"Failed to create project structure: {message}")
        return {}

    # Fetch metadata using GraphQL
    try:
        raw_data = get_tableau_metadata(config_path)
        logger.info("Successfully fetched metadata from Tableau GraphQL API")
    except Exception as e:
        logger.error(f"GraphQL query execution failed: {str(e)}")
        return {}

    # Save raw metadata
    if save_raw_data:
        try:
            current_file = Path(__file__).resolve()
            project_root = current_file.parents[1]
            output_path = project_root / "sample_data" / "raw_metadata.json"
            
            with open(output_path, "w") as f:
                json.dump(raw_data, f, indent=2)
            logger.info(f"Saved raw metadata to {output_path}")
        except Exception as e:
            logger.warning(f"Failed to save raw metadata: {str(e)}")

    # Process data and generate CSVs
    results = {}

    # Step 1: Generate Projects CSV
    try:
        projects = raw_data.get('projects', [])
        results["projects"] = generate_projects_csv_from_config(projects)
        logger.info("Step 1: Projects CSV generation completed")
    except Exception as e:
        results["projects"] = (False, f"Projects failed: {str(e)}")
        logger.error("Step 1: Projects CSV generation failed")

    # Generate other CSVs
    try:
        workbooks = get_workbooks(raw_data)
        results["workbooks"] = generate_workbooks_csv_from_config(workbooks)
    except Exception as e:
        results["workbooks"] = (False, f"Workbooks failed: {str(e)}")

    try:
        views = get_views(raw_data)
        results["views"] = generate_views_csv_from_config(views)
    except Exception as e:
        results["views"] = (False, f"Views failed: {str(e)}")

    try:
        datasources = get_datasources(raw_data)
        results["datasources"] = generate_datasources_csv_from_config(datasources)
    except Exception as e:
        results["datasources"] = (False, f"Datasources failed: {str(e)}")

    try:
        connections = get_connections(raw_data)
        results["connections"] = generate_connections_csv_from_config(connections)
    except Exception as e:
        results["connections"] = (False, f"Connections failed: {str(e)}")

    try:
        tags = get_tags(raw_data)
        results["tags"] = generate_tags_csv_from_config(tags)
    except Exception as e:
        results["tags"] = (False, f"Tags failed: {str(e)}")

    # Print summary
    logger.info("\nCSV Generation Summary:\n" + "=" * 50)
    for key, (success, msg) in results.items():
        logger.info(f"{key.title()} - {'Success' if success else 'Failed'}: {msg}")
    logger.info("=" * 50)

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate CSVs from Tableau metadata.")
    parser.add_argument(
        "--config",
        type=Path,
        help="Path to the Tableau configuration file",
        default=None
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Don't save raw metadata JSON"
    )

    args = parser.parse_args()
    graphql_to_csv_pipeline(args.config, save_raw_data=not args.no_save) 