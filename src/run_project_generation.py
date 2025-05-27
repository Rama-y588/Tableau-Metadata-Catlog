import sys
from pathlib import Path
import argparse
from typing import Dict, Any, List, Tuple, Optional
import json

from src.utils.logger import app_logger as logger
from src.api.tableau_metadata_api import get_tableau_metadata
from src.generate_csv.generate_projects_csv import generate_projects_csv_from_config


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


def run_project_generation(config_path: Optional[Path] = None, save_raw_data: bool = True) -> Dict[str, Tuple[bool, str]]:
    """
    Run the project CSV generation process.
    
    Args:
        config_path (Optional[Path]): Path to the Tableau configuration file
        save_raw_data (bool): Whether to save raw metadata to a JSON file
        
    Returns:
        Dict[str, Tuple[bool, str]]: Results of the operation
    """
    logger.info("Starting project generation process")
    results = {}

    # Step 1: Create project structure
    success, message = create_project_structure()
    if not success:
        logger.error(f"Failed to create project structure: {message}")
        return {"project_structure": (False, message)}

    # Step 2: Fetch metadata using GraphQL
    try:
        raw_data = get_tableau_metadata(config_path)
        logger.info("Successfully fetched metadata from Tableau GraphQL API")
    except Exception as e:
        error_msg = f"GraphQL query execution failed: {str(e)}"
        logger.error(error_msg)
        return {"metadata_fetch": (False, error_msg)}

    # Step 3: Save raw metadata if requested
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

    # Step 4: Generate Projects CSV
    try:
        projects = raw_data.get('projects', [])
        results["projects"] = generate_projects_csv_from_config(projects)
        logger.info("Successfully generated projects CSV")
    except Exception as e:
        error_msg = f"Projects CSV generation failed: {str(e)}"
        logger.error(error_msg)
        results["projects"] = (False, error_msg)

    # Print summary
    logger.info("\nProject Generation Summary:\n" + "=" * 50)
    for key, (success, msg) in results.items():
        logger.info(f"{key.title()} - {'Success' if success else 'Failed'}: {msg}")
    logger.info("=" * 50)

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate project CSV and create project structure.")
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
    run_project_generation(args.config, save_raw_data=not args.no_save) 