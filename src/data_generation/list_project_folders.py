import csv
from pathlib import Path
from typing import List, Dict, Any, Optional
from src.utils.logger import app_logger as logger
from src.generate_csv.generate_projects_csv import generate_project_csv_from_config
from src.config.config_loader import get_project_file_path
from grahql_query_generation_for_projects import   generate_graphql_queries
import yaml
from src.querying.graphql_query_loader import get_query_by_name

generate_project_csv_from_config()
current_file = Path(__file__).resolve()
root_folder =  current_file.parents[2]
def load_project_mappings(csv_path: Path) -> Dict[str, str]:
    """
    Loads project_id to project_name mapping from the CSV.
    """
    mappings = {}
    with open(csv_path, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            project_id = row.get("project_id")
            project_name = row.get("project_name")
            if project_id and project_name:
                mappings[project_id] = project_name
    return mappings


if __name__ == "__main__":
    current_file = Path(__file__).resolve().name
    logger.info(f"Running script: {current_file}")

    try:
        # Step 1: Generate CSV
        generate_project_csv_from_config()
        logger.info("CSV generated successfully.")

        # Step 2: Load the generated CSV
        csv_path = root_folder / "app" / "data" / "temp"  / "tableau_projects_list.csv"
        logger.info(f"Loading project mappings from: {csv_path}")
        project_mappings = load_project_mappings(csv_path)
        for i in project_mappings:
            generate_graphql_queries()

        # Step 3: You provide the string here
        test_input = "This belongs to project_id=12345 which needs update."
        template_query =   get_query_by_name(query_name= "master_query")
        updated_string = get_query_by_name(project_mappings ,query_name= template_query)
        print(updated_string)

        # Step 4: Print the updated string
        print("Updated string:", updated_string)

    except Exception as e:
        logger.error(f"Error in {current_file}: {e}")
