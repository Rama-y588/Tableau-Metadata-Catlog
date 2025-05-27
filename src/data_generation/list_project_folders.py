import csv
from pathlib import Path
from typing import List, Dict, Any, Optional
from src.utils.logger import app_logger as logger
from src.generate_csv.generate_projects_csv import generate_project_csv_from_config
from src.config.config_loader import get_project_file_path
from src.data_generation.grahql_query_generation_for_projects import generate_graphql_queries
from src.querying.graphql_query_loader import get_query_by_name
import yaml
from  src.querying.metadata_querying import save_metadata_from_query

# Generate the CSV that stores project IDs and names
generate_project_csv_from_config()

# Resolve root folder of the repo (assumes this file is nested at least 2 levels deep)
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]

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
    script_name = Path(__file__).resolve().name
    logger.info(f"Running script: {script_name}")

    try:
        # Step 1: Generate CSV
        generate_project_csv_from_config()
        logger.info("CSV generated successfully.")

        # Step 2: Load the generated CSV
        csv_path = root_folder / "app" / "data" / "temp" / "tableau_projects_list.csv"
        logger.info(f"Loading project mappings from: {csv_path}")
        project_mappings = load_project_mappings(csv_path)

        # Step 3: Load GraphQL query template
        template_query = get_query_by_name(query_name="master_query")

        # Step 4: Generate GraphQL queries
        graphql_queries = generate_graphql_queries(mapping=project_mappings, query_template=template_query)

        # Step 5: Print each generated query
        for item in graphql_queries:
            print(f"Project ID: {item['project_id']}")
            print(f"Project Name: {item['project_name']}")
            print("Generated Query:")
            print(item['query'])
            print("=" * 80)
            save_metadata_from_query(query= item["query"])
            #sequrece of operations here are need to be writtem 
        

    except Exception as e:
        logger.error(f"Error in {script_name}: {e}")
