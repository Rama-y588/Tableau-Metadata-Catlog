import csv
from pathlib import Path
from typing import List, Dict, Any
from src.utils.logger import app_logger as logger
from src.generate_csv.generate_projects_csv import generate_project_csv_from_config
from src.config.config_loader import load_YAML_config
from src.data_generation.grahql_query_generation_for_projects import generate_graphql_queries
from src.querying.graphql_query_loader import get_query_by_name
from src.querying.metadata_querying import save_metadata_from_query

# Load config file
current_file = Path(__file__).resolve()
root_folder = current_file.parents[3]
CONFIG_FILE_PATH =root_folder / "config" / "csv_exporter.yaml"

# Utility function to load the project mappings from the CSV
def load_project_mappings(csv_path: Path) -> List[Dict[str, Any]]:
    mappings = []
    try:
        with open(csv_path, mode='r', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                mappings.append(row)
    except Exception as e:
        logger.error(f"Failed to read CSV: {e}")
        raise
    return mappings

def main():
    script_name = Path(__file__).resolve().name
    logger.info(f"Running script: {script_name}")

    try:
        # Step 1: Load configuration
        config = load_YAML_config(CONFIG_FILE_PATH)
        if not config:
            logger.error("Failed to load configuration. Aborting CSV generation.")
            return

        file_settings = config.get('file_settings', {})
        data_folder_path_str = file_settings.get('data_folder_path')
        temp_subfolder_name = file_settings.get('temp_subfolder_name')
        project_csv_filename = file_settings.get('project_csv_filename')

        if not all([data_folder_path_str, temp_subfolder_name, project_csv_filename]):
            logger.error("One or more required file settings are missing in the config.")
            return

        # Build full CSV path using config
        csv_path = Path(data_folder_path_str) / temp_subfolder_name / project_csv_filename
        logger.info(f"CSV will be loaded from: {csv_path}")

        # Step 2: Generate CSV
        generate_project_csv_from_config()
        logger.info("CSV generated successfully.")

        # Step 3: Load the generated CSV
        project_mappings = load_project_mappings(csv_path)

        # Step 4: Load GraphQL query template
        template_query = get_query_by_name(query_name="master_query")

        # Step 5: Generate GraphQL queries
        graphql_queries = generate_graphql_queries(mapping=project_mappings, query_template=template_query)

        # Step 6: Print each generated query and save metadata
        for item in graphql_queries:
            print(f"Project ID: {item['project_id']}")
            print(f"Project Name: {item['project_name']}")
            print("Generated Query:")
            print(item['query'])
            print("=" * 80)

            save_metadata_from_query(query=item["query"])

        logger.info("All queries processed and metadata saved successfully.")

    except Exception as e:
        logger.error(f"Error in {script_name}: {e}")

if __name__ == "__main__":
    main()
