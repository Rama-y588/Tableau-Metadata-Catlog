import yaml
from pathlib import Path
from typing import Dict, Any, Optional
from src.utils.logger import app_logger as logger

# Get the root folder path
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]  # Go up to src directory
GRAPHQL_QUERIES_CONFIG_PATH = root_folder / 'config' / 'GraphQL_queries.yaml'

def load_graphql_query(query_name: str) -> str:
    """
    Load a specific GraphQL query by name from the configuration file
    
    Args:
        query_name (str): Name of the query to load (e.g., 'master_query', 'metadata_query', etc.)
        
    Returns:
        str: The requested GraphQL query string
        
    Raises:
        FileNotFoundError: If the GraphQL queries YAML file is not found
        ValueError: If the query name is not found or the YAML format is invalid
    """
    try:
        if not GRAPHQL_QUERIES_CONFIG_PATH.exists():
            logger.error(f"GraphQL queries configuration file not found at: {GRAPHQL_QUERIES_CONFIG_PATH}")
            raise FileNotFoundError(f"GraphQL queries configuration file not found at: {GRAPHQL_QUERIES_CONFIG_PATH}")

        # Load the YAML file
        with open(GRAPHQL_QUERIES_CONFIG_PATH, 'r', encoding='utf-8') as f:
            queries_data = yaml.safe_load(f)

        if not isinstance(queries_data, dict):
            logger.error(f"Invalid YAML format in {GRAPHQL_QUERIES_CONFIG_PATH}. Expected a dictionary.")
            raise ValueError(f"Invalid YAML format in {GRAPHQL_QUERIES_CONFIG_PATH}. Expected a dictionary.")

        # Get the requested query
        query_block = queries_data.get(query_name)
        if not query_block:
            logger.error(f"GraphQL query '{query_name}' not found in {GRAPHQL_QUERIES_CONFIG_PATH}.")
            raise ValueError(f"GraphQL query '{query_name}' not found in {GRAPHQL_QUERIES_CONFIG_PATH}.")
        
        query_string = query_block.get('query')
        if not query_string:
            logger.error(f"Missing 'query' key for '{query_name}' in {GRAPHQL_QUERIES_CONFIG_PATH}.")
            raise ValueError(f"Missing 'query' key for '{query_name}' in {GRAPHQL_QUERIES_CONFIG_PATH}.")

        logger.info(f"Successfully loaded GraphQL query: {query_name}")
        return query_string

    except yaml.YAMLError as e:
        logger.error(f"Error parsing GraphQL queries YAML file {GRAPHQL_QUERIES_CONFIG_PATH}: {e}")
        raise ValueError(f"Error parsing GraphQL queries YAML file {GRAPHQL_QUERIES_CONFIG_PATH}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading GraphQL query '{query_name}': {e}")
        raise


def get_query_by_name(query_name: str) -> str:
    """
    Get a GraphQL query by name and print it
    
    Args:
        query_name (str): Name of the query to load
        
    Returns:
        str: The requested GraphQL query string
    """
    try:
        # Load the query
        query = load_graphql_query(query_name)
        
        # Print the query
        logger.info(f"\nGraphQL Query '{query_name}':\n" + "=" * 50)
        logger.info(query)
        logger.info("=" * 50)
        
        return query
        
    except Exception as e:
        logger.error(f"Failed to get query '{query_name}': {str(e)}")
        raise


if __name__ == "__main__":
        
    query_name = "master_query"
    
    try:
        # Get and print the query
        query = get_query_by_name(query_name)
        print(query)
    except Exception as e:
        logger.error(f"Error: {str(e)}")
       