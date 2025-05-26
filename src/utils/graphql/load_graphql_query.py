import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple

# Assuming get_tableau_server and disconnect_tableau_server are in this module path
from src.Auth.server_connection import get_tableau_server, disconnect_tableau_server
# Assuming app_logger is defined and configured in src.utils.logging
from src.utils.logger import app_logger as logger

# --- Module Constants ---
current_file = Path(__file__).resolve()
# This navigates from 'graphql_data_fetcher.py' -> 'new_code' -> 'src' -> 'Tableau_application'
root_folder = current_file.parent.parent.parent
GRAPHQL_QUERIES_CONFIG_PATH = root_folder / 'config' / 'GraphQL_queries.yaml'

def load_graphql_query(query_name: str) -> str:
    """
    Loads a specific GraphQL query string from the configuration file.

    Args:
        query_name (str): The key name of the query in the YAML file.

    Returns:
        str: The GraphQL query string.

    Raises:
        FileNotFoundError: If the GraphQL queries YAML file is not found.
        ValueError: If the query name is not found, the 'query' key is missing,
                    or the YAML format is invalid.
    """
    if not GRAPHQL_QUERIES_CONFIG_PATH.exists():
        logger.error(f"GraphQL queries configuration file not found at: {GRAPHQL_QUERIES_CONFIG_PATH}")
        raise FileNotFoundError(f"GraphQL queries configuration file not found at: {GRAPHQL_QUERIES_CONFIG_PATH}")

    try:
        with open(GRAPHQL_QUERIES_CONFIG_PATH, 'r') as f:
            queries_data = yaml.safe_load(f)

        if not isinstance(queries_data, dict):
            logger.error(f"Invalid YAML format in {GRAPHQL_QUERIES_CONFIG_PATH}. Expected a dictionary.")
            raise ValueError(f"Invalid YAML format in {GRAPHQL_QUERIES_CONFIG_PATH}. Expected a dictionary.")

        query_block = queries_data.get(query_name)
        if not query_block:
            logger.error(f"GraphQL query '{query_name}' not found in {GRAPHQL_QUERIES_CONFIG_PATH}.")
            raise ValueError(f"GraphQL query '{query_name}' not found in {GRAPHQL_QUERIES_CONFIG_PATH}.")
        
        query_string = query_block.get('query')
        if not query_string:
            logger.error(f"Missing 'query' key for '{query_name}' in {GRAPHQL_QUERIES_CONFIG_PATH}.")
            raise ValueError(f"Missing 'query' key for '{query_name}' in {GRAPHQL_QUERIES_CONFIG_PATH}.")

        logger.debug(f"Successfully loaded GraphQL query '{query_name}'.")
        return query_string

    except yaml.YAMLError as e:
        logger.error(f"Error parsing GraphQL queries YAML file {GRAPHQL_QUERIES_CONFIG_PATH}: {e}")
        raise ValueError(f"Error parsing GraphQL queries YAML file {GRAPHQL_QUERIES_CONFIG_PATH}: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred while loading GraphQL query '{query_name}': {e}", exc_info=True)
        raise

