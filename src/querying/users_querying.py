import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple

# Assuming these imports are correct based on your project structure
from src.utils.graphql.execute_graphql_query import execute_graphql_query
from src.utils.graphql.load_graphql_query import load_graphql_query # Renamed to match previous refactoring
from src.utils.logger import app_logger as logger
# --- Module Constants (if needed for this specific file, otherwise remove) ---
# Example: If get_users_from_tableau_server needs its own config for query names
# current_file = Path(__file__).resolve()
# root_folder = current_file.parent.parent.parent
# GRAPHQL_QUERIES_CONFIG_PATH = root_folder / 'config' / 'GraphQ_queries.yaml'

def get_users_from_tableau_server(profile_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """
    Retrieves user information from Tableau Server using the Metadata API.

    Args:
        profile_name (Optional[str]): The name of the Tableau Server profile to use.
                                      If None, the default profile will be used.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, where each dictionary
                              represents a user with their fetched attributes.
                              Returns an empty list if no data is found or an error occurs.

    Raises:
        FileNotFoundError: If the configuration file for GraphQL queries is not found.
        ValueError: If there's an issue with configuration, missing environment variables,
                    or an invalid GraphQL query.
        ConnectionError: If connection to Tableau Server fails.
        Exception: For other unexpected errors during the API call or data processing.
    """
    query_name = "get_projects_from_workbook"   # Name of the query in GraphQ_queries.yaml
    logger.info(f"Preparing to fetch user information using query: '{query_name}'")

    try:
        # 1. Load the GraphQL query string
        query = load_graphql_query(query_name) # Using the refactored _load_graphql_query

        # 2. Execute the query against Tableau Server
        # _execute_graphql_query already handles connection, disconnection, and GraphQL errors
        resp = execute_graphql_query(query, profile_name)
        
        # 3. Extract and return user information from the response
        users = resp.get('data', {}).get('users', [])
        logger.info(f"Successfully fetched {len(users)} users from Tableau Server.")
        
        return users
    except (FileNotFoundError, ValueError, ConnectionError) as e:
        # Catch specific, anticipated errors related to config or connection
        logger.critical(f"Failed to retrieve user data due to configuration or connection error: {e}")
        raise # Re-raise the specific exception
    except Exception as e:
        # Catch any other unexpected errors during query loading or execution
        logger.critical(f"An unexpected error occurred during user data fetching: {e}", exc_info=True)
        raise # Re-raise the unexpected exception

# --- Example Usage and Testing (for direct execution of this file) ---
if __name__ == "__main__":
    # This block assumes your `src.utils.logging.app_logger` is already configured
    # and will output logs as per its configuration (e.g., to console or file).
    logger.info("--- Running User Listing from Tableau Server ---")
    try:
        # Call the function to get user data
        # You can pass a specific profile_name here, e.g., get_users_from_tableau_server("development")
        users_data = get_users_from_tableau_server() 
        
        if users_data:
            logger.info(f"\nSuccessfully retrieved {len(users_data)} users:")
            # Print a few details for demonstration
            for i, user in enumerate(users_data[:5]): # Print first 5 users
                logger.info(f"  User {i+1}: ID={user.get('id')}, Name={user.get('name')}, Username={user.get('username')}, Email={user.get('email')}")
            if len(users_data) > 5:
                logger.info(f"  ...and {len(users_data) - 5} more users.")
        else:
            logger.info("No user data retrieved.")

    except (FileNotFoundError, ValueError, ConnectionError, Exception) as e:
        logger.critical(f"Script execution failed: {e}")

    logger.info("\n--- User Listing from Tableau Server Finished ---")