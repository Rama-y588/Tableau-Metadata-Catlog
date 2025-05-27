import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple

# Assuming get_tableau_server and disconnect_tableau_server are in this module path
from src.Auth.server_connection import get_tableau_server, disconnect_tableau_server, get_available_profiles
# Assuming app_logger is defined and configured in src.utils.logging
from src.utils.logger import app_logger as logger
from src.utils.graphql.load_graphql_query import load_graphql_query
from src.utils.graphql.execute_graphql_query import execute_graphql_query
import tableauserverclient as TSC # Import TSC for type hinting in execute_graphql_query assumption
from typing import Dict, Any, Optional
# --- Module Constants ---
current_file = Path(__file__).resolve()
root_folder = current_file.parent.parent


def get_unique_workbook_projects(profile_name: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Fetches unique workbook project IDs and names from Tableau Server Metadata API.
    Each unique project is returned as a dictionary.

    Args:
        profile_name (Optional[str]): The name of the Tableau Server profile to use.
                                      If None, the default profile will be used.

    Returns:
        List[Dict[str, str]]: A list of dictionaries, where each dictionary
                              contains 'project_id' and 'project_name' for a unique project.
                              Returns an empty list if no data is found or an error occurs.

    Raises:
        FileNotFoundError: If the configuration file for GraphQL queries is not found.
        ValueError: If there's an issue with configuration, missing environment variables,
                    or an invalid GraphQL query.
        ConnectionError: If connection to Tableau Server fails.
        Exception: For other unexpected errors during the API call or data processing.
    """
    query_name = "get_projects_from_workbook" # Name of the query in GraphQ_queries.yaml
    logger.info(f"Attempting to fetch unique workbook project details using query: '{query_name}'")
    
    try:
        query = load_graphql_query(query_name)
    except (FileNotFoundError, ValueError) as e:
        logger.critical(f"Failed to load GraphQL query: {e}")
        raise

    unique_projects_set: Set[Tuple[str, str]] = set() # Use a set to store (project_id, project_name) tuples for uniqueness
    server: Optional[TSC.Server] = None # Initialize server to None with type hint

    try:
        # FIX: Pass the profile_name to get_tableau_server
        server = get_tableau_server(profile_name=profile_name) 
        logger.debug(f"Executing Tableau Metadata API query:\n{query}")
        
        # FIX: Pass the server object to execute_graphql_query
        # Assuming execute_graphql_query takes the server object as its first argument.
        resp = execute_graphql_query(server, query) 
        
        # Check for GraphQL errors in the response
        if 'errors' in resp:
            error_messages = [err.get('message', 'Unknown error') for err in resp['errors']]
            logger.error(f"GraphQL query returned errors: {'; '.join(error_messages)}")
            raise Exception(f"GraphQL query errors: {'; '.join(error_messages)}")

        # Extract data from the response
        workbooks_data = resp.get('data', {}).get('workbooks', [])
        
        if not workbooks_data:
            logger.info("No workbooks found or metadata query returned no data.")
            return []

        for workbook in workbooks_data:
            project_uid = workbook.get('projectuid')
            project_name = workbook.get('projectname')

            if project_uid and project_name:
                unique_projects_set.add((project_uid, project_name)) # Add as a tuple to the set
            else:
                # Log a warning if a workbook doesn't have complete project info, but continue processing
                workbook_name = workbook.get('name', 'Unnamed Workbook')
                workbook_id = workbook.get('id', 'Unknown ID')
                logger.warning(f"Workbook '{workbook_name}' (ID: {workbook_id}) found with missing project details.")
        
        logger.info(f"Successfully fetched {len(unique_projects_set)} unique workbook project entries.")
        
        # Convert the set of tuples back to a list of dictionaries for the return value
        result_list = [{'project_id': pid, 'project_name': pname} for pid, pname in unique_projects_set]
        return result_list

    except ConnectionError as e:
        logger.error(f"Failed to connect to Tableau Server: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while fetching unique workbook project names: {e}")
        raise
    finally:
        # Ensure disconnection happens even if an error occurs
        if server:
            # FIX: Pass the profile_name to ensure the correct connection is disconnected
            # This is crucial if get_tableau_server manages multiple cached connections by profile_name.
            disconnect_tableau_server(profile_name)
            logger.debug(f"Disconnected from Tableau Server profile: {profile_name}")