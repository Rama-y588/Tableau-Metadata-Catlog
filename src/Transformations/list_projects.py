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


def get_dummy_project_info() -> List[Dict[str, str]]:
    """
    A dummy function that simulates fetching unique workbook project data.
    Used for testing purposes without requiring a Tableau Server connection.

    Returns:
        List[Dict[str, str]]: A list of dictionaries, where each dictionary
                              contains 'project_id' and 'project_name'.
    """
    logger.info("Executing dummy function: get_dummy_project_info")
    dummy_data = [
        {"project_id": "dummy_id_1", "project_name": "Sales Analytics"},
        {"project_id": "dummy_id_2", "project_name": "Marketing Dashboard"},
        {"project_id": "dummy_id_3", "project_name": "HR Reports"},
        {"project_id": "dummy_id_1", "project_name": "Sales Analytics"}, # Duplicate to test uniqueness logic
        {"project_id": "dummy_id_4", "project_name": "Finance Metrics"},
        {"project_id": "dummy_5", "project_name": "Operations Overview"},
    ]
    # Simulate uniqueness logic if it were applied externally (or within the dummy logic)
    unique_set = set((d['project_id'], d['project_name']) for d in dummy_data)
    result = [{'project_id': pid, 'project_name': pname} for pid, pname in unique_set]
    logger.info(f"Dummy function returned {len(result)} unique projects.")
    return result


"""
## Example Usage and Testing

This block demonstrates how to use the `get_unique_workbook_projects` function. It will attempt to connect to your Tableau Server (using the default profile unless specified), fetch unique project folders associated with workbooks, and print them.

"""
if __name__ == "__main__":
    # Set up basic console logging for the test block if not already configured

    print("--- Running Unique Workbook Project Listing Tests ---")

    # --- Test Case 1: Fetch using default profile ---
    print("\n---")
    print("## Test Case 1: Fetching projects using the default Tableau profile")
    try:
        # Call the function without profile_name to use the default from .env
        default_profile_projects = get_unique_workbook_projects() 
        
        if default_profile_projects:
            print(f"  Successfully retrieved {len(default_profile_projects)} unique projects from default profile.")
            # Print a few examples for verification
            print("  Sample projects (first 5, sorted by name):")
            for item in sorted(default_profile_projects, key=lambda x: x['project_name'])[:5]:
                print(f"    ID: {item['project_id']}, Name: {item['project_name']}")
        else:
            print("  No unique workbook project data retrieved from default profile (may be expected if no workbooks).")
        print("Test Case 1 Status: PASSED\n")

    except Exception as e:
        print(f"Test Case 1 Status: FAILED - Error: {e}")
        logger.error(f"Test Case 1 failed: {e}", exc_info=True) # exc_info to print traceback


    # --- Test Case 2: Fetch using a specific profile (e.g., 'dev_server') ---
    # IMPORTANT: Ensure you have a 'dev_server' profile configured in your
    # config/server_profiles.yaml and corresponding PATs in your .env file.
    specific_profile_name = "dev_server" 
    print("\n---")
    print(f"## Test Case 2: Fetching projects using specific profile '{specific_profile_name}'")
    try:
        # Attempt to fetch using a specific profile
        # First, check if the profile exists in configuration before trying to connect
        if specific_profile_name not in get_available_profiles():
            print(f"  Skipping Test Case 2: Profile '{specific_profile_name}' not found in configuration. "
                  f"Please configure it in config/server_profiles.yaml and .env if you want to run this test.")
        else:
            specific_profile_projects = get_unique_workbook_projects(profile_name=specific_profile_name)

            if specific_profile_projects:
                print(f"  Successfully retrieved {len(specific_profile_projects)} unique projects from '{specific_profile_name}' profile.")
                print("  Sample projects (first 5, sorted by name):")
                for item in sorted(specific_profile_projects, key=lambda x: x['project_name'])[:5]:
                    print(f"    ID: {item['project_id']}, Name: {item['project_name']}")
            else:
                print(f"  No unique workbook project data retrieved from '{specific_profile_name}' profile (may be expected).")
            print("Test Case 2 Status: PASSED\n")

    except Exception as e:
        print(f"Test Case 2 Status: FAILED - Error: {e}")
        logger.error(f"Test Case 2 failed for profile '{specific_profile_name}': {e}", exc_info=True)


    # --- Test Case 3: Error Handling - Non-existent GraphQL Query (Conceptual) ---
    print("\n---")
    print("## Test Case 3: Error Handling for Non-existent GraphQL Query (Conceptual)")
    print("  This test relies on `load_graphql_query` raising `FileNotFoundError` or `ValueError`")
    print("  when a non-existent query name is passed to it. The `get_unique_workbook_projects`")
    print("  function hardcodes the query name, so we cannot directly test passing a bad name to it.")
    print("  However, if `load_graphql_query` (which is called internally) fails,")
    print("  `get_unique_workbook_projects` is designed to re-raise that error.")
    print("  Please ensure your `src/utils/graphql/load_graphql_query.py` handles missing files correctly.")
    print("Test Case 3 Status: CONSIDERED (Manual verification of `load_graphql_query` recommended).\n")


    # --- Test Case 4: Attempt to connect to a non-existent profile (expect failure) ---
    print("\n---")
    print("## Test Case 4: Attempting to connect to a non-existent profile (Expected Failure)")
    non_existent_profile = "non_existent_profile_xyz"
    try:
        # This will call get_tableau_server with a profile name that doesn't exist
        # and should raise a ValueError from server_connection.py
        get_unique_workbook_projects(profile_name=non_existent_profile)
        print(f"Test Case 4 Status: FAILED - Unexpectedly connected to '{non_existent_profile}'.")
    except ValueError as e:
        print(f"Test Case 4 Status: PASSED - Correctly raised ValueError for non-existent profile: {e}")
    except Exception as e:
        print(f"Test Case 4 Status: FAILED - Unexpected error type: {e}")
        logger.error(f"Test Case 4 failed for non-existent profile: {e}", exc_info=True)

    # --- Test Case 5: Test Dummy Function ---
    print("\n---")
    print("## Test Case 5: Testing the Dummy Function `get_dummy_project_info`")
    try:
        dummy_projects = get_dummy_project_info()
        if dummy_projects:
            print(f"  Successfully retrieved {len(dummy_projects)} unique projects from dummy data.")
            print("  Dummy projects (sorted by name):")
            for item in sorted(dummy_projects, key=lambda x: x['project_name']):
                print(f"    ID: {item['project_id']}, Name: {item['project_name']}")
            
            # Assertions to verify dummy data logic (optional but good practice)
            assert len(dummy_projects) == 5, "Expected 5 unique dummy projects."
            assert any(p['project_name'] == 'Sales Analytics' for p in dummy_projects), "Missing 'Sales Analytics' in dummy data."
            print("Test Case 5 Status: PASSED\n")
        else:
            print("  Test Case 5 Failed: No dummy data retrieved.")
            print("Test Case 5 Status: FAILED\n")

    except Exception as e:
        print(f"Test Case 5 Status: FAILED - Error: {e}")
        logger.error(f"Test Case 5 failed: {e}", exc_info=True)

    print("--- All Tests Finished ---")

    # --- Test Case 6: Verify all connections are cleared ---
    print("\n---")
    print("## Test Case 6: Verify All Connections Cleared")
    try:
        disconnect_tableau_server() # Disconnect all remaining connections
        # This check relies on the internal `_active_connections` dictionary
        # being accessible and cleared by disconnect_tableau_server.
        # In a production scenario, you might not expose this internal state directly.
        if not get_tableau_server.__globals__['_active_connections']: # Accessing the module-level state
            print("Test Case 6 Status: PASSED - All active connections successfully cleared.")
        else:
            print(f"Test Case 6 Status: FAILED - Some connections remain active: {list(get_tableau_server.__globals__['_active_connections'].keys())}")
    except Exception as e:
        print(f"Test Case 6 Status: FAILED - Error: {e}")
        logger.error(f"Test Case 6 failed: {e}", exc_info=True)