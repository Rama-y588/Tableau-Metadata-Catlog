from datetime import datetime
from typing import List, Dict, Set, Tuple, Optional, Any
import tableauserverclient as TSC
from src.Auth.server_connection import get_tableau_server, disconnect_tableau_server
from src.utils.logger import app_logger as logger
from src.utils.graphql.load_graphql_query import load_graphql_query
from src.utils.graphql.execute_graphql_query import execute_graphql_query
import json
from pathlib import Path

current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]

def get_unique_users(profile_name: Optional[str] = None) -> List[Dict[str, Optional[str]]]:
    """
    Fetches unique user details (specifically, owners of workbooks) from the Tableau Server Metadata API.
    """
    query_name = "master_query"  # Assuming this query fetches workbook data including owner details
    logger.info(f"Initiating fetch for unique user details using GraphQL query: '{query_name}'")

    try:
        query = load_graphql_query(query_name)
    except (FileNotFoundError, ValueError) as e:
        logger.critical(f"Fatal error: Failed to load GraphQL query '{query_name}'. Details: {e}")
        raise

    unique_users_set: Set[Tuple[Optional[str], Optional[str], Optional[str], Optional[str], Optional[str], Optional[str]]] = set()
    server: Optional[TSC.Server] = None

    try:
        # server = get_tableau_server(profile_name=profile_name)
        logger.debug(f"Executing Tableau Metadata API query:\n{query}")
        sample_data_path = root_folder / 'sample_data' / 'data_test.json'
        with open(sample_data_path, 'r') as f:
            resp = json.load(f)

        # resp = execute_graphql_query(server, query)

        if 'errors' in resp:
            error_messages = [err.get('message', 'Unknown GraphQL error') for err in resp['errors']]
            logger.error(f"GraphQL query execution failed with errors: {'; '.join(error_messages)}")
            raise Exception(f"Tableau Metadata API GraphQL errors: {'; '.join(error_messages)}")

        # Fix: Properly navigate the GraphQL response structure
        workbooks_data = resp.get('data', {}).get('workbooks', {})

        if not workbooks_data:
            logger.info("No workbooks found or the Tableau Metadata API query returned no data.")
            return []

        for workbook in workbooks_data:
            owner = workbook.get('owner')
            if not owner:
                workbook_name = workbook.get('name', 'Unnamed Workbook')
                workbook_id = workbook.get('id', 'Unknown ID')
                logger.warning(f"Workbook '{workbook_name}' (ID: {workbook_id}) has no owner. Skipping.")
                continue

            user_tuple = (
                owner.get('id'),
                owner.get('name'),
                owner.get('username'),
                owner.get('email'),
                owner.get("createdAt"),
                owner.get("updatedAt")
            )

            # Only add if at least one identifying piece of information exists for the owner
            if any(user_tuple[:4]):  # Check id, name, username, email for 'any' value
                unique_users_set.add(user_tuple)
            else:
                workbook_name = workbook.get('name', 'Unnamed Workbook')
                workbook_id = workbook.get('id', 'Unknown ID')
                logger.warning(
                    f"Workbook '{workbook_name}' (ID: {workbook_id}) found with owner having all primary identifying fields missing. Skipping this owner entry."
                )

        logger.info(f"Successfully identified {len(unique_users_set)} unique user entries from workbooks.")

        # Convert the set of tuples back to a list of dictionaries.
        result_list = [
            {
                'id': user[0] or "",
                'name': user[1] or "",
                'username': user[2] or "",
                'email': user[3] or "",
                'created_at': user[4] or "",
                'updated_at': user[5] or ""
            }
            for user in unique_users_set
        ]

        return result_list

    except ConnectionError as e:
        logger.error(f"Failed to establish connection to Tableau Server. Details: {e}")
        raise
    except Exception as e:
        logger.error(f"An unexpected error occurred while fetching unique user details. Details: {e}", exc_info=True)
        raise
    finally:
        if server:
            disconnect_tableau_server(server)
            logger.debug(f"Successfully disconnected from Tableau Server profile: {profile_name if profile_name else 'default'}")