from src.Auth.server_connection import get_tableau_server,disconnect_tableau_server
from src.utils.logger import app_logger as logger
from typing import Dict, Any, Optional

"""

### New Function for Query Execution

"""
def execute_graphql_query(query: str, profile_name: Optional[str] = None) -> Dict[str, Any]:
    """
    Executes a given GraphQL query against Tableau Server's Metadata API.

    Args:
        query (str): The GraphQL query string to execute.
        profile_name (Optional[str]): The name of the Tableau Server profile to use.

    Returns:
        Dict[str, Any]: The raw JSON response from the Tableau Metadata API.

    Raises:
        ConnectionError: If connecting to Tableau Server fails.
        Exception: If the GraphQL query execution returns errors or any other
                   unexpected issue occurs during the API call.
    """
    server = None
    try:
        server = get_tableau_server(profile_name)
        logger.debug(f"Executing Tableau Metadata API query for profile '{profile_name or 'default'}':\n{query}")
        resp = server.metadata.query(query)
        
        # Check for GraphQL errors in the response before returning
        if 'errors' in resp:
            error_messages = [err.get('message', 'Unknown error') for err in resp['errors']]
            logger.error(f"GraphQL query returned errors: {'; '.join(error_messages)}")
            raise Exception(f"GraphQL query errors: {'; '.join(error_messages)}")

        logger.debug("GraphQL query executed successfully.")
        return resp

    except ConnectionError as e:
        logger.error(f"Failed to connect to Tableau Server to execute GraphQL query: {e}")
        raise ConnectionError(f"Tableau Server connection failed: {e}")
    except Exception as e:
        logger.error(f"An unexpected error occurred during GraphQL query execution: {e}", exc_info=True)
        raise
    finally:
        if server:
            disconnect_tableau_server(profile_name)
            logger.debug(f"Disconnected from Tableau Server profile: {profile_name or 'default'}")