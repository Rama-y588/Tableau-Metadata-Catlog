"""
Tableau User Listing Module

This module provides functionality to list users from Tableau Server using both
the Metadata API (GraphQL) and the REST API. It includes a dummy function for testing
without requiring a live Tableau Server connection.
"""

import yaml
from pathlib import Path
from typing import List, Dict, Any, Optional, Set, Tuple
from src.Auth.server_connection import get_tableau_server, disconnect_tableau_server, get_available_profiles
from src.utils.logger import app_logger as logger
from src.utils.graphql.load_graphql_query import load_graphql_query
from src.utils.graphql.execute_graphql_query import execute_graphql_query
import tableauserverclient as TSC

# --- Module Constants ---
current_file = Path(__file__).resolve()
root_folder = current_file.parent.parent

def get_tableau_users(profile_name: Optional[str] = None) -> List[Dict[str, str]]:
    """
    Fetches user information from Tableau Server using the REST API.
    
    Args:
        profile_name (Optional[str]): The name of the Tableau Server profile to use.
                                     If None, the default profile will be used.
    
    Returns:
        List[Dict[str, str]]: A list of dictionaries containing user information:
            - id: User's unique identifier
            - name: User's full name
            - email: User's email address
            - site_role: User's role in the site
            - last_login: Last login timestamp
    
    Raises:
        ConnectionError: If connection to Tableau Server fails
        ValueError: If server object is invalid
        Exception: For other unexpected errors
    """
    logger.info(f"Attempting to fetch user information from Tableau Server")
    server: Optional[TSC.Server] = None
    
    try:
        server = get_tableau_server(profile_name=profile_name)
        logger.debug("Successfully connected to Tableau Server")
        
        # Get all users
        all_users = list(server.users.get())
        users_list = []
        
        for user in all_users:
            user_info = {
                'id': user.id,
                'name': user.name,
                'email': user.email,
                'site_role': user.site_role,
                'last_login': user.last_login.isoformat() if user.last_login else 'Never'
            }
            users_list.append(user_info)
            logger.debug(f"Found user: {user.name} ({user.email})")
        
        logger.info(f"Successfully retrieved {len(users_list)} users")
        return users_list
        
    except Exception as e:
        logger.error(f"Error fetching users: {str(e)}")
        raise
    finally:
        if server:
            disconnect_tableau_server(profile_name)
            logger.debug(f"Disconnected from Tableau Server profile: {profile_name}")

def get_dummy_users() -> List[Dict[str, str]]:
    """
    Returns dummy user data for testing purposes.
    
    Returns:
        List[Dict[str, str]]: A list of dictionaries containing dummy user information
    """
    logger.info("Executing dummy function: get_dummy_users")
    dummy_data = [
        {
            "id": "user1",
            "name": "John Doe",
            "email": "john.doe@example.com",
            "site_role": "Creator",
            "last_login": "2024-03-15T10:30:00"
        },
        {
            "id": "user2",
            "name": "Jane Smith",
            "email": "jane.smith@example.com",
            "site_role": "Explorer",
            "last_login": "2024-03-14T15:45:00"
        },
        {
            "id": "user3",
            "name": "Bob Johnson",
            "email": "bob.johnson@example.com",
            "site_role": "Viewer",
            "last_login": "2024-03-13T09:15:00"
        },
        {
            "id": "user4",
            "name": "Alice Brown",
            "email": "alice.brown@example.com",
            "site_role": "Creator",
            "last_login": "2024-03-12T14:20:00"
        },
        {
            "id": "user5",
            "name": "Charlie Wilson",
            "email": "charlie.wilson@example.com",
            "site_role": "Explorer",
            "last_login": "2024-03-11T11:10:00"
        }
    ]
    logger.info(f"Dummy function returned {len(dummy_data)} users")
    return dummy_data

def format_user_output(users: List[Dict[str, str]]) -> str:
    """
    Formats user information for display.
    
    Args:
        users (List[Dict[str, str]]): List of user dictionaries
    
    Returns:
        str: Formatted string containing user information
    """
    output = []
    output.append("\nUser Information:")
    output.append("-" * 80)
    output.append(f"{'Name':<20} {'Email':<30} {'Role':<15} {'Last Login':<20}")
    output.append("-" * 80)
    
    for user in sorted(users, key=lambda x: x['name']):
        output.append(
            f"{user['name']:<20} "
            f"{user['email']:<30} "
            f"{user['site_role']:<15} "
            f"{user['last_login']:<20}"
        )
    
    output.append("-" * 80)
    output.append(f"Total Users: {len(users)}")
    return "\n".join(output)

# --- Test Code ---
if __name__ == "__main__":
    print("--- Running Tableau User Listing Tests ---")
    
    # --- Test Case 1: Dummy Data ---
    print("\nTest Case 1: Testing with dummy data...")
    try:
        dummy_users = get_dummy_users()
        print(format_user_output(dummy_users))
        print("Test Case 1 Status: PASSED\n")
    except Exception as e:
        print(f"Test Case 1 Status: FAILED - Error: {e}")
        logger.error(f"Test Case 1 failed: {e}", exc_info=True)
    
    # --- Test Case 2: Live Server (Default Profile) ---
    print("\nTest Case 2: Testing with live server (default profile)...")
    try:
        live_users = get_tableau_users()
        print(format_user_output(live_users))
        print("Test Case 2 Status: PASSED\n")
    except Exception as e:
        print(f"Test Case 2 Status: FAILED - Error: {e}")
        logger.error(f"Test Case 2 failed: {e}", exc_info=True)
    
    # --- Test Case 3: Specific Profile ---
    specific_profile = "dev_server"
    print(f"\nTest Case 3: Testing with specific profile '{specific_profile}'...")
    try:
        if specific_profile not in get_available_profiles():
            print(f"  Skipping Test Case 3: Profile '{specific_profile}' not found in configuration.")
        else:
            profile_users = get_tableau_users(profile_name=specific_profile)
            print(format_user_output(profile_users))
            print("Test Case 3 Status: PASSED\n")
    except Exception as e:
        print(f"Test Case 3 Status: FAILED - Error: {e}")
        logger.error(f"Test Case 3 failed: {e}", exc_info=True)
    
    # --- Test Case 4: Error Handling ---
    print("\nTest Case 4: Testing error handling with non-existent profile...")
    try:
        get_tableau_users(profile_name="non_existent_profile")
        print("Test Case 4 Status: FAILED - Unexpectedly connected to non-existent profile")
    except ValueError as e:
        print(f"Test Case 4 Status: PASSED - Correctly raised ValueError: {e}")
    except Exception as e:
        print(f"Test Case 4 Status: FAILED - Unexpected error type: {e}")
        logger.error(f"Test Case 4 failed: {e}", exc_info=True)
    
    print("\n--- All Tests Finished ---")
