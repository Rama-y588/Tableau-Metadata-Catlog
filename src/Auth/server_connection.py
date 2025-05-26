"""
Tableau Server Connection Module

This module provides functionality for managing connections to Tableau Server instances.
It supports multiple server profiles, connection pooling, and automatic session management.

Key Features:
- Multiple server profile support with YAML configuration
- Environment-based credential management
- Connection pooling and session reuse
- Automatic retry mechanism for failed connections
- Context manager for safe connection handling
- Detailed logger of all operations

Configuration:
    The module requires two configuration files:
    1. server_profiles.yaml: Contains server profiles and connection settings
    2. .env: Contains sensitive credentials and default profile selection

Environment Variables:
    Required:
    - TABLEAU_PROFILE: Default server profile to use
    - TABLEAU_PAT_NAME: Default Personal Access Token name
    - TABLEAU_PAT_SECRET: Default Personal Access Token secret

    Optional (for profile-specific credentials):
    - TABLEAU_PAT_NAME_{PROFILE}: Profile-specific PAT name
    - TABLEAU_PAT_SECRET_{PROFILE}: Profile-specific PAT secret
    - LOG_LEVEL: logger level (DEBUG, INFO, WARNING, ERROR, CRITICAL)

Usage:
    Basic usage with default profile:
    >>> server = get_tableau_server()
    >>> # Use server object
    >>> disconnect_tableau_server()

    Using a specific profile:
    >>> server = get_tableau_server("development")
    >>> # Use server object
    >>> disconnect_tableau_server("development")

    Using context manager:
    >>> with TableauServerConnection("development") as server:
    >>>     # Use server object
    >>>     # Connection automatically closed after block

Exceptions:
    - FileNotFoundError: Configuration file not found
    - ValueError: Invalid configuration or missing required settings
    - ConnectionError: Failed to connect to Tableau Server
    - RuntimeError: Internal state error

Author: Your Name
Version: 1.0.0
"""

import os
import yaml
import time
from pathlib import Path
from typing import Optional, Dict
from dataclasses import dataclass
import tableauserverclient as TSC
from dotenv import load_dotenv
from src.utils.logger import app_logger as logger

# --- Module Constants ---
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]

# --- Data Classes ---
@dataclass
class ServerProfile:
    """
    Data class to hold parsed server profile information and credentials.
    
    Attributes:
        name (str): Unique identifier for the profile
        url (str): Tableau Server URL
        site_name (str): Tableau site name
        api_version (str): Tableau API version
        description (str): Human-readable description of the profile
        pat_name (str): Personal Access Token name
        pat_secret (str): Personal Access Token secret
    """
    name: str
    url: str
    site_name: str
    api_version: str
    description: str = ""
    pat_name: str = ""  # Will be populated from environment
    pat_secret: str = "" # Will be populated from environment

# --- Module-Level State ---
_profiles: Dict[str, ServerProfile] = {}
_default_profile_name: Optional[str] = None
_connection_settings: Dict[str, int] = {}
_active_connections: Dict[str, TSC.Server] = {} # Store active connections

def _load_config_and_env() -> None:
    """
    Loads server profiles from YAML and credentials/default profile from .env.
    This function is called only once when the module is first imported.
    
    Raises:
        FileNotFoundError: If configuration file is not found
        ValueError: If configuration is invalid or missing required settings
        Exception: For other configuration loading errors
    """
    global _profiles, _default_profile_name, _connection_settings

    config_path = root_folder / 'config' / 'server_profiles.yaml'
    if not config_path.exists():
        logger.error(f"Configuration file not found at: {config_path}")
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    try:
        with open(config_path, 'r') as f:
            config_data = yaml.safe_load(f)

        if not isinstance(config_data, dict):
            logger.error("Invalid YAML config format. Expected a dictionary.")
            raise ValueError("Invalid YAML config format. Expected a dictionary.")

        _connection_settings = {
            'retry_attempts': config_data.get('connection', {}).get('retry_attempts', 3),
            'retry_delay': config_data.get('connection', {}).get('retry_delay', 5)
        }
        logger.debug(f"Loaded connection settings: {_connection_settings}")

        for name, profile_data in config_data.items():
            if name in ['logger', 'connection']: # Skip non-profile sections
                continue
            
            if not isinstance(profile_data, dict):
                logger.warning(f"Skipping malformed profile '{name}'. Expected dictionary.")
                continue

            try:
                profile = ServerProfile(
                    name=name,
                    url=profile_data['url'],
                    site_name=profile_data['site_name'],
                    api_version=profile_data.get('api_version', '3.19'),
                    description=profile_data.get('description', f"Tableau Server profile for {name}")
                )
                _profiles[name] = profile
                logger.debug(f"Loaded profile '{name}': {profile}")
            except KeyError as e:
                logger.error(f"Missing required key '{e}' in profile '{name}' in config file.")
                raise ValueError(f"Missing required key '{e}' in profile '{name}' in config file.")

        if not _profiles:
            logger.error("No valid server profiles found in configuration.")
            raise ValueError("No valid server profiles found in configuration.")

        logger.info(f"Successfully loaded {len(_profiles)} server profiles from YAML.")

    except yaml.YAMLError as e:
        logger.error(f"Error reading YAML config file: {str(e)}")
        raise ValueError(f"Error reading YAML config file: {str(e)}")
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        raise Exception(f"Failed to load configuration: {str(e)}")

    # Load environment variables
    env_path = root_folder / '.env'
    if not env_path.exists():
        logger.warning(f".env file not found at: {env_path}. Credentials must be set as system environment variables.")
    else:
        load_dotenv(env_path)
        logger.info(f"Successfully loaded environment variables from {env_path}")

    # Get default profile name directly from env
    _default_profile_name = os.getenv('TABLEAU_PROFILE')
    if not _default_profile_name:
        logger.error("Environment variable 'TABLEAU_PROFILE' not set. Please specify a default profile.")
        raise ValueError("Environment variable 'TABLEAU_PROFILE' not set. Please specify a default profile.")
    
    if _default_profile_name not in _profiles:
        logger.error(
            f"Default profile '{_default_profile_name}' from TABLEAU_PROFILE "
            f"not found in configured profiles: {list(_profiles.keys())}"
        )
        raise ValueError(
            f"Default profile '{_default_profile_name}' from TABLEAU_PROFILE "
            f"not found in configured profiles: {list(_profiles.keys())}"
        )
    logger.info(f"Default Tableau profile selected from .env: '{_default_profile_name}'.")

    # Populate PAT credentials into ServerProfile objects
    for profile_name, profile in _profiles.items():
        pat_name_env_var = f'TABLEAU_PAT_NAME_{profile_name.upper()}'
        pat_secret_env_var = f'TABLEAU_PAT_SECRET_{profile_name.upper()}'
        
        # Prioritize profile-specific env vars, then generic ones
        profile.pat_name = os.getenv(pat_name_env_var) or os.getenv('TABLEAU_PAT_NAME')
        profile.pat_secret = os.getenv(pat_secret_env_var) or os.getenv('TABLEAU_PAT_SECRET')

        if not profile.pat_name or not profile.pat_secret:
            logger.error(
                f"Missing Personal Access Token credentials for profile '{profile_name}'. "
                f"Please set '{pat_name_env_var}' and '{pat_secret_env_var}' "
                f"(or generic 'TABLEAU_PAT_NAME' and 'TABLEAU_PAT_SECRET') in your .env file or environment variables."
            )
            raise ValueError(
                f"Missing Personal Access Token credentials for profile '{profile_name}'. "
                f"Please set '{pat_name_env_var}' and '{pat_secret_env_var}' "
                f"(or generic 'TABLEAU_PAT_NAME' and 'TABLEAU_PAT_SECRET') in your .env file or environment variables."
            )
        logger.debug(f"Loaded PAT credentials for profile '{profile_name}'")
    
    logger.info("Successfully loaded Tableau PAT credentials for all profiles.")

# Execute configuration loading once when the module is imported
_load_config_and_env()

def get_tableau_server(profile_name: Optional[str] = None) -> TSC.Server:
    """
    Establishes and returns a connection to a Tableau Server instance.
    If profile_name is None, it uses the default profile from the TABLEAU_PROFILE env var.

    Connections are cached and reused if already active.

    Args:
        profile_name: The name of the server profile to connect to.
                      If None, the default profile from the configuration will be used.

    Returns:
        TSC.Server: A connected Tableau Server instance.

    Raises:
        FileNotFoundError: If the configuration file is not found during initial load.
        ValueError: If there's an issue with configuration parsing,
                    missing environment variables, or unknown profile names.
        ConnectionError: If connection to the Tableau Server fails after retries.
    """
    target_profile_name = profile_name if profile_name else _default_profile_name
    
    if not target_profile_name:
        logger.error("No profile name provided and no default profile configured via TABLEAU_PROFILE.")
        raise ValueError("No profile name provided and no default profile configured via TABLEAU_PROFILE.")
    
    profile_data = _profiles.get(target_profile_name)
    if not profile_data:
        logger.error(f"Profile '{target_profile_name}' not found in configuration.")
        raise ValueError(f"Profile '{target_profile_name}' not found in configuration.")

    # Check if already connected to this profile and session is active
    if target_profile_name in _active_connections:
        server = _active_connections[target_profile_name]
        try:
            # Attempt a simple call to verify active session
            server.users.get() 
            logger.info(f"Already connected and session active for profile: '{target_profile_name}'. Reusing existing connection.")
            return server
        except TSC.ServerResponseError as e:
            logger.warning(f"Session for profile '{target_profile_name}' seems inactive or expired: {e}. Attempting to reconnect.")
        except Exception as e:
            logger.warning(f"Unexpected error checking session for profile '{target_profile_name}': {e}. Attempting to reconnect.")

    if not profile_data.pat_name or not profile_data.pat_secret:
        logger.error(f"PAT credentials not loaded for profile '{target_profile_name}'.")
        raise ValueError(f"PAT credentials not loaded for profile '{target_profile_name}'.")

    logger.info(f"Attempting to connect to Tableau Server profile: '{target_profile_name}' at {profile_data.url}")

    server = TSC.Server(profile_data.url, use_server_version=True)
    tableau_auth = TSC.PersonalAccessTokenAuth(
        token_name=profile_data.pat_name,
        personal_access_token=profile_data.pat_secret,
        site_id=profile_data.site_name
    )

    retry_attempts = _connection_settings.get('retry_attempts', 3)
    retry_delay = _connection_settings.get('retry_delay', 5)

    for attempt in range(retry_attempts):
        try:
            server.auth.sign_in(tableau_auth)
            logger.info(
                f"Successfully connected to '{target_profile_name}' server "
                f"(site: '{profile_data.site_name}', version: {server.version})"
            )
            _active_connections[target_profile_name] = server # Cache the connection
            return server
        except Exception as e:
            logger.warning(
                f"Connection attempt {attempt + 1}/{retry_attempts} failed for '{target_profile_name}': {e}"
            )
            if attempt < retry_attempts - 1:
                logger.info(f"Waiting {retry_delay} seconds before next attempt...")
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect to '{target_profile_name}' after {retry_attempts} attempts.")
                raise ConnectionError(f"Failed to connect to Tableau Server profile '{target_profile_name}': {e}")
    
    # This line should ideally not be reached if exceptions are always raised on failure
    logger.error(f"Unknown error: Failed to connect to '{target_profile_name}'.")
    raise ConnectionError(f"Unknown error: Failed to connect to '{target_profile_name}'.")

def disconnect_tableau_server(profile_name: Optional[str] = None) -> None:
    """
    Disconnects from a specific Tableau Server profile or all active connections.

    Args:
        profile_name: Name of the profile to disconnect from. If None, disconnects from all profiles.
    """
    if profile_name:
        if profile_name in _active_connections:
            server = _active_connections[profile_name]
            try:
                if server.auth.is_signed_in():
                    server.auth.sign_out()
                    logger.info(f"Successfully signed out from '{profile_name}' server.")
                else:
                    logger.info(f"Server '{profile_name}' was not signed in.")
            except Exception as e:
                logger.warning(f"Error during sign out from '{profile_name}': {e}")
            finally:
                del _active_connections[profile_name]
        else:
            logger.info(f"No active connection found for profile '{profile_name}'.")
    else:
        # Disconnect from all active servers
        for name, server in list(_active_connections.items()):
            try:
                if server.auth.is_signed_in():
                    server.auth.sign_out()
                    logger.info(f"Successfully signed out from '{name}' server.")
                else:
                    logger.info(f"Server '{name}' was not signed in.")
            except Exception as e:
                logger.warning(f"Error during sign out from '{name}': {e}")
        _active_connections.clear()
        logger.info("All Tableau Server connections disconnected.")


class TableauServerConnection:
    """
    Context manager for Tableau Server connections.
    Ensures connection is signed out upon exiting the 'with' block.

    Example:
        >>> with TableauServerConnection("development") as server:
        >>>     # Use server object
        >>>     # Connection automatically closed after block
    """
    def __init__(self, profile_name: Optional[str] = None):
        """
        Initialize the context manager.

        Args:
            profile_name: Name of the server profile to connect to.
                         If None, uses the default profile.
        """
        self.profile_name = profile_name
        self.server: Optional[TSC.Server] = None
        logger.debug(f"Initialized TableauServerConnection for profile: {profile_name}")

    def __enter__(self) -> TSC.Server:
        """
        Establishes connection when entering the context.

        Returns:
            TSC.Server: Connected Tableau Server instance.
        """
        self.server = get_tableau_server(self.profile_name)
        logger.debug(f"Entered context for profile: {self.profile_name}")
        return self.server

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """
        Ensures connection is closed when exiting the context.
        """
        if self.server:
            logger.debug(f"Exiting context for profile: {self.profile_name}")
            # Pass the specific profile name to disconnect only that one,
            # especially important if 'self.profile_name' was explicitly set.
            disconnect_tableau_server(self.profile_name)

def get_available_profiles() -> Dict[str, str]:
    """
    Returns names and descriptions of all configured Tableau server profiles.

    Returns:
        Dict[str, str]: Dictionary mapping profile names to their descriptions.
    """
    return {name: profile.description for name, profile in _profiles.items()}

def get_default_profile_name() -> str:
    """
    Returns the name of the default profile as determined by TABLEAU_PROFILE env var.

    Returns:
        str: Name of the default profile.

    Raises:
        RuntimeError: If default profile name was not initialized.
    """
    if not _default_profile_name:
        logger.error("Default profile name was not initialized. Check _load_config_and_env.")
        raise RuntimeError("Default profile name was not initialized. Check _load_config_and_env.")
    return _default_profile_name

# # --- Test Code (Executed only when script is run directly) ---
# if __name__ == "__main__":
#     print("--- Running Tableau Server Connection Tests ---")


#     # --- Test 1: Get available profiles and default profile ---
#     try:
#         print("\nTest 1: Available Profiles & Default")
#         available_profiles = get_available_profiles()
#         print(f"Available profiles: {available_profiles}")
#         default_profile = get_default_profile_name()
#         print(f"Default profile: {default_profile}")
#         if not available_profiles:
#             raise ValueError("No profiles loaded. Check config/server_profiles.yaml")
#         if not default_profile:
#             raise ValueError("Default profile not loaded. Check .env TABLEAU_PROFILE")
#         print("Test 1 Passed: Profiles and default profile retrieved successfully.")
#     except Exception as e:
#         print(f"Test 1 Failed: {e}")
#         logger.error(f"Test 1 Failed: {e}")

#     # --- Test 2: Connect to default profile directly ---
#     default_server_obj = None
#     try:
#         print("\nTest 2: Connect to Default Profile Directly")
#         default_server_obj = get_tableau_server()
#         print(f"Connected to default server: {default_server_obj.baseurl}, site: '{default_server_obj.site_id}'")
#         # Example: Try to get some data to confirm active session
#         logger.info("Attempting to fetch current user info...")
#         current_user = default_server_obj.users.get_current()
#         print(f"Current user on default server: {current_user.name}")
#         print("Test 2 Passed: Connected and session active.")
#     except Exception as e:
#         print(f"Test 2 Failed: {e}")
#         logger.error(f"Test 2 Failed: {e}")
#     finally:
#         if default_server_obj:
#             disconnect_tableau_server(get_default_profile_name())
#             print(f"Disconnected from default profile ('{get_default_profile_name()}').")

#     # --- Test 3: Connect to a specific profile using context manager ---
#     print("\nTest 3: Connect to Specific Profile via Context Manager")
#     test_profile_name = "dev_server" # Ensure this profile exists in your config/env
#     try:
#         if test_profile_name not in get_available_profiles():
#             raise ValueError(f"Skipping test 3: Profile '{test_profile_name}' not found in configuration.")

#         with TableauServerConnection(profile_name=test_profile_name) as server_dev:
#             print(f"Connected to '{test_profile_name}': {server_dev.baseurl}, site: '{server_dev.site_id}'")
#             # Example: Try to fetch some data
#             logger.info(f"Attempting to fetch projects from '{test_profile_name}'...")
#             all_projects = list(server_dev.projects.get())
#             print(f"Found {len(all_projects)} projects on '{test_profile_name}'.")
#         print(f"Test 3 Passed: Successfully connected via context manager and disconnected.")
#     except Exception as e:
#         print(f"Test 3 Failed: {e}")
#         logger.error(f"Test 3 Failed: {e}")

#     # --- Test 4: Attempt to connect to a non-existent profile (expect failure) ---
#     print("\nTest 4: Connect to Non-existent Profile (Expected Failure)")
#     try:
#         get_tableau_server(profile_name="non_existent_profile")
#         print("Test 4 Failed: Connected to a non-existent profile (should have failed).")
#     except ValueError as e:
#         print(f"Test 4 Passed: Correctly raised ValueError for non-existent profile: {e}")
#     except Exception as e:
#         print(f"Test 4 Failed: Unexpected error type for non-existent profile: {e}")

#     # --- Test 5: Verify all connections are cleared ---
#     print("\nTest 5: Verify All Connections Cleared")
#     try:
#         disconnect_tableau_server() # Disconnect all remaining connections
#         if not _active_connections:
#             print("Test 5 Passed: All active connections successfully cleared.")
#         else:
#             print(f"Test 5 Failed: Some connections remain active: {list(_active_connections.keys())}")
#     except Exception as e:
#         print(f"Test 5 Failed: {e}")
#         logger.error(f"Test 5 Failed: {e}")

#     print("\n--- Tableau Server Connection Tests Finished ---")