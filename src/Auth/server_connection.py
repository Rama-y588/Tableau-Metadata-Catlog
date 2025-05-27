import os
import yaml
import time
import logging
from pathlib import Path
from typing import Dict
import tableauserverclient as TSC
from dotenv import load_dotenv
from src.utils.logger import app_logger as logger 

# --- Module Constants ---
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]  # Adjust if your project structure differs


# --- Module-Level State ---
_profiles: Dict[str, Dict] = {}
_default_profile_name: str = ""  # Guaranteed to be set after _load_config_and_env
_connection_settings: Dict[str, int] = {}

def _load_config_and_env() -> None:
    """
    Loads server profiles from YAML and credentials/default profile from .env.
    This runs once when the module is imported.
    """
    global _profiles, _default_profile_name, _connection_settings

    config_path = root_folder / 'config' / 'server_profiles.yaml'
    if not config_path.exists():
        logger.error(f"Configuration file not found at: {config_path}")
        raise FileNotFoundError(f"Configuration file not found at: {config_path}")

    logger.info(f"Loading configuration from: {config_path}")
    with open(config_path, 'r') as f:
        config_data = yaml.safe_load(f)

    if not isinstance(config_data, dict):
        logger.error("Invalid YAML config format. Expected a dictionary.")
        raise ValueError("Invalid YAML config format. Expected a dictionary.")

    _connection_settings = {
        'retry_attempts': config_data.get('connection', {}).get('retry_attempts', 3),
        'retry_delay': config_data.get('connection', {}).get('retry_delay', 5)
    }

    for name, profile_data in config_data.items():

        
        if name == 'connection':
            continue
        if not isinstance(profile_data, dict):
            continue
        _profiles[name] = profile_data

    if not _profiles:
        logger.error("No valid server profiles found in configuration.")
        raise ValueError("No valid server profiles found in configuration.")

    env_path = root_folder / '.env'
    if env_path.exists():
        logger.info(f"Loading environment variables from: {env_path}")
        load_dotenv(env_path)
    else:
        logger.warning(".env file not found; relying on environment variables.")

    default_env_profile = os.getenv('TABLEAU_PROFILE')
    if default_env_profile:
        if default_env_profile in _profiles:
            _default_profile_name = default_env_profile
        else:
            logger.error(f"Default profile '{default_env_profile}' from env not found in config.")
            raise ValueError(
                f"Default profile '{default_env_profile}' from TABLEAU_PROFILE env var "
                f"not found in configured profiles: {list(_profiles.keys())}"
            )
    elif "default" in _profiles:
        _default_profile_name = "default"
    else:
        logger.error("No default profile specified and no 'default' in config.")
        raise ValueError(
            "No default profile specified. Set 'TABLEAU_PROFILE' env var or "
            "define a 'default' profile in server_profiles.yaml."
        )

    for profile_name, profile_data in _profiles.items():
        pat_name_env_var = f'TABLEAU_PAT_NAME_{profile_name.upper()}'
        pat_secret_env_var = f'TABLEAU_PAT_SECRET_{profile_name.upper()}'

        profile_data['pat_name'] = os.getenv(pat_name_env_var) or os.getenv('TABLEAU_PAT_NAME')
        profile_data['pat_secret'] = os.getenv(pat_secret_env_var) or os.getenv('TABLEAU_PAT_SECRET')

        if not profile_data['pat_name'] or not profile_data['pat_secret']:
            logger.error(f"Missing PAT credentials for profile '{profile_name}'")
            raise ValueError(
                f"Missing Personal Access Token credentials for profile '{profile_name}'. "
                f"Please set '{pat_name_env_var}' and '{pat_secret_env_var}' "
                f"(or generic 'TABLEAU_PAT_NAME' and 'TABLEAU_PAT_SECRET') in your .env file or environment variables."
            )

    logger.info(f"Loaded Tableau profiles: {list(_profiles.keys())}")
    logger.info(f"Default Tableau profile: {_default_profile_name}")

_load_config_and_env()


def get_tableau_server() -> TSC.Server:
    """
    Connects to the default Tableau Server instance determined during module initialization.
    """
    target_profile_name = _default_profile_name
    profile_data = _profiles.get(target_profile_name)

    if not profile_data:
        logger.error(f"Profile '{target_profile_name}' missing after config load.")
        raise ValueError(f"Default profile '{target_profile_name}' not found after initialization.")

    server = TSC.Server(profile_data['url'], use_server_version=True)
    tableau_auth = TSC.PersonalAccessTokenAuth(
        token_name=profile_data['pat_name'],
        personal_access_token=profile_data['pat_secret'],
        site_id=profile_data['site_name']
    )

    retry_attempts = _connection_settings.get('retry_attempts', 3)
    retry_delay = _connection_settings.get('retry_delay', 5)

    for attempt in range(retry_attempts):
        try:
            server.auth.sign_in(tableau_auth)
            logger.info(f"Successfully connected to Tableau Server: '{target_profile_name}'")
            return server
        except Exception as e:
            logger.warning(f"Connection attempt {attempt + 1}/{retry_attempts} failed: {e}")
            if attempt < retry_attempts - 1:
                time.sleep(retry_delay)
            else:
                logger.error(f"Failed to connect after {retry_attempts} attempts.")
                raise ConnectionError(f"Failed to connect to Tableau Server profile '{target_profile_name}': {e}")

    raise ConnectionError(f"Unknown error connecting to '{target_profile_name}'")


def disconnect_tableau_server(server: TSC.Server) -> None:
    """
    Disconnects from a given Tableau Server instance.
    """
    try:
        if server.auth.is_signed_in():
            server.auth.sign_out()
            logger.info("Successfully signed out from Tableau Server.")
        else:
            logger.warning("Server was not signed in.")
    except Exception as e:
        logger.error(f"Error during sign out: {e}")


if __name__ == "__main__":
    logger.info(f"Running test in file: {current_file.name}")

    try:
        logger.info("Attempting to connect to Tableau Server using default profile...")
        server = get_tableau_server()
        logger.info("Connected successfully. Listing available workbooks...")

        all_workbooks, pagination_item = server.workbooks.get()
        logger.info(f"Found {pagination_item.total_available} workbooks.")

        for wb in all_workbooks[:5]:  # Display first 5 workbooks
            logger.info(f"Workbook: {wb.name} (ID: {wb.id})")

        disconnect_tableau_server(server)

    except Exception as e:
        logger.exception(f"Error during Tableau Server test: {e}")
