from datetime import datetime
from typing import List, Dict, Set, Tuple, Optional, Any
from pathlib import Path
import json

from src.utils.logger import app_logger as logger

# --- Module Constants ---
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]
MODULE_NAME = current_file.name

class UserData:
    """Data class to store user information"""
    id: str
    name: str
    username: str
    email: str

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'UserData':
        """Create UserData instance from dictionary"""
        return cls(
            id=str(data.get('id', '')),
            name=str(data.get('name', '')),
            username=str(data.get('username', '')),
            email=str(data.get('email', ''))
        )

    def to_dict(self) -> Dict[str, str]:
        """Convert UserData instance to dictionary"""
        return {
            'id': self.id,
            'name': self.name,
            'username': self.username,
            'email': self.email
        }

def get_users(raw_data: Dict[str, Any]) -> List[Dict[str, str]]:
    """
    Extracts user information from the provided raw JSON data.

    Args:
        raw_data (Dict[str, Any]): Dictionary containing user data.

    Returns:
        List[Dict[str, str]]: A list of dictionaries containing user information:
            - id: User's unique identifier
            - name: User's name
            - username: User's username
            - email: User's email

    Raises:
        Exception: For unexpected errors during parsing.
    """
    start_time = datetime.now()
    logger.info(f"[{MODULE_NAME}] Starting user extraction process")

    try:
        users_list = []
        all_workbooks_raw = raw_data.get('data', {}).get('workbooks', [])
        
        if not all_workbooks_raw:
            logger.warning(f"[{MODULE_NAME}] No workbooks found in raw data")
            return users_list

        # Use a set to track unique users by their ID
        unique_users = set()

        for workbook_raw in all_workbooks_raw:
            owner = workbook_raw.get('owner', {})
            if not owner:
                workbook_name = workbook_raw.get('name', 'Unnamed Workbook')
                workbook_id = workbook_raw.get('id', 'Unknown ID')
                logger.warning(f"[{MODULE_NAME}] Workbook '{workbook_name}' (ID: {workbook_id}) has no owner. Skipping.")
                continue

            user_id = owner.get('id')
            if not user_id or user_id in unique_users:
                continue

            user_info = {
                'id': str(owner.get('id', '')),
                'name': str(owner.get('name', '')),
                'username': str(owner.get('username', '')),
                'email': str(owner.get('email', ''))
            }

            # Only add if at least one identifying field exists
            if any([user_info['id'], user_info['name'], user_info['username'], user_info['email']]):
                users_list.append(user_info)
                unique_users.add(user_id)
                logger.debug(f"[{MODULE_NAME}] Found user: {user_info['name']} (ID: {user_info['id']})")
            else:
                workbook_name = workbook_raw.get('name', 'Unnamed Workbook')
                workbook_id = workbook_raw.get('id', 'Unknown ID')
                logger.warning(
                    f"[{MODULE_NAME}] Workbook '{workbook_name}' (ID: {workbook_id}) found with owner having all primary identifying fields missing. Skipping."
                )

        processing_time = (datetime.now() - start_time).total_seconds()
        logger.info(f"[{MODULE_NAME}] Successfully processed {len(users_list)} unique users in {processing_time:.2f} seconds")
        return users_list

    except Exception as e:
        logger.error(f"[{MODULE_NAME}] An unexpected error occurred while fetching users: {str(e)}")
        raise

def main() -> None:
    """Main function for testing the module."""
    logger.info(f"[{MODULE_NAME}] Starting script execution")
    
    try:
        # Load sample data
        sample_data_path = root_folder / 'sample_data' / 'data_test.json'
        logger.info(f"[{MODULE_NAME}] Loading test data from: {sample_data_path}")
        
        with open(sample_data_path, 'r', encoding='utf-8') as f:
            raw_data = json.load(f)

        # Get users from raw data
        users = get_users(raw_data=raw_data)
        
        # Print sample of users for verification
        logger.info(f"[{MODULE_NAME}] Retrieved {len(users)} unique users")
        for user in users[:3]:  # Print first 3 users
            logger.info(f"[{MODULE_NAME}] Sample user: {user}")
            
    except FileNotFoundError:
        logger.error(f"[{MODULE_NAME}] Test data file not found: {sample_data_path}")
    except json.JSONDecodeError:
        logger.error(f"[{MODULE_NAME}] Invalid JSON in test data file")
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Script execution failed: {str(e)}")

if __name__ == "__main__":
    main()