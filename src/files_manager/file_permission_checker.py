import os
from pathlib import Path
from src.utils.logger import app_logger as logger

# Define module constants
current_file = Path(__file__).resolve()
root_folder = current_file.parents[2]
MODULE_NAME = "file_permission_checker"

def check_file_creation_ability(directory: Path, filename: str = "emap.txt", content: str = "Sample emap file content") -> bool:
    """
    Check if we can create and delete a file in the given directory.
    
    Args:
        directory (Path): Directory to check
        filename (str): Name of test file to create
        content (str): Content to write to test file
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if not directory.exists():
            directory.mkdir(parents=True, exist_ok=True)
            logger.info(f"[{MODULE_NAME}] Created directory: {directory}")

        file_path = directory / filename
        
        with open(file_path, "w", encoding='utf-8') as f:
            f.write(content)
        logger.info(f"[{MODULE_NAME}] Successfully created file: {file_path}")
        
        # Clean up after test
        file_path.unlink()
        logger.info(f"[{MODULE_NAME}] Successfully deleted test file: {file_path}")
        
        return True
    except Exception as e:
        logger.error(f"[{MODULE_NAME}] Cannot create or delete file {file_path}: {str(e)}")
        return False

if __name__ == "__main__":
    prod_dir = root_folder / "app" / "data" / "prod"
    
    if check_file_creation_ability(prod_dir):
        logger.info(f"[{MODULE_NAME}] You can create and delete files in production directory.")
    else:
        logger.error(f"[{MODULE_NAME}] File creation permission denied in production directory.")