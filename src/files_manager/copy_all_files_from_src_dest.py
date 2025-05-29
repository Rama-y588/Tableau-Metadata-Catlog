import shutil
from pathlib import Path
from typing import Dict, Any
from src.utils.logger import app_logger as logger

file_name = "file_copier"

def copy_files_to_destination(source_dir: Path, dest_dir: Path) -> Dict[str, Any]:
    """
    Copy all files from source directory to destination directory.
    
    Args:
        source_dir (Path): Source directory path
        dest_dir (Path): Destination directory path
        
    Returns:
        Dict[str, Any]: Status dictionary containing:
            - success: bool
            - message: str
            - copied_files: List[str]
            - error: str (if any)
    """
    status = {
        "success": False,
        "message": "",
        "copied_files": [],
        "error": None
    }
    
    try:
        # Validate directories
        if not source_dir.exists():
            status["error"] = f"Source directory does not exist: {source_dir}"
            logger.error(f"[{file_name}] {status['error']}")
            return status
            
        if not dest_dir.exists():
            dest_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"[{file_name}] Created destination directory: {dest_dir}")
        
        # Get all files in source directory
        files_to_copy = [f for f in source_dir.iterdir() if f.is_file()]
        
        if not files_to_copy:
            status["message"] = f"No files found in {source_dir}"
            logger.warning(f"[{file_name}] {status['message']}")
            return status
        
        # Copy each file
        for file_path in files_to_copy:
            try:
                dest_file = dest_dir / file_path.name
                shutil.copy2(file_path, dest_file)
                status["copied_files"].append(str(file_path.name))
                logger.info(f"[{file_name}] Copied file: {file_path.name}")
            except Exception as e:
                status["error"] = f"Error copying file {file_path.name}: {str(e)}"
                logger.error(f"[{file_name}] {status['error']}")
                return status
        
        status["success"] = True
        status["message"] = f"Successfully copied {len(status['copied_files'])} files"
        logger.info(f"[{file_name}] {status['message']}")
        
    except Exception as e:
        status["error"] = f"Unexpected error during file copy: {str(e)}"
        logger.error(f"[{file_name}] {status['error']}")
    
    return status

if __name__ == "__main__":
    # Example usage
    current_file = Path(__file__).resolve()
    root_folder = current_file.parents[2]
    
    # Define source and destination directories
    source_dir = root_folder / "app" / "data" / "temp"
    dest_dir = root_folder / "app" / "data" / "prod"
    
    # Test copy operation
    copy_status = copy_files_to_destination(source_dir, dest_dir)
    if copy_status["success"]:
        logger.info(f"[{file_name}] Copy operation successful: {copy_status['message']}")
        logger.info(f"[{file_name}] Copied files: {', '.join(copy_status['copied_files'])}")
    else:
        logger.error(f"[{file_name}] Copy operation failed: {copy_status['error']}")