import csv
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from src.utils.logger import app_logger as logger
from src.data_generation.list_project_folders import load_projects_csv, get_project_hierarchy


def generate_project_csv(project: Dict[str, Any], output_dir: Path) -> Tuple[bool, str]:
    """
    Generate a CSV file for a specific project and its contents.
    
    Args:
        project (Dict[str, Any]): Project data dictionary
        output_dir (Path): Directory to save the CSV file
        
    Returns:
        Tuple[bool, str]: Success status and message
    """
    try:
        project_name = project.get('Project Name', 'Unnamed')
        project_id = project.get('Project ID', '')
        
        # Create project-specific directory
        project_dir = output_dir / project_name
        project_dir.mkdir(parents=True, exist_ok=True)
        
        # Define CSV file path
        csv_path = project_dir / f"{project_name}_details.csv"
        
        # Define CSV headers
        headers = [
            'Project ID',
            'Project Name',
            'Description',
            'Parent Project ID',
            'Parent Project Name',
            'Creation Date',
            'Last Modified Date',
            'Owner ID',
            'Owner Name'
        ]
        
        # Write project details to CSV
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerow(project)
            
        logger.info(f"Generated CSV for project: {project_name}")
        return True, f"Successfully generated CSV for {project_name}"
        
    except Exception as e:
        error_msg = f"Failed to generate CSV for project {project.get('Project Name', 'Unnamed')}: {str(e)}"
        logger.error(error_msg)
        return False, error_msg


def generate_project_structure_csv(project: Dict[str, Any], hierarchy: Dict[str, List[Dict[str, Any]]], output_dir: Path) -> Tuple[bool, str]:
    """
    Generate a CSV file showing the structure of a project and its subprojects.
    
    Args:
        project (Dict[str, Any]): Project data dictionary
        hierarchy (Dict[str, List[Dict[str, Any]]]): Project hierarchy dictionary
        output_dir (Path): Directory to save the CSV file
        
    Returns:
        Tuple[bool, str]: Success status and message
    """
    try:
        project_name = project.get('Project Name', 'Unnamed')
        project_dir = output_dir / project_name
        
        # Define CSV file path
        csv_path = project_dir / f"{project_name}_structure.csv"
        
        # Define CSV headers
        headers = [
            'Level',
            'Project Name',
            'Project ID',
            'Parent Project',
            'Description',
            'Owner'
        ]
        
        # Collect all projects in the hierarchy
        structure_data = []
        
        def collect_structure(proj: Dict[str, Any], level: int = 0):
            structure_data.append({
                'Level': level,
                'Project Name': proj.get('Project Name', 'Unnamed'),
                'Project ID': proj.get('Project ID', ''),
                'Parent Project': proj.get('Parent Project Name', 'None'),
                'Description': proj.get('Description', ''),
                'Owner': proj.get('Owner Name', '')
            })
            
            # Add child projects
            children = hierarchy.get(proj.get('Project Name', ''), [])
            for child in children:
                collect_structure(child, level + 1)
        
        # Start collection from the given project
        collect_structure(project)
        
        # Write structure to CSV
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=headers)
            writer.writeheader()
            writer.writerows(structure_data)
            
        logger.info(f"Generated structure CSV for project: {project_name}")
        return True, f"Successfully generated structure CSV for {project_name}"
        
    except Exception as e:
        error_msg = f"Failed to generate structure CSV for project {project.get('Project Name', 'Unnamed')}: {str(e)}"
        logger.error(error_msg)
        return False, error_msg


def generate_all_project_csvs(output_dir: Optional[Path] = None) -> Dict[str, Tuple[bool, str]]:
    """
    Generate CSV files for all projects and their structures.
    
    Args:
        output_dir (Optional[Path]): Directory to save CSV files. If None, uses default path.
        
    Returns:
        Dict[str, Tuple[bool, str]]: Results of the operation for each project
    """
    try:
        # Set up output directory
        if output_dir is None:
            current_file = Path(__file__).resolve()
            project_root = current_file.parents[2]  # Go up to src directory
            output_dir = project_root / "output" / "project_csvs"
        
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # Load projects and get hierarchy
        projects = load_projects_csv()
        if not projects:
            return {"error": (False, "No projects found")}
            
        hierarchy = get_project_hierarchy(projects)
        results = {}
        
        # Generate CSV for each project
        for project in projects:
            project_name = project.get('Project Name', 'Unnamed')
            
            # Generate project details CSV
            success, msg = generate_project_csv(project, output_dir)
            results[f"{project_name}_details"] = (success, msg)
            
            # Generate project structure CSV
            success, msg = generate_project_structure_csv(project, hierarchy, output_dir)
            results[f"{project_name}_structure"] = (success, msg)
        
        # Print summary
        logger.info("\nProject CSV Generation Summary:\n" + "=" * 50)
        for key, (success, msg) in results.items():
            logger.info(f"{key} - {'Success' if success else 'Failed'}: {msg}")
        logger.info("=" * 50)
        
        return results
        
    except Exception as e:
        error_msg = f"Failed to generate project CSV files: {str(e)}"
        logger.error(error_msg)
        return {"error": (False, error_msg)}


if __name__ == "__main__":
    # Generate all project CSV files
    results = generate_all_project_csvs()
    
    # Check if any operations failed
    failed_operations = [key for key, (success, _) in results.items() if not success]
    if failed_operations:
        logger.error(f"Failed operations: {', '.join(failed_operations)}")
        sys.exit(1)
    else:
        logger.info("All project CSV files generated successfully") 