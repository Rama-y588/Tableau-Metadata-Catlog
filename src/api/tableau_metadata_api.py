from pathlib import Path
from typing import Dict, Any, Optional
import json

from src.utils.logger import app_logger as logger
from src.utils.execute_graphql_query import execute_graphql_query
from src.utils.helper import load_YAML_config


class TableauMetadataAPI:
    """Class to handle Tableau Metadata API interactions"""
    
    def __init__(self, config_path: Optional[Path] = None):
        """
        Initialize the Tableau Metadata API client.
        
        Args:
            config_path (Optional[Path]): Path to the configuration file. If None, uses default path.
        """
        if config_path is None:
            current_file = Path(__file__).resolve()
            project_root = current_file.parents[1]
            config_path = project_root / 'config' / 'tableau_config.json'
        
        self.config = self._load_config(config_path)
        self.base_url = self.config.get('base_url')
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f"Bearer {self.config.get('access_token')}"
        }
    
    def _load_config(self, config_path: Path) -> Dict[str, Any]:
        """
        Load configuration from JSON file.
        
        Args:
            config_path (Path): Path to the configuration file
            
        Returns:
            Dict[str, Any]: Configuration dictionary
            
        Raises:
            FileNotFoundError: If config file doesn't exist
            json.JSONDecodeError: If config file is not valid JSON
        """
        try:
            with open(config_path, 'r') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.error(f"Configuration file not found: {config_path}")
            raise
        except json.JSONDecodeError:
            logger.error(f"Invalid JSON in configuration file: {config_path}")
            raise
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Execute the metadata GraphQL query and return the results.
        
        Returns:
            Dict[str, Any]: Raw metadata from Tableau
            
        Raises:
            Exception: If query execution fails
        """
        # GraphQL query to fetch all required metadata
        query = """
        query GetTableauMetadata {
            workbooks {
                id
                name
                project {
                    id
                    name
                }
                owner {
                    id
                    name
                }
                description
                createdAt
                updatedAt
                size
                viewCount
                tags {
                    id
                    name
                }
                views {
                    id
                    name
                    path
                    type
                    createdAt
                    updatedAt
                }
            }
            datasources {
                id
                name
                resourceUri
                hasExtracts
                extractLastRefreshTime
                type
                createdAt
                updatedAt
                connections {
                    id
                    name
                    type
                    connectsTo
                    createdAt
                    updatedAt
                }
            }
            tags {
                id
                name
                createdAt
                updatedAt
            }
        }
        """
        
        try:
            # Execute the GraphQL query
            response = execute_graphql_query(
                url=self.base_url,
                query=query,
                headers=self.headers
            )
            
            if not response:
                raise Exception("Empty response from GraphQL query")
            
            # Log success
            logger.info("Successfully retrieved metadata from Tableau")
            
            return response
            
        except Exception as e:
            error_msg = f"Failed to execute metadata query: {str(e)}"
            logger.error(error_msg)
            raise Exception(error_msg)


def get_tableau_metadata(config_path: Optional[Path] = None) -> Dict[str, Any]:
    """
    Convenience function to get Tableau metadata.
    
    Args:
        config_path (Optional[Path]): Path to the configuration file
        
    Returns:
        Dict[str, Any]: Raw metadata from Tableau
    """
    api = TableauMetadataAPI(config_path)
    return api.get_metadata()


if __name__ == "__main__":
    # Test the API
    try:
        metadata = get_tableau_metadata()
        logger.info("Successfully retrieved metadata")
        
        # Save raw data for testing
        current_file = Path(__file__).resolve()
        project_root = current_file.parents[1]
        output_path = project_root / "sample_data" / "data_test.json"
        
        with open(output_path, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        logger.info(f"Saved raw metadata to: {output_path}")
        
    except Exception as e:
        logger.error(f"Failed to get metadata: {str(e)}") 