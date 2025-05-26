from datetime import datetime
from typing import Dict, List, Any, Optional, Union
from pathlib import Path
import json
from src.utils.logger import app_logger
from src.api.graphql_client import TableauGraphQLClient

class TableauDataTransformer:
    """Handles data parsing and transformation from GraphQL API to structured format"""
    
    def __init__(self, data_source: Union[str, Dict[str, Any]]):
        """
        Initialize the transformer with either a GraphQL client or direct data
        
        Args:
            data_source: Either a path to JSON file (str) or a GraphQL client instance (TableauGraphQLClient)
                        or direct data dictionary
        """
        if isinstance(data_source, str):
            # Handle file path
            self.data = self._load_json(data_source)
        elif isinstance(data_source, TableauGraphQLClient):
            # Handle GraphQL client
            self.data = data_source.get_tableau_data()
        elif isinstance(data_source, dict):
            # Handle direct data
            self.data = data_source
        else:
            raise ValueError("data_source must be a file path (str), GraphQL client, or data dictionary")
        
    def _load_json(self, json_file_path: str) -> Dict[str, Any]:
        """Load and parse the JSON file"""
        try:
            with open(json_file_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            app_logger.error(f"Failed to load JSON file: {str(e)}")
            raise
    
    def _parse_datetime(self, dt_str: Optional[str]) -> Optional[datetime]:
        """Parse datetime string to datetime object"""
        if not dt_str:
            return None
        try:
            return datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
        except Exception as e:
            app_logger.warning(f"Failed to parse datetime {dt_str}: {str(e)}")
            return None
    
    def transform_users(self) -> List[Dict[str, Any]]:
        """Transform user data"""
        users = set()
        for workbook in self.data['data']['workbooks']:
            owner = workbook.get('owner', {})
            users.add((
                owner.get('id'),
                owner.get('name'),
                owner.get('username'),
                owner.get('email')
            ))
        
        return [{
            'id': user[0] or None,
            'name': user[1] or None,
            'username': user[2] or None,
            'email': user[3] or None,
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        } for user in users]
    
    def transform_workbooks(self) -> List[Dict[str, Any]]:
        """Transform workbook data"""
        workbooks = []
        for workbook in self.data['data']['workbooks']:
            workbooks.append({
                'id': workbook.get('id') or None,
                'name': workbook.get('name') or None,
                'project_name': workbook.get('projectName') or None,
                'uri': workbook.get('uri') or None,
                'owner_id': workbook.get('owner', {}).get('id') or None,
                'created_at': datetime.now(),
                'updated_at': datetime.now()
            })
        return workbooks
    
    def transform_views(self) -> List[Dict[str, Any]]:
        """Transform view data"""
        views = []
        for workbook in self.data['data']['workbooks']:
            for view in workbook.get('views', []):
                views.append({
                    'id': view.get('id') or None,
                    'name': view.get('name') or None,
                    'workbook_id': workbook.get('id') or None,
                    'path': view.get('path') or None,
                    'type': view.get('__typename') or None,
                    'created_at': self._parse_datetime(view.get('createdAt')),
                    'updated_at': self._parse_datetime(view.get('updatedAt'))
                })
        return views
    
    def transform_datasources(self) -> List[Dict[str, Any]]:
        """Transform datasource data"""
        datasources = []
        for workbook in self.data['data']['workbooks']:
            # Process upstream datasources
            for ds in workbook.get('upstreamDatasources', []):
                datasources.append({
                    'id': ds.get('id') or None,
                    'name': ds.get('name') or None,
                    'uri': ds.get('uri') or None,
                    'has_extracts': ds.get('hasExtracts') or None,
                    'extract_last_refresh_time': self._parse_datetime(ds.get('extractLastRefreshTime')),
                    'workbook_id': workbook.get('id') or None,
                    'type': 'upstream',
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                })
            
            # Process embedded datasources
            for ds in workbook.get('embeddedDatasources', []):
                datasources.append({
                    'id': ds.get('id') or None,
                    'name': ds.get('name') or None,
                    'uri': None,
                    'has_extracts': ds.get('hasExtracts') or None,
                    'extract_last_refresh_time': self._parse_datetime(ds.get('extractLastRefreshTime')),
                    'workbook_id': workbook.get('id') or None,
                    'type': 'embedded',
                    'created_at': datetime.now(),
                    'updated_at': datetime.now()
                })
        return datasources
    
    def transform_databases(self) -> List[Dict[str, Any]]:
        """Transform database data"""
        databases = set()
        datasource_relations = []
        
        for workbook in self.data['data']['workbooks']:
            # Process upstream datasource databases
            for ds in workbook.get('upstreamDatasources', []):
                for db in ds.get('upstreamDatabases', []):
                    db_key = (
                        db.get('name') or None,
                        db.get('connectionType') or None,
                        db.get('__typename') or None
                    )
                    databases.add(db_key)
                    datasource_relations.append((ds.get('id') or None, db.get('name') or None))
            
            # Process embedded datasource databases
            for ds in workbook.get('embeddedDatasources', []):
                for db in ds.get('upstreamDatabases', []):
                    db_key = (
                        db.get('name') or None,
                        db.get('connectionType') or None,
                        db.get('__typename') or None
                    )
                    databases.add(db_key)
                    datasource_relations.append((ds.get('id') or None, db.get('name') or None))
        
        # Create database records
        db_records = [{
            'id': f"db_{i}",
            'name': db[0],
            'connection_type': db[1],
            'type': db[2],
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        } for i, db in enumerate(databases)]
        
        # Store datasource-database relations
        self.datasource_database_relations = datasource_relations
        
        return db_records
    
    def transform_tags(self) -> List[Dict[str, Any]]:
        """Transform tag data"""
        tags = set()
        workbook_tag_relations = []
        
        for workbook in self.data['data']['workbooks']:
            for tag in workbook.get('tags', []):
                tags.add((
                    tag.get('id') or None,
                    tag.get('name') or None
                ))
                workbook_tag_relations.append((
                    workbook.get('id') or None,
                    tag.get('id') or None
                ))
        
        # Create tag records
        tag_records = [{
            'id': tag[0],
            'name': tag[1],
            'created_at': datetime.now(),
            'updated_at': datetime.now()
        } for tag in tags]
        
        # Store workbook-tag relations
        self.workbook_tag_relations = workbook_tag_relations
        
        return tag_records
    
    def transform_all(self) -> Dict[str, List[Dict[str, Any]]]:
        """Transform all data and return as a dictionary of tables"""
        try:
            app_logger.info("Starting data transformation")
            
            result = {
                'users': self.transform_users(),
                'workbooks': self.transform_workbooks(),
                'views': self.transform_views(),
                'datasources': self.transform_datasources(),
                'databases': self.transform_databases(),
                'tags': self.transform_tags(),
                'datasource_database_relations': [
                    {
                        'datasource_id': rel[0],
                        'database_id': rel[1]
                    }
                    for rel in getattr(self, 'datasource_database_relations', [])
                ],
                'workbook_tag_relations': [
                    {
                        'workbook_id': rel[0],
                        'tag_id': rel[1]
                    }
                    for rel in getattr(self, 'workbook_tag_relations', [])
                ]
            }
            
            app_logger.info("Successfully transformed all data")
            return result
            
        except Exception as e:
            app_logger.error(f"Failed to transform data: {str(e)}")
            raise

# Example usage
if __name__ == "__main__":
    # Example 1: Using GraphQL API (commented out for now)
    """
    client = TableauGraphQLClient(
        api_url="https://your-tableau-server/api/graphql",
        api_token="your-api-token"
    )
    transformer = TableauDataTransformer(client)
    transformed_data = transformer.transform_all()
    """
    
    # Example 2: Using JSON file
    sample_data_path = Path(__file__).parent.parent.parent/ 'sample_data' / 'data_test.json'
    transformer = TableauDataTransformer(str(sample_data_path))
    transformed_data = transformer.transform_all()
    
    # Example 3: Using direct data (commented out for now)
    """
    data = {
        "workbooks": [
            {
                "id": "123",
                "name": "Sample Workbook",
                # ... other workbook data ...
            }
        ]
    }
    transformer = TableauDataTransformer(data)
    transformed_data = transformer.transform_all()
    """
    
    # Print some statistics
    for table_name, records in transformed_data.items():
        print(f"{table_name}: {len(records)} records") 