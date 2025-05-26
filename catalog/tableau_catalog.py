import os
import json
import csv
from datetime import datetime
from typing import List, Dict, Any, Optional
import logging
from pathlib import Path
import pandas as pd
from ..config.catalog_config import CatalogConfig

class TableauCatalog:
    def __init__(self, client, config: CatalogConfig):
        """
        Initialize the Tableau Catalog system.
        
        Args:
            client: Tableau API client instance
            config: Catalog configuration
        """
        self.client = client
        self.config = config
        self.logger = self._setup_logger()

    def _setup_logger(self) -> logging.Logger:
        """Setup logging configuration"""
        logger = logging.getLogger('TableauCatalog')
        logger.setLevel(logging.INFO)
        
        if not logger.handlers:
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)
        
        return logger

    async def run_full_catalog(self) -> Dict[str, Any]:
        """
        Run full catalog extraction for all projects.
        
        Returns:
            Dict containing the complete catalog data
        """
        if not self.config.modes['full'].enabled:
            raise ValueError('Full catalog mode is disabled')

        self.logger.info('Starting full catalog extraction...')
        
        # Get all projects
        projects = await self._get_all_projects()
        
        results = {
            'timestamp': datetime.now().isoformat(),
            'total_projects': len(projects),
            'projects': []
        }

        for project in projects:
            self.logger.info(f'Processing project: {project["name"]}')
            project_data = await self._extract_project_data(project['id'])
            results['projects'].append(project_data)
            
            # Save intermediate results
            await self._save_results(project_data, 'full', project['name'])

        # Save complete catalog
        await self._save_results(results, 'full', 'complete_catalog')
        
        return results

    async def run_project_catalog(self, project_path: Optional[str] = None) -> Dict[str, Any]:
        """
        Run catalog extraction for specific project(s).
        
        Args:
            project_path: Optional specific project path to process
            
        Returns:
            Dict containing the project catalog data
        """
        if not self.config.modes['project'].enabled:
            raise ValueError('Project catalog mode is disabled')

        project_paths = [project_path] if project_path else self.config.modes['project'].project_paths
        results = {
            'timestamp': datetime.now().isoformat(),
            'projects': []
        }

        for path in project_paths:
            self.logger.info(f'Processing project: {path}')
            project_data = await self._extract_project_data_by_path(path)
            results['projects'].append(project_data)
            
            # Save individual project results
            await self._save_results(
                project_data, 
                'project', 
                path.split('/')[-1] or path
            )

        return results

    async def run_workbook_catalog(self, project_name: str, workbook_name: str) -> Dict[str, Any]:
        """
        Run catalog extraction for a specific workbook.
        
        Args:
            project_name: Name of the project
            workbook_name: Name of the workbook
            
        Returns:
            Dict containing the workbook catalog data
        """
        if not self.config.modes['workbook'].enabled:
            raise ValueError('Workbook catalog mode is disabled')

        self.logger.info(f'Processing workbook: {workbook_name} in project: {project_name}')
        
        workbook_data = await self._extract_workbook_data(project_name, workbook_name)
        await self._save_results(
            workbook_data, 
            'workbook', 
            f'{project_name}_{workbook_name}'
        )
        
        return workbook_data

    async def _get_all_projects(self) -> List[Dict[str, Any]]:
        """Get all projects from Tableau"""
        query = """
        query GetAllProjects {
            projects {
                id
                name
                path
                workbooks {
                    id
                    name
                }
            }
        }
        """
        
        result = await self.client.query(query)
        return result['projects']

    async def _extract_project_data(self, project_id: str) -> Dict[str, Any]:
        """Extract detailed data for a specific project"""
        query = """
        query GetProjectDetails($projectId: ID!) {
            project(id: $projectId) {
                id
                name
                path
                workbooks {
                    id
                    name
                    owner {
                        name
                        email
                    }
                    views {
                        id
                        name
                    }
                    dataSources {
                        id
                        name
                        type
                        hasExtracts
                        extractLastRefreshTime
                    }
                }
            }
        }
        """

        result = await self.client.query(query, {'projectId': project_id})
        return result['project']

    async def _extract_project_data_by_path(self, project_path: str) -> Dict[str, Any]:
        """Extract project data using project path"""
        query = """
        query GetProjectByPath($path: [String!]!) {
            projects(path: $path) {
                id
                name
                path
                workbooks {
                    id
                    name
                    owner {
                        name
                        email
                    }
                    views {
                        id
                        name
                    }
                    dataSources {
                        id
                        name
                        type
                        hasExtracts
                        extractLastRefreshTime
                    }
                }
            }
        }
        """

        result = await self.client.query(query, {'path': project_path.split('/')})
        return result['projects'][0]

    async def _extract_workbook_data(self, project_name: str, workbook_name: str) -> Dict[str, Any]:
        """Extract detailed data for a specific workbook"""
        query = """
        query GetWorkbookDetails($projectName: String!, $workbookName: String!) {
            workbooks(where: { 
                project: { name: $projectName },
                name: $workbookName
            }) {
                id
                name
                owner {
                    name
                    email
                }
                views {
                    id
                    name
                    createdAt
                    updatedAt
                }
                dataSources {
                    id
                    name
                    type
                    hasExtracts
                    extractLastRefreshTime
                    upstreamDatabases {
                        id
                        name
                        connectionType
                    }
                }
            }
        }
        """

        result = await self.client.query(query, {
            'projectName': project_name,
            'workbookName': workbook_name
        })
        return result['workbooks'][0]

    async def _save_results(self, data: Dict[str, Any], mode: str, identifier: str) -> None:
        """
        Save results in configured formats.
        
        Args:
            data: Data to save
            mode: Catalog mode ('full', 'project', or 'workbook')
            identifier: Unique identifier for the output file
        """
        timestamp = datetime.now().strftime(self.config.export.timestamp_format)
        output_path = Path(self.config.modes[mode].output_path)
        output_path.mkdir(parents=True, exist_ok=True)

        for format_type in self.config.export.formats:
            filename = f'{identifier}_{timestamp}.{format_type}'
            filepath = output_path / filename
            
            if format_type == 'json':
                with open(filepath, 'w') as f:
                    json.dump(data, f, indent=2)
            
            elif format_type == 'csv':
                # Convert nested data to flat structure for CSV
                flat_data = self._flatten_data(data)
                df = pd.DataFrame(flat_data)
                df.to_csv(filepath, index=False)
            
            elif format_type == 'excel':
                # Convert nested data to flat structure for Excel
                flat_data = self._flatten_data(data)
                df = pd.DataFrame(flat_data)
                df.to_excel(filepath, index=False)
            
            self.logger.info(f'Saved {format_type} file: {filename}')

    def _flatten_data(self, data: Dict[str, Any], prefix: str = '') -> List[Dict[str, Any]]:
        """
        Flatten nested dictionary for CSV/Excel export.
        
        Args:
            data: Nested dictionary to flatten
            prefix: Prefix for nested keys
            
        Returns:
            List of flat dictionaries
        """
        items = []
        
        def flatten_dict(d: Dict[str, Any], p: str = '') -> None:
            flat_item = {}
            for k, v in d.items():
                new_key = f'{p}{k}' if p else k
                
                if isinstance(v, dict):
                    flatten_dict(v, f'{new_key}_')
                elif isinstance(v, list):
                    for i, item in enumerate(v):
                        if isinstance(item, dict):
                            flatten_dict(item, f'{new_key}_{i}_')
                        else:
                            flat_item[f'{new_key}_{i}'] = item
                else:
                    flat_item[new_key] = v
            
            if flat_item:
                items.append(flat_item)
        
        flatten_dict(data, prefix)
        return items

# Usage Example:
"""
async def main():
    # Initialize client and config
    client = TableauGraphQLClient(...)
    config = CatalogConfig()
    
    # Create catalog instance
    catalog = TableauCatalog(client, config)
    
    # Run different catalog modes
    # 1. Full catalog
    full_results = await catalog.run_full_catalog()
    
    # 2. Project catalog
    project_results = await catalog.run_project_catalog('Enterprise/Finance')
    
    # 3. Workbook catalog
    workbook_results = await catalog.run_workbook_catalog('Finance', 'Budget Dashboard')

if __name__ == '__main__':
    import asyncio
    asyncio.run(main())
""" 