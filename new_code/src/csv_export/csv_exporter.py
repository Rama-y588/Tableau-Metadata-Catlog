from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
from src.utils.logger import app_logger

class TableauCSVExporter:
    """
    Handles exporting transformed data to CSV files, enforcing DB schema column order.
    """

    # Define the expected column order for each table as per DB schema
    TABLE_COLUMNS = {
        "users": [
            "id", "name", "username", "email", "created_at", "updated_at"
        ],
        "workbooks": [
            "id", "name", "project_name", "uri", "owner_id", "created_at", "updated_at"
        ],
        "views": [
            "id", "name", "workbook_id", "path", "created_at", "updated_at", "type"
        ],
        "datasources": [
            "id", "name", "uri", "has_extracts", "extract_last_refresh_time", "type", "created_at", "updated_at"
        ],
        "workbook_datasources": [
            "workbook_id", "datasource_id"
        ],
        "connections": [
            "id", "name", "connection_type", "connects_to", "created_at", "updated_at"
        ],
        "datasource_connections": [
            "datasource_id", "connection_id"
        ],
        "tags": [
            "id", "name", "created_at", "updated_at"
        ],
        "workbook_tags": [
            "workbook_id", "tag_id"
        ]
    }

    def __init__(self, output_dir: Optional[str] = None):
        """
        Initialize the CSV exporter with output directory.
        Args:
            output_dir (str): Directory to store CSV files. Defaults to data/csv_exports.
        """
        if output_dir is None:
            self.output_dir = Path(__file__).parent.parent.parent / 'data' / 'csv_exports'
        else:
            self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def transform_data(self, data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform the input data to match the schema structure, including relation tables.
        Args:
            data: Dictionary of tables and their records
        Returns:
            Transformed data matching the schema
        """
        transformed = {}
        app_logger.info(f"Starting data transformation. Input tables: {list(data.keys())}")
        
        # Transform users
        if 'users' in data:
            transformed['users'] = [{
                'id': record.get('id'),
                'name': record.get('name'),
                'username': record.get('username'),
                'email': record.get('email'),
                'created_at': record.get('created_at'),
                'updated_at': record.get('updated_at')
            } for record in data['users']]
            app_logger.info(f"Transformed {len(transformed['users'])} users")
        
        # Transform workbooks
        if 'workbooks' in data:
            transformed['workbooks'] = [{
                'id': record.get('id'),
                'name': record.get('name'),
                'project_name': record.get('project_name'),
                'uri': record.get('uri'),
                'owner_id': record.get('owner_id'),
                'created_at': record.get('created_at'),
                'updated_at': record.get('updated_at')
            } for record in data['workbooks']]
            app_logger.info(f"Transformed {len(transformed['workbooks'])} workbooks")
        
        # Transform views
        if 'views' in data:
            transformed['views'] = [{
                'id': record.get('id'),
                'name': record.get('name'),
                'workbook_id': record.get('workbook_id'),
                'path': record.get('path'),
                'created_at': record.get('created_at'),
                'updated_at': record.get('updated_at'),
                'type': record.get('__typename')
            } for record in data['views']]
            app_logger.info(f"Transformed {len(transformed['views'])} views")
        
        # Transform datasources
        if 'datasources' in data:
            transformed['datasources'] = [{
                'id': record.get('id'),
                'name': record.get('name'),
                'uri': record.get('uri'),
                'has_extracts': record.get('has_extracts', False),
                'extract_last_refresh_time': record.get('extract_last_refresh_time'),
                'type': record.get('type', 'upstream'),  # Default to upstream if not specified
                'created_at': record.get('created_at'),
                'updated_at': record.get('updated_at')
            } for record in data['datasources']]
            app_logger.info(f"Transformed {len(transformed['datasources'])} datasources")
        
        # Extract workbook_datasources relations
        if 'workbook_datasources' in data:
            transformed['workbook_datasources'] = [{
                'workbook_id': record.get('workbook_id'),
                'datasource_id': record.get('datasource_id')
            } for record in data['workbook_datasources']]
            app_logger.info(f"Transformed {len(transformed['workbook_datasources'])} workbook_datasources relations")
        
        # Transform connections
        if 'connections' in data:
            transformed['connections'] = [{
                'id': record.get('id'),
                'name': record.get('name'),
                'connection_type': record.get('connection_type'),
                'connects_to': record.get('type'),  # Map 'type' to 'connects_to'
                'created_at': record.get('created_at'),
                'updated_at': record.get('updated_at')
            } for record in data['connections']]
            app_logger.info(f"Transformed {len(transformed['connections'])} connections")
        
        # Extract datasource_connections relations
        if 'datasource_connections' in data:
            transformed['datasource_connections'] = [{
                'datasource_id': record.get('datasource_id'),
                'connection_id': record.get('connection_id')
            } for record in data['datasource_connections']]
            app_logger.info(f"Transformed {len(transformed['datasource_connections'])} datasource_connections relations")
        
        # Transform tags
        if 'tags' in data:
            transformed['tags'] = [{
                'id': record.get('id'),
                'name': record.get('name'),
                'created_at': record.get('created_at'),
                'updated_at': record.get('updated_at')
            } for record in data['tags']]
            app_logger.info(f"Transformed {len(transformed['tags'])} tags")
        
        # Extract workbook_tags relations
        if 'workbook_tags' in data:
            transformed['workbook_tags'] = [{
                'workbook_id': record.get('workbook_id'),
                'tag_id': record.get('tag_id')
            } for record in data['workbook_tags']]
            app_logger.info(f"Transformed {len(transformed['workbook_tags'])} workbook_tags relations")
        
        # Log the number of records in each table
        app_logger.info("Final transformed data summary:")
        for table_name, records in transformed.items():
            app_logger.info(f"Table {table_name}: {len(records)} records")
        
        return transformed

    def export_table(
        self, table_name: str, records: List[Dict[str, Any]],
        prefix: str = '', suffix: str = ''
    ) -> Optional[Path]:
        """
        Export a single table to CSV, enforcing DB schema column order.
        Args:
            table_name: Name of the table.
            records: List of records to export.
            prefix: Optional prefix for filename.
            suffix: Optional suffix for filename.
        Returns:
            Path to the created CSV file, or None if no records.
        """
        try:
            if not records:
                app_logger.warning(f"No records to export for table {table_name}")
                return None

            # Use column order from schema if available
            columns = self.TABLE_COLUMNS.get(table_name)
            filename = f"{prefix}{table_name}{suffix}.csv"
            filepath = self.output_dir / filename

            df = pd.DataFrame(records)
            if columns:
                # Reorder columns and fill missing columns with None
                for col in columns:
                    if col not in df.columns:
                        df[col] = None
                df = df[columns]
            df.to_csv(filepath, index=False)

            app_logger.info(f"Exported {len(records)} records to {filepath}")
            return filepath

        except Exception as e:
            app_logger.error(f"Failed to export table {table_name} to CSV: {str(e)}")
            raise

    def export_all(
        self, data: Dict[str, List[Dict[str, Any]]],
        prefix: str = '', suffix: str = ''
    ) -> Dict[str, Path]:
        """
        Export all tables to CSV files, including relation tables.
        Args:
            data: Dictionary of tables and their records.
            prefix: Optional prefix for filenames.
            suffix: Optional suffix for filenames.
        Returns:
            Dictionary mapping table names to their CSV file paths.
        """
        try:
            app_logger.info("Starting CSV export")
            app_logger.info(f"Input data contains tables: {list(data.keys())}")
            
            # Transform data to match schema, including relation tables
            transformed_data = self.transform_data(data)
            app_logger.info(f"Transformed data contains tables: {list(transformed_data.keys())}")
            
            # Export transformed data
            exported_files = {}
            for table_name, records in transformed_data.items():
                app_logger.info(f"Processing table {table_name} with {len(records)} records")
                if records:  # Only export if there are records
                    filepath = self.export_table(table_name, records, prefix, suffix)
                    if filepath:
                        exported_files[table_name] = filepath
                        app_logger.info(f"Successfully exported {len(records)} records to {filepath}")
                else:
                    app_logger.warning(f"No records to export for table {table_name}")
            
            app_logger.info(f"Successfully exported all tables to CSV. Exported files: {list(exported_files.keys())}")
            return exported_files
        except Exception as e:
            app_logger.error(f"Failed to export data to CSV: {str(e)}")
            raise

# Example usage
if __name__ == "__main__":
    from src.parser.transformations import TableauDataTransformer

    # Transform the data
    sample_data_path = Path(__file__).parent.parent.parent / 'sample_data' / 'data_test.json'
    transformer = TableauDataTransformer(str(sample_data_path))
    transformed_data = transformer.transform_all()

    # Export to CSV
    exporter = TableauCSVExporter()
    csv_files = exporter.export_all(transformed_data, prefix='tableau_')

    # Print exported files
    print("\nExported CSV files:")
    for table, filepath in csv_files.items():
        print(f"{table}: {filepath}")
