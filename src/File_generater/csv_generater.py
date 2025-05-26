from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
import json
from datetime import datetime
from src.utils.logger import app_logger

class TableauCSVExporter:
    """
    Handles exporting transformed data to CSV files based on schema.dbml and configuration.
    """

    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize the CSV exporter with configuration.
        Args:
            config_path (str): Path to the configuration file. Defaults to src/config/csv_exporter.config
        """
        if config_path is None:
            config_path = Path(__file__).parent.parent / 'config' / 'csv_exporter.config'
        
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        self.output_dir = Path(self.config['output']['directory'])
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.date_format = self.config['output']['date_format']

    def _format_datetime(self, dt_str: Optional[str]) -> Optional[str]:
        """Format datetime string according to configuration."""
        if not dt_str:
            return None
        try:
            dt = datetime.fromisoformat(dt_str.replace('Z', '+00:00'))
            return dt.strftime(self.date_format)
        except (ValueError, AttributeError):
            return dt_str

    def _transform_record(self, record: Dict[str, Any], table_name: str) -> Dict[str, Any]:
        """Transform a single record according to schema and configuration."""
        required_columns = self.config['tables'][table_name]['required_columns']
        transformed = {}
        
        for col in required_columns:
            value = record.get(col)
            if col in ['created_at', 'updated_at']:
                value = self._format_datetime(value)
            transformed[col] = value
            
        return transformed

    def transform_data(self, data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Transform the input data to match the schema structure.
        Args:
            data: Dictionary of tables and their records
        Returns:
            Transformed data matching the schema
        """
        transformed = {}
        app_logger.info(f"Starting data transformation. Input tables: {list(data.keys())}")
        
        for table_name in self.config['tables'].keys():
            if table_name in data:
                transformed[table_name] = [
                    self._transform_record(record, table_name)
                    for record in data[table_name]
                ]
                app_logger.info(f"Transformed {len(transformed[table_name])} {table_name}")
        
        app_logger.info("Final transformed data summary:")
        for table_name, records in transformed.items():
            app_logger.info(f"Table {table_name}: {len(records)} records")
        
        return transformed

    def export_table(
        self, table_name: str, records: List[Dict[str, Any]]
    ) -> Optional[Path]:
        """
        Export a single table to CSV.
        Args:
            table_name: Name of the table.
            records: List of records to export.
        Returns:
            Path to the created CSV file, or None if no records.
        """
        try:
            if not records:
                app_logger.warning(f"No records to export for table {table_name}")
                return None

            table_config = self.config['tables'][table_name]
            filename = f"{self.config['output']['file_prefix']}{table_config['filename']}{self.config['output']['file_suffix']}"
            filepath = self.output_dir / filename

            df = pd.DataFrame(records)
            # Ensure all required columns exist
            for col in table_config['required_columns']:
                if col not in df.columns:
                    df[col] = None
            # Reorder columns to match required order
            df = df[table_config['required_columns']]
            
            df.to_csv(
                filepath,
                index=False,
                encoding=self.config['output']['encoding']
            )

            app_logger.info(f"Exported {len(records)} records to {filepath}")
            return filepath

        except Exception as e:
            app_logger.error(f"Failed to export table {table_name} to CSV: {str(e)}")
            raise

    def export_all(
        self, data: Dict[str, List[Dict[str, Any]]]
    ) -> Dict[str, Path]:
        """
        Export all tables to CSV files.
        Args:
            data: Dictionary of tables and their records.
        Returns:
            Dictionary mapping table names to their CSV file paths.
        """
        try:
            app_logger.info("Starting CSV export")
            app_logger.info(f"Input data contains tables: {list(data.keys())}")
            
            transformed_data = self.transform_data(data)
            app_logger.info(f"Transformed data contains tables: {list(transformed_data.keys())}")
            
            exported_files = {}
            for table_name, records in transformed_data.items():
                if records:
                    filepath = self.export_table(table_name, records)
                    if filepath:
                        exported_files[table_name] = filepath
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
    csv_files = exporter.export_all(transformed_data)

    # Print exported files
    print("\nExported CSV files:")
    for table, filepath in csv_files.items():
        print(f"{table}: {filepath}")