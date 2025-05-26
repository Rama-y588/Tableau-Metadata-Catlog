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
            "id", "name", "uri", "has_extracts", "extract_last_refresh_time",
            "workbook_id", "type", "created_at", "updated_at"
        ],
        "databases": [
            "id", "name", "connection_type", "type", "created_at", "updated_at"
        ],
        "datasource_databases": [
            "datasource_id", "database_id"
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
            self.output_dir = Path(__file__).parent.parent / 'data' / 'csv_exports'
        else:
            self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

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
        Export all tables to CSV files.
        Args:
            data: Dictionary of tables and their records.
            prefix: Optional prefix for filenames.
            suffix: Optional suffix for filenames.
        Returns:
            Dictionary mapping table names to their CSV file paths.
        """
        try:
            app_logger.info("Starting CSV export")
            exported_files = {}
            for table_name, records in data.items():
                filepath = self.export_table(table_name, records, prefix, suffix)
                if filepath:
                    exported_files[table_name] = filepath
            app_logger.info("Successfully exported all tables to CSV")
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
