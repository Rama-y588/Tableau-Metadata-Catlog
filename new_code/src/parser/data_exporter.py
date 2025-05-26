import csv
import sqlite3
from pathlib import Path
from typing import Dict, List, Any, Optional
import pandas as pd
from new_code.utils.custom_logger import app_logger

class TableauDataExporter:
    def __init__(self, output_dir: str = None):
        """
        Initialize the exporter with output directory
        
        Args:
            output_dir (str): Directory to store exported files
        """
        if output_dir is None:
            output_dir = Path(__file__).parent.parent / 'data' / 'exports'
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
    def export_to_csv(self, data: Dict[str, List[Dict[str, Any]]], prefix: str = '') -> Dict[str, Path]:
        """
        Export parsed data to CSV files
        
        Args:
            data: Dictionary of tables and their records
            prefix: Optional prefix for filenames
            
        Returns:
            Dictionary mapping table names to their CSV file paths
        """
        try:
            app_logger.info("Starting CSV export")
            exported_files = {}
            
            for table_name, records in data.items():
                if not records:
                    app_logger.warning(f"No records to export for table {table_name}")
                    continue
                    
                # Create filename
                filename = f"{prefix}{table_name}.csv" if prefix else f"{table_name}.csv"
                filepath = self.output_dir / filename
                
                # Convert to DataFrame for better handling
                df = pd.DataFrame(records)
                
                # Export to CSV
                df.to_csv(filepath, index=False)
                exported_files[table_name] = filepath
                
                app_logger.info(f"Exported {len(records)} records to {filepath}")
            
            return exported_files
            
        except Exception as e:
            app_logger.error(f"Failed to export data to CSV: {str(e)}")
            raise
    
    def export_to_sqlite(self, data: Dict[str, List[Dict[str, Any]]], db_name: str = 'tableau_data.db') -> Path:
        """
        Export parsed data to SQLite database
        
        Args:
            data: Dictionary of tables and their records
            db_name: Name of the SQLite database file
            
        Returns:
            Path to the created database file
        """
        try:
            app_logger.info("Starting SQLite export")
            db_path = self.output_dir / db_name
            
            # Connect to SQLite database
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Create tables and insert data
            for table_name, records in data.items():
                if not records:
                    app_logger.warning(f"No records to export for table {table_name}")
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame(records)
                
                # Create table
                df.to_sql(table_name, conn, if_exists='replace', index=False)
                
                # Create indexes for better performance
                if table_name in ['workbooks', 'views', 'datasources']:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name}(id)")
                if table_name in ['workbooks']:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_owner_id ON {table_name}(owner_id)")
                if table_name in ['views', 'datasources']:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_workbook_id ON {table_name}(workbook_id)")
                
                app_logger.info(f"Exported {len(records)} records to table {table_name}")
            
            conn.commit()
            conn.close()
            
            app_logger.info(f"Successfully exported data to {db_path}")
            return db_path
            
        except Exception as e:
            app_logger.error(f"Failed to export data to SQLite: {str(e)}")
            if 'conn' in locals():
                conn.close()
            raise
    
    def export_to_postgres(self, data: Dict[str, List[Dict[str, Any]]], 
                          connection_params: Dict[str, Any]) -> None:
        """
        Export parsed data to PostgreSQL database
        
        Args:
            data: Dictionary of tables and their records
            connection_params: Dictionary containing PostgreSQL connection parameters
                Required keys: host, port, database, user, password
        """
        try:
            import psycopg2
            from psycopg2.extras import execute_values
            
            app_logger.info("Starting PostgreSQL export")
            
            # Connect to PostgreSQL
            conn = psycopg2.connect(**connection_params)
            cursor = conn.cursor()
            
            for table_name, records in data.items():
                if not records:
                    app_logger.warning(f"No records to export for table {table_name}")
                    continue
                
                # Convert to DataFrame
                df = pd.DataFrame(records)
                
                # Create table
                columns = df.columns.tolist()
                create_table_sql = f"""
                CREATE TABLE IF NOT EXISTS {table_name} (
                    {', '.join(f"{col} TEXT" for col in columns)}
                )
                """
                cursor.execute(create_table_sql)
                
                # Insert data
                insert_sql = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES %s
                ON CONFLICT DO NOTHING
                """
                
                # Convert DataFrame to list of tuples
                values = [tuple(x) for x in df.values]
                execute_values(cursor, insert_sql, values)
                
                # Create indexes
                if table_name in ['workbooks', 'views', 'datasources']:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_id ON {table_name}(id)")
                if table_name in ['workbooks']:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_owner_id ON {table_name}(owner_id)")
                if table_name in ['views', 'datasources']:
                    cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{table_name}_workbook_id ON {table_name}(workbook_id)")
                
                app_logger.info(f"Exported {len(records)} records to table {table_name}")
            
            conn.commit()
            conn.close()
            
            app_logger.info("Successfully exported data to PostgreSQL")
            
        except Exception as e:
            app_logger.error(f"Failed to export data to PostgreSQL: {str(e)}")
            if 'conn' in locals():
                conn.close()
            raise

# Example usage
if __name__ == "__main__":
    from new_code.parser.json_parser import TableauDataParser
    
    # Parse the data
    parser = TableauDataParser(str(Path(__file__).parent.parent / 'sample_data' / 'data_test.json'))
    parsed_data = parser.parse_all()
    
    # Create exporter
    exporter = TableauDataExporter()
    
    # Export to CSV
    csv_files = exporter.export_to_csv(parsed_data, prefix='tableau_')
    print("\nExported CSV files:")
    for table, filepath in csv_files.items():
        print(f"{table}: {filepath}")
    
    # Export to SQLite
    db_path = exporter.export_to_sqlite(parsed_data)
    print(f"\nExported to SQLite database: {db_path}")
    
    # Example PostgreSQL export (commented out)
    """
    pg_params = {
        'host': 'localhost',
        'port': 5432,
        'database': 'tableau_data',
        'user': 'your_user',
        'password': 'your_password'
    }
    exporter.export_to_postgres(parsed_data, pg_params)
    """ 