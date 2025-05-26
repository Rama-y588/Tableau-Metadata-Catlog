from pathlib import Path
from src.parser.transformations import TableauDataTransformer
from src.csv_export.csv_exporter import TableauCSVExporter
from src.utils.logger import app_logger

def main():
    try:
        # Get the path to the sample data
        sample_data_path = Path(__file__).parent.parent / 'sample_data' / 'data_test.json'
        app_logger.info(f"Using sample data from: {sample_data_path}")
        
        # Transform the data
        app_logger.info("Starting data transformation...")
        transformer = TableauDataTransformer(str(sample_data_path))
        transformed_data = transformer.transform_all()
        app_logger.info(f"Transformed data contains tables: {list(transformed_data.keys())}")
        
        # Export to CSV
        app_logger.info("Starting CSV export...")
        exporter = TableauCSVExporter()
        csv_files = exporter.export_all(transformed_data, prefix='tableau_')
        
        # Print results
        app_logger.info("\nExported CSV files:")
        for table, filepath in csv_files.items():
            app_logger.info(f"{table}: {filepath}")
            
    except Exception as e:
        app_logger.error(f"Failed to run CSV export: {str(e)}")
        raise

if __name__ == "__main__":
    main() 