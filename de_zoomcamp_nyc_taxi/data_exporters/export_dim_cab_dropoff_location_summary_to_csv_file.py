import os
import subprocess
from mage_ai.data_preparation.decorators import data_exporter
import pandas as pd
from mage_ai.io.file import FileIO

@data_exporter
def export_data_to_file(df: pd.DataFrame, **kwargs) -> None:
    """
    This function triggers a dbt model to export a CSV file.
    Mage recognizes this as a valid data_exporter block 
    because of the @data_exporter decorator.
    """
    filepath = '/home/src/de_zoomcamp_nyc_taxi/resources/csv/dim_cab_dropoff_location_summary.csv'
    
    dbt_command = [
        'dbt',
        'run',
        '--select',
        'production.dim.zone.export.export_cab_dropoff_location_summary',
        '--target',
        'production'
    ]
    
    print(f"Executing DBT command: {' '.join(dbt_command)}")
    result = subprocess.run(dbt_command, capture_output=True, text=True)
    
    if result.returncode == 0:
        print("DBT command completed successfully.")
        print(result.stdout)
    else:
        print("DBT command failed!")
        print("STDERR:", result.stderr)
        raise RuntimeError(f"DBT export command failed with exit code {result.returncode}")
    
    if os.path.exists(filepath):
        print(f"Export successful! CSV file created at: {filepath}")
    else:
        raise FileNotFoundError(f"CSV file not found at: {filepath}")
