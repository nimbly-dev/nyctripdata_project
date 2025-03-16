import os
import subprocess
import pandas as pd
from sqlalchemy import create_engine

if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom

@custom
def export_dim_cab_dropoff_location_summary(df: pd.DataFrame, **kwargs) -> None:
    # Retrieve USER_CODE_PATH from environment variables
    user_code_path = os.environ.get("USER_CODE_PATH")
    if not user_code_path:
        raise EnvironmentError("USER_CODE_PATH is not set in the environment.")
    
    # Build the project directory and CSV output path using USER_CODE_PATH
    project_dir = os.path.join(user_code_path, "dbt", "dbt_cab_trip_data_analytics")
    profiles_dir = project_dir
    csv_filepath = os.path.join(user_code_path, "resources", "csv", "dim_cab_dropoff_location_summary.csv")
    

    df.to_csv(csv_filepath, index=False)
    print(f"Export successful! CSV file created at: {csv_filepath}")