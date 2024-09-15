import pandas as pd
import uuid
from datetime import datetime

if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test

def generate_dwid(df: pd.DataFrame, year: int, month: int) -> pd.DataFrame:
    """
    Function to generate 'dwid' based on year, month, and sequence.
    """
    # Ensure year and month columns exist
    df['year'] = pd.to_datetime(df['pickup_datetime']).dt.year
    df['month'] = pd.to_datetime(df['pickup_datetime']).dt.month
    
    # Generate row number within each (year, month) partition
    df['row_number'] = df.groupby(['year', 'month']).cumcount() + 1
    
    # Determine the length of the sequence number
    total_rows = len(df)
    digit_length = len(str(total_rows))
    
    # Format the sequence number with leading zeros
    df['formatted_row_number'] = df['row_number'].apply(lambda x: str(x).zfill(digit_length))
    
    # Concatenate year, month, and formatted row number to form 'dwid'
    df['dwid'] = df.apply(lambda row: f"{year}{month:02d}{row['formatted_row_number']}", axis=1)
    
    # Drop the temporary columns used for calculation
    df = df.drop(columns=['row_number', 'formatted_row_number'])
    
    # Ensure 'dwid' column is unique
    df = df.drop_duplicates(subset=['dwid'])
    
    return df

@transformer
def transform(data: pd.DataFrame, *args, **kwargs) -> pd.DataFrame:
    pickup_column_name = kwargs['configuration'].get('pickup_column_name')
    dropoff_column_name = kwargs['configuration'].get('dropoff_column_name')
    dataset_name = kwargs['configuration'].get('dataset_name')
    
    # Generate a single timestamp for the entire dataframe transformation
    timestamp = datetime.now().isoformat()

    # Extract year and month from timestamp for consistent 'dwid' generation
    year = pd.to_datetime(timestamp).year
    month = pd.to_datetime(timestamp).month
    
    # Generate 'dwid' for each row
    df = generate_dwid(data, year, month)

    return df

@test
def test_output(output: pd.DataFrame, *args) -> None:
    assert output is not None, 'The output is undefined'
    
    # Check if 'dwid' column exists in the output DataFrame
    assert 'dwid' in output.columns, "The 'dwid' column is missing in the output"
    
    # Convert 'dwid' column to a set to check for uniqueness
    dwid_set = set(output['dwid'])
    
    # Assert that the length of the set is equal to the number of rows
    if len(dwid_set) != len(output):
        print(f"Duplicate 'dwid' values found. Total rows: {len(output)}, Unique 'dwid' values: {len(dwid_set)}")
        duplicates = output[output.duplicated(subset=['dwid'], keep=False)]
        #print("Duplicates:")
        #print(duplicates)
    assert len(dwid_set) == len(output), "Not all 'dwid' values are unique"
