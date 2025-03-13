import os
import ast

@custom
def transform_custom(*args, **kwargs):
    start_year = kwargs['start_year']
    start_month = kwargs['start_month']
    end_year = kwargs['end_year']
    end_month = kwargs['end_month']
    get_data_from = kwargs.get('get_data_from', [])

    if isinstance(get_data_from, str):
        get_data_from = ast.literal_eval(get_data_from)

    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            if (year == start_year and month < start_month) or (year == end_year and month > end_month):
                continue

            year_month = f"{year}_{month:02d}"

            # Set the base directory to the mounted directory (host side)
            base_dir = "./temp_copy"  # This corresponds to /tmp/temp_copy in the container
            
            # List of cab types for which to create folders.
            folders = ["yellow", "green", "fhv", "combined"]
            
            for folder in folders:
                # Build the full directory path for each cab type and year_month.
                dir_path = os.path.join(base_dir, folder, year_month)
                # Create the directory (and any parent directories) if it doesn't exist.
                os.makedirs(dir_path, exist_ok=True)
                # Adjust permissions so that the container's postgres user can write into it.
                # 0o777 makes the directory readable, writable, and executable by everyone.
                os.chmod(dir_path, 0o777)
                print(f"Created directory with permissions 777: {dir_path}")

    return {}
