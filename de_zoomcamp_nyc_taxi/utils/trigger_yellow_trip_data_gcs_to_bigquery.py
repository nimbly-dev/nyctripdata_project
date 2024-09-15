import requests
import json
import time

# Define the API endpoint
api_endpoint = "http://localhost:6789/api/pipeline_schedules/5/pipeline_runs/69fc290ba1f94cc1866409bac24cb8cb"

# Define start and end year, month
start_year = 2020
start_month = 6
end_year = 2021
end_month = 12

# Function to call the API
def call_api(year, month):
    payload = {
        "pipeline_run": {
            "variables": {
                "year": year,
                "month": month,
                "log_level": "INFO"
            }
        }
    }
    response = requests.post(api_endpoint, headers={"Content-Type": "application/json"}, data=json.dumps(payload))
    if response.status_code == 200:
        print(f"Successfully triggered pipeline for {year}-{month:02d}")
    else:
        print(f"Failed to trigger pipeline for {year}-{month:02d}: {response.text}")

# Loop through the months and years
current_year = start_year
current_month = start_month

print(f'Downloading NYC Yellow trip Data from ${start_year}-${start_month} to ${end_year}-${end_month}')

while current_year < end_year or (current_year == end_year and current_month <= end_month):
    call_api(current_year, current_month)
    time.sleep(45)  # Sleep for 30 seconds
    current_month += 1
    if current_month > 12:
        current_month = 1
        current_year += 1
    print(f'Done fetching {current_year}-{current_month} NYC Green trip Data')
