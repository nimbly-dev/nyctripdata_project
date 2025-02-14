# Examples

This directory contains sample sub-projects that uses the existing Data Pipeline and Infrastructure present when you run the main project. This is not needed to run when trying out the project.

## NYC Tripdata Data Dashboard

This Proof of Concept (PoC) project demonstrates a data infrastructure pipeline for analyzing NYC cab data. Using a Jupyter Notebook, I explore and identify insights from cab trip data extracted from our production database, covering `yellow`, `green`, and `fhv` cab types.

### Key Components

1. **Unified Fact Table**: 
   - I combined trip data from `yellow`, `green`, and `fhv` cabs into a single fact table, allowing unified analysis across all cab types. The attributes and metrics included in this fact table are documented for consistent reference.

2. **Dimensional Lookup Tables**:
   - I identified and created dimensional (dim) lookup tables to enrich our analyses, such as tables for `zones`, `rate codes`, `holidays`, `vendors`, and `payment types`.

3. **Materialized Views**:
   - To support efficient querying, we created materialized views based on these dimensional tables. Each materialized view is designed to precompute aggregations and metrics, improving the speed and efficiency of data retrieval for downstream applications.
   - These materialized views are the foundation for visualizations on our dashboard.

4. **Dashboard Visualization**:
   - I developed a dashboard on Google Looker Studio to visualize key metrics and insights. The dashboard URL is [here](https://lookerstudio.google.com/reporting/2b0189c7-098b-4a3d-b463-daa1abcf5b40), with a static copy stored in the `/dashboard` folder.
   - **Note**: The dashboard data is sourced from a Google Sheets file, available [here](https://docs.google.com/spreadsheets/d/1BPRPqt0xZhPokIClULvelAIpou7xPric7O9e_PRXhIM/edit?usp=sharing).

5. **Data Refresh Pipelines**:
   - I implemented pipelines to refresh the materialized views on-demand. These pipelines allow selective refreshing of specific views based on updated data, ensuring that our analyses and dashboard reflect the latest insights.

### Limitations

The current setup requires manual data import:
- The dashboard relies on data uploaded to Google Sheets, meaning any updates to the data must be manually imported via CSV.
- To update the dashboard with new data, run the `dim_mv_refresh_pipeline` to refresh the necessary materialized views, export the data as CSV, and upload it to Google Sheets.
