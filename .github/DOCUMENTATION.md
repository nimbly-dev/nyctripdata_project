# On-Prem Runner Pipeline

This repository contains workflows designed to run using GitHub Self-Hosted Runners. The runner is initiated by executing the following command in the root folder of the repository:

```bash
docker-compose up -d
```

The runner setup automatically creates a GitHub Runner and generates a Runner Token each time Docker is started. This process requires specific environment variables, particularly a **Personal Access Token (PAT)**.

## Required Environment Variables

Create a `.env` file in the root folder and define the following variables:

```plaintext
GITHUB_PAT= # Your GitHub Personal Access Token
REPO_OWNER=nimbly-dev # Repository owner (organization or user)
REPO_NAME=nyctripdata_project # Repository name
```

### Access Scope for Personal Access Token

Ensure your **Personal Access Token (PAT)** has the following permissions enabled:

1. **`repo`**: Full repository access
2. **`workflow`**: Workflow read and write permissions
3. **`admin:org`**:
   - **`manage_runners`**: Permission to manage runners

## Running Workflows via PR Comments

Workflows can be triggered by commenting specific commands on a pull request. When triggered, GitHub will respond with the workflow details and pipeline status.

## Supported Workflows

The following workflows are currently supported:

1. **CI:run_yellow_etl_dev**  
   Runs the ETL pipeline for the Yellow dataset in the development environment.
   
2. **CI:run_green_etl_dev**  
   Runs the ETL pipeline for the Green dataset in the development environment.
   
3. **CI:run_fhv_etl_dev**  
   Runs the ETL pipeline for the FHV dataset in the development environment.
   
4. **CI:populate_tripdata:{cab_type}**  
   Populates trip data for the specified cab type (`yellow`, `green`, or `fhv`).  
   Example:  
   ```plaintext
   CI:populate_tripdata:yellow
   ```

5. **CI:populate_fact_tripdata:{environment}**  
   Populates fact trip data for the specified environment (`stage` or `production`).  
   Example:  
   ```plaintext
   CI:populate_fact_tripdata:production
   ```