# On-prem/Local Pipeline test123

This guide covers setting up a CI/CD pipeline on GitHub using a self-hosted runner to orchestrate `populate_tripdata` and `fact_tripdata` pipelines. This setup allows us to automatically test for any breaking changes when creating Merge Requests (MRs).

## Overview

Using a self-hosted runner, we can utilize local resources to run Mage orchestration workflows as part of our GitHub Actions CI/CD. This allows for on-prem or local environment testing during pull requests, giving early warnings for potential issues.

## Action Plan

### 1. Set Up a Self-Hosted GitHub Runner

#### Step 1: Register the Runner

1. Go to **Settings > Actions > Runners** in your GitHub repository.
2. Click **New self-hosted runner** and select your machine’s operating system.
3. Follow the provided steps to download, configure, and register the runner on your local machine. Run these commands in your terminal.

#### Step 2: Install and Start the Runner

1. Install any required dependencies for the pipeline:
   ```bash
   pip install -r requirements.txt
   ```
2. Start the runner to keep it ready for jobs:
   ```bash
   ./run.sh  # (or .\run.cmd on Windows)
   ```

### 2. Configure GitHub Actions Workflow

Create the workflow file `.github/workflows/run_pipeline.yml` in your repository to define the CI/CD steps. This is already configured when you pull from master.  


### 3. Verify the Setup

1. Confirm your runner is `Idle` under **Settings > Actions > Runners**.
2. Create a pull request to trigger the workflow.
3. Check logs in GitHub Actions and your terminal to verify the job runs on your local resources.

### Notes

- **Security**: Use GitHub Secrets for sensitive information.
- **Runner Configuration**: Set up the runner as a service for continuous availability.
