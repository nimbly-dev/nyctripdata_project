#!/bin/bash

# Navigate to the GitHub runner directory
cd /actions-runner

# Configure the GitHub runner
./config.sh --url $GITHUB_URL \
            --token $GITHUB_TOKEN \
            --name "$RUNNER_NAME" \
            --work "$RUNNER_WORKDIR" \
            --unattended \
            --replace

# Start the runner
./run.sh
