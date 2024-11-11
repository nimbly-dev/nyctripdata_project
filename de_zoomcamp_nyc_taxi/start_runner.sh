#!/bin/bash

export GITHUB_API_URL="https://api.github.com"
export PAT="$GITHUB_PAT"
export OWNER="nimbly-dev"
export REPO="nyctripdata_project"

# Fetch the token directly and check
response=$(curl -s -X POST -H "Authorization: token $PAT" "$GITHUB_API_URL/repos/$OWNER/$REPO/actions/runners/registration-token")
TOKEN=$(echo "$response" | jq -r .token)

if [ "$TOKEN" == "null" ]; then
    echo "Failed to retrieve a valid token. Response was: $response"
    exit 1
fi

# Run config with the token
cd /actions-runner
./config.sh --url "https://github.com/$OWNER/$REPO" --token "$TOKEN" --unattended --replace
./run.sh
