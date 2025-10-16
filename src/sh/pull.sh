#!/bin/bash

cd /airflow/dags/de-atlassian-issues
git pull

# Optional: Add error handling
if [ $? -eq 0 ]; then
  echo "Git pull completed successfully."
else
  echo "Error during git pull."
  exit 1
fi

chmod a+x src/sh/pull.sh