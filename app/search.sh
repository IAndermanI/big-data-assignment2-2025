#!/bin/bash
# This script will run the query.py as a Spark job on YARN.

# Check if query is provided
if [ -z "$1" ]; then
    echo "Error: No search query provided"
    echo "Usage: $0 \"your search query\""
    exit 1
fi

echo "Initializing search process..."

# Activate virtual environment
source .venv/bin/activate

# Configure Python environment
export PYSPARK_DRIVER_PYTHON=$(which python)
export PYSPARK_PYTHON=./.venv/bin/python

# Run search query
echo "Executing search query: $@"
spark-submit \
    --master yarn \
    --deploy-mode client \
    --archives /app/.venv.tar.gz#.venv \
    query.py "$@"

echo "Search process completed."
