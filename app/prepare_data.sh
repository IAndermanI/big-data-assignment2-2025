#!/bin/bash

# Activate virtual environment
source .venv/bin/activate

# Set Python environment variables
export PYSPARK_DRIVER_PYTHON=$(which python)
unset PYSPARK_PYTHON

# Create necessary HDFS directories
echo "Setting up HDFS directory structure..."
hdfs dfs -put data / && \
hdfs dfs -mkdir -p /index/data && \
hdfs dfs -mkdir -p /tmp/index/pipeline1 && \
hdfs dfs -mkdir -p /tmp/index/pipeline2

# Verify directory creation
echo "Verifying HDFS directories..."
hdfs dfs -ls /data && \
hdfs dfs -ls /index/data

echo "Data preparation completed successfully!"