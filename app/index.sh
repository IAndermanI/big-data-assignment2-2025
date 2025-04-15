#!/bin/bash

echo "Starting document indexing process..."

# Set default input path and output paths
INPUT_PATH=${1:-/index/data}
OUTPUT_PATH1=/tmp/index/output_1
OUTPUT_PATH2=/tmp/index/output_2

# Configure Hadoop environment
export HADOOP_CONF_DIR=""
export HADOOP_MAPRED_HOME=$HADOOP_HOME
export HADOOP_YARN_HOME=$HADOOP_HOME

# Wait for Cassandra to be fully initialized
echo "Waiting for Cassandra to be fully initialized..."
max_attempts=60
attempt=1
while [ $attempt -le $max_attempts ]; do
    echo "Attempt $attempt of $max_attempts to connect to Cassandra..."
    
    # Try to connect to Cassandra using Python
    python3 -c "
import socket
import sys
try:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(2)
    result = sock.connect_ex(('cassandra-server', 9042))
    sock.close()
    sys.exit(0 if result == 0 else 1)
except Exception as e:
    print(f'Error: {e}')
    sys.exit(1)
"
    
    if [ $? -eq 0 ]; then
        echo "Cassandra is up and running!"
        break
    fi
    
    echo "Cassandra is not ready yet. Waiting 5 seconds..."
    sleep 5
    attempt=$((attempt + 1))
done

if [ $attempt -gt $max_attempts ]; then
    echo "Warning: Cassandra may not be fully initialized. Proceeding anyway..."
fi

# Function to run MapReduce job with timeout
run_mapreduce_job() {
    local input=$1
    local output=$2
    local mapper=$3
    local reducer=$4
    local timeout=300  # 5 minutes timeout
    
    echo "Running MapReduce job..."
    echo "Input: $input"
    echo "Output: $output"
    
    hdfs dfs -rm -r -f $output
    
    # Run the job with timeout
    timeout $timeout hadoop jar $HADOOP_HOME/share/hadoop/tools/lib/hadoop-streaming-*.jar \
        -D mapreduce.framework.name=local \
        -input "$input" \
        -output "$output" \
        -file mapreduce/$mapper \
        -file mapreduce/$reducer \
        -mapper "python3 $mapper" \
        -reducer "python3 $reducer"
    
    # Check if the job timed out
    if [ $? -eq 124 ]; then
        echo "ERROR: MapReduce job timed out after $timeout seconds"
        return 1
    fi
    
    return 0
}

# Run first MapReduce job (Inverted Index)
echo "Starting inverted index creation..."
if ! run_mapreduce_job "$INPUT_PATH" "$OUTPUT_PATH1" "mapper1.py" "reducer1.py"; then
    echo "ERROR: First MapReduce job failed. Exiting."
    exit 1
fi

# Run second MapReduce job (Statistics)
echo "Starting statistics calculation..."
if ! run_mapreduce_job "$INPUT_PATH" "$OUTPUT_PATH2" "mapper2.py" "reducer2.py"; then
    echo "ERROR: Second MapReduce job failed. Exiting."
    exit 1
fi

echo "Indexing process completed successfully!"
