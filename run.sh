#!/bin/bash

# run-spark-clean.sh
# Script to run Spark applications with minimal logging

APP_NAME="$1"
LOG4J_CONFIG="$(pwd)/log4j.properties"

if [ -z "$APP_NAME" ]; then
    echo "Usage: $0 <ScalaFileName>"
    echo "Example: $0 DataFrameBasics"
    exit 1
fi

echo "Running $APP_NAME with clean output..."
echo "================================================"

# Run with SBT and custom log4j configuration
sbt "runMain $APP_NAME" \
    -Dlog4j.configuration=file:$LOG4J_CONFIG \
    -Dspark.ui.showConsoleProgress=false \
    2>/dev/null

echo "================================================"
echo "Execution completed."
