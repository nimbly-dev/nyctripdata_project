#!/bin/bash

echo "SPARK_WORKLOAD: $SPARK_WORKLOAD"

if [ "$SPARK_WORKLOAD" == "master" ]; then
  start-master.sh -h spark-master -p 7077 --webui-port 8080
elif [ "$SPARK_WORKLOAD" == "worker" ]; then
  WORKER_WEBUI_PORT=${SPARK_WORKER_WEBUI_PORT:-8081}
  echo "Starting Spark Worker on port $WORKER_WEBUI_PORT"
  start-worker.sh spark://spark-master:7077 --webui-port $WORKER_WEBUI_PORT
elif [ "$SPARK_WORKLOAD" == "history" ]; then
  echo "Starting Spark History Server"
  start-history-server.sh
elif [ "$SPARK_WORKLOAD" == "connect" ]; then
  start-connect-server.sh --driver-memory 512M --executor-memory 500M --executor-cores 1
else
  echo "Unknown SPARK_WORKLOAD: $SPARK_WORKLOAD"
  exit 1
fi
