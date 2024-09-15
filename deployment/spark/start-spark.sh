#!/bin/bash

# Source Spark environment variables
. "/opt/spark/bin/load-spark-env.sh"

# Determine the workload type and start the appropriate Spark component
if [ "$SPARK_WORKLOAD" == "master" ]; then
    export SPARK_MASTER_HOST=$(hostname)

    # Start Spark Master
    cd /opt/spark/bin && ./spark-class org.apache.spark.deploy.master.Master \
        --ip $SPARK_MASTER_HOST \
        --port $SPARK_MASTER_PORT \
        --webui-port $SPARK_MASTER_WEBUI_PORT >> $SPARK_MASTER_LOG 2>&1 &

    # Optionally start the shuffle service (if needed)
    if [ "$SPARK_SHUFFLE_SERVICE" == "true" ]; then
        echo "Starting Spark Shuffle Service"
        $SPARK_HOME/sbin/start-shuffle-service.sh >> $SPARK_MASTER_LOG 2>&1 &
    fi

    # Keep the master container running
    tail -f $SPARK_MASTER_LOG

elif [ "$SPARK_WORKLOAD" == "worker" ]; then
    # Start Spark Worker
    cd /opt/spark/bin && ./spark-class org.apache.spark.deploy.worker.Worker \
        --port $SPARK_WORKER_PORT \
        --webui-port $SPARK_WORKER_WEBUI_PORT \
        $SPARK_MASTER >> $SPARK_WORKER_LOG 2>&1 &

    # Keep the worker container running
    tail -f $SPARK_WORKER_LOG

elif [ "$SPARK_WORKLOAD" == "history" ]; then
    # Ensure the log directory exists
    mkdir -p "$SPARK_LOG_DIR"

    # Export SPARK_HISTORY_OPTS if not already exported
    export SPARK_HISTORY_OPTS="$SPARK_HISTORY_OPTS"

    # Start Spark History Server using the provided script
    $SPARK_HOME/sbin/start-history-server.sh

    # Keep the history server running
    tail -f $SPARK_LOG_DIR/spark--org.apache.spark.deploy.history.HistoryServer-1-$(hostname).out

elif [ "$SPARK_WORKLOAD" == "submit" ]; then
    echo "SPARK SUBMIT"

else
    echo "Undefined Workload Type $SPARK_WORKLOAD, must specify: master, worker, history, submit"
fi
