# Use a single-stage build
FROM openjdk:11.0.11-jre-slim-buster

# Install dependencies for PySpark and general utilities
RUN apt-get update && apt-get install -y \
    curl wget software-properties-common ssh net-tools ca-certificates \
    python3-pip python3-dev \
    build-essential libssl-dev zlib1g-dev libncurses5-dev libgdbm-dev \
    libnss3-dev libreadline-dev libffi-dev libsqlite3-dev libbz2-dev liblzma-dev tk-dev procps \
    rsync \
    && rm -rf /var/lib/apt/lists/*

# Install Python 3.10.14
RUN wget https://www.python.org/ftp/python/3.10.14/Python-3.10.14.tgz \
    && tar -xzf Python-3.10.14.tgz \
    && cd Python-3.10.14 \
    && ./configure --enable-optimizations \
    && make -j$(nproc) \
    && make altinstall \
    && cd .. \
    && rm -rf Python-3.10.14 Python-3.10.14.tgz \
    && ln -sf /usr/local/bin/python3.10 /usr/bin/python \
    && ln -sf /usr/local/bin/pip3.10 /usr/bin/pip

# Download and setup Hadoop
RUN wget -qO- https://archive.apache.org/dist/hadoop/common/hadoop-3.3.4/hadoop-3.3.4.tar.gz | tar xvz -C /opt/ \
    && ln -s /opt/hadoop-3.3.4 /opt/hadoop

# Install Spark
ENV SPARK_VERSION=3.5.1 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark 

RUN wget -q -O apache-spark.tgz "https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz" \
    && mkdir -p /opt/spark \
    && tar -xf apache-spark.tgz -C /opt/spark --strip-components=1 \
    && rm apache-spark.tgz

# Set working directory
WORKDIR /opt/spark

# Environment variables for Spark
ENV PYSPARK_PYTHON=/usr/bin/python \
    PYSPARK_DRIVER_PYTHON=/usr/bin/python \
    SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_LOG_DIR=/opt/spark/logs \
    SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
    SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
    SPARK_LOCAL_DIRS=/opt/spark/data \
    SPARK_WORKER_WEBUI_PORT=8081 \
    SPARK_WORKER_PORT=7000 \
    SPARK_MASTER="spark://spark-master:7077" \
    GOOGLE_APPLICATION_CREDENTIALS=/opt/keys/my-creds.json

# Expose necessary ports
EXPOSE 7077 8080 8081 4040 18080

# Create necessary directories and set permissions
RUN mkdir -p $SPARK_LOG_DIR $SPARK_LOCAL_DIRS /opt/spark/data/warehouse /tmp/spark-events /opt/spark/data/checkpoints /opt/spark/data/tmp /opt/spark/spark-lakehouse /opt/spark/logs/eventLog \
    && chmod -R 777 /opt/spark

# Download necessary JARs
RUN mkdir -p /opt/spark/third-party-jars/ \
    && wget -q -P /opt/spark/third-party-jars/ https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar \
    && wget -q -P /opt/spark/third-party-jars/ https://jdbc.postgresql.org/download/postgresql-42.2.24.jar \
    && wget -q -P /opt/spark/third-party-jars/ https://storage.googleapis.com/spark-lib/bigquery/spark-3.5-bigquery-0.40.0.jar 

# Copy Spark configurations and scripts
COPY spark-config/spark-defaults.conf /opt/spark/conf/
COPY .keys /opt/spark/keys
COPY start-spark.sh /

# Make the start script executable
RUN chmod +x /start-spark.sh

# Clean up unnecessary files to reduce image size
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

# Set the entry point
CMD ["/bin/bash", "/start-spark.sh"]
