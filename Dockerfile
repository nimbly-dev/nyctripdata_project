FROM mageai/mageai:latest

# Set argument for user code path
ARG USER_CODE_PATH=de_zoomcamp_nyc_taxi
ARG USER_CODE_PATH=/home/src/${PROJECT_NAME}

# Add Debian Bullseye repository
RUN echo 'deb http://deb.debian.org/debian bullseye main' > /etc/apt/sources.list.d/bullseye.list

# Install dependencies
RUN apt-get update && \
    apt-get install -y wget openjdk-11-jdk


# Verify the JDK installation and set JAVA_HOME
RUN mkdir -p /usr/lib/jvm/java-11-openjdk-amd64 && \
    ln -sfn /usr/lib/jvm/java-11-openjdk-amd64 /usr/lib/jvm/default-java

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PROJECT_NAME=de_zoomcamp_nyc_taxi

# Download the GCS connector jar
RUN wget -P /opt/spark/jars/ https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar

# Download the PostgreSQL JDBC driver
RUN wget -P /opt/spark/jars/ https://jdbc.postgresql.org/download/postgresql-42.2.24.jar

# Copy requirements.txt and install Python dependencies
COPY requirements.txt ${USER_CODE_PATH}/requirements.txt
RUN pip3 install -r ${USER_CODE_PATH}/requirements.txt

#Copy Keys to opt directory
# RUN mkdir keys
# COPY deployment/spark/.keys/my-creds.json keys/my-creds.json

# Set the working directory
WORKDIR ${USER_CODE_PATH}

# Remove Debian Bullseye repository
RUN rm /etc/apt/sources.list.d/bullseye.list

# Clean up APT cache to reduce image size
RUN apt-get clean && rm -rf /var/lib/apt/lists/*

CMD ["mageai", "start", "project"]
