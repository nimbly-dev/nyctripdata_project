---
# Data Engineering Project: NYC Tripdata Data Infrastructure

![Spark](https://img.shields.io/badge/Spark-3.5.1-orange)
![Python](https://img.shields.io/badge/Python-3.10.14-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-blue)
![Docker](https://img.shields.io/badge/Docker-Available-blue)
![Mage](https://img.shields.io/badge/Mage-Orchestration-orange)

## Table of Contents
- [About](#-about)
- [Project Infrastructure](#-project-infrastructure)
- [Dataset](#-dataset)
- [Setup](#-setup)
- [Documentation](#documentation)

## 🚀 About
This project simulates a **production-grade Data Infrastructure** designed to process NYC trip data through multiple stages: **dev**, **stage**, and **production**. The pipeline handles **millions of trip data records**, ensuring reliability and scalability through techniques like **batch writing** and **disk spill management**.

## 🗂️ Project Infrastructure
![Environment Diagram](images/environment_diagram.png)

The data processing in this project follows a **vertical pipeline** architecture with three stages:
1. **Development**: Focuses on data cleaning, column transformations, and preparing raw data for subsequent stages.
2. **Staging**: Responsible for **data governance**—applying rules to the datasets, modifying columns, and ensuring that data quality is maintained throughout the process.
3. **Production**: In this final stage, data is ready for **feature extraction**, **reporting**, and **analysis**.

The project leverages both **Data Lakehouse** and **Data Warehouse** architectures for effective data management:
- **Data Lakehouse**: This is where raw and intermediate data is stored. The local storage is organized under the `spark-lakehouse` directory, where temporary files, downloads, and processed trip data are housed.
- **Data Warehouse**: The processed data moves through a series of PostgreSQL databases: **dev**, **stage**, and **production**, ensuring a smooth transition across the lifecycle and enhancing governance and data management.

This pipeline architecture demonstrates the system’s ability to handle **large datasets** with high **reliability** and **efficiency**, mimicking a production-like environment.

## 📊 Dataset
The data is sourced from the [NYC Taxi & Limousine Commission Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Many data engineering principles used in this project are inspired by the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).

## 📝 Setup

### Prerequisites

Before running the `docker-compose up -d` command, please review the [Running on Docker](###Running-on-docker) section to modify the image resources if running on a low-end environment. Take note of the System Requirements below.

You must have the latest version of Docker and docker-compose installed. Furthermore, you also must have Postman to trigger the pipeline.

### System Requirements

| Specification       | Suggested Requirements                          | Minimum Requirements (for alternative `docker-compose`) TODO |
|---------------------|-------------------------------------------------|----------------------------------------------------------|
| **CPU**             | 6 cores, 3.5 GHz or higher                      | 3 cores                                                   |
| **RAM**             | 32 GB (16 GB allocated to Docker)               | 16 GB (8 GB allocated to Docker)                          |
| **Spark Cluster**    | 3 workers, each with 2 cores and 4 GB of RAM    | 2 workers, each with 2 cores and 2.5 GB of RAM            |
| **Storage**         | 30-50 GB                                        | 30-50 GB                                                  |


### Running Project using Docker Compose

```shell
# Open a terminal (Command Prompt or PowerShell for Windows, Terminal for macOS or Linux)

# Ensure Git is installed
# Visit https://git-scm.com to download and install console Git if not already installed

# Clone the repository
git clone https://github.com/Abblix/Oidc.Server.git](https://github.com/nimbly-dev/nyctripdata_project.git

# Navigate to the project directory
cd nyctripdata_project

# Execute this command, wait until installation is complete and after finishing it will start the server.
docker-compose up -d
```

## 📚 Documentation

### Running on Docker

This project is containerized using **Docker** to simplify deployment across multiple environments. Docker allows for easy distribution and configuration management by simply editing the service declarations in the `docker-compose.yml` file.

The Docker Compose configuration sets up the following services, all connected through a shared network named **`mage-network`**:

```
├── mage_orchestrator     # Manages workflows and pipelines (Mage AI)
├── spark_master          # Master node managing the Spark cluster
│   ├── spark-worker-1    # Spark worker node 1
│   ├── spark-worker-2    # Spark worker node 2
│   └── spark-worker-3    # Spark worker node 3
├── pg_admin              # PgAdmin web UI for PostgreSQL management
├── postgres-dev          # PostgreSQL for the development environment
├── postgres-staging      # PostgreSQL for the staging environment
└── postgres-production   # PostgreSQL for the production environment
```

#### Spark Configuration

The default Spark service configuration includes the following ports:

- **Master Node**: 7077
- **Worker UI**: 7000
- **Web UI**: 9090
- **Worker Web UI**: 9091, 9092, 909* (for workers)

#### Adding a New Spark Worker

If you need to add additional Spark workers to the cluster, you can easily append a new worker configuration to the `docker-compose.yml`. Below is an example configuration to add a new Spark worker:

```yaml
  spark-new-worker:
    image: cluster-apache-spark:python3.10.14-spark3.5.1
    container_name: spark-new-worker
    entrypoint: ['/bin/bash', '/start-spark.sh', 'worker']
    networks:
      - mage-network
    depends_on:
      - spark-master
    environment:
      - SPARK_MASTER=spark://spark-master:7077
      - SPARK_WORKER_CORES=2  
      - SPARK_WORKER_MEMORY=4G 
      - SPARK_WORKLOAD=worker
      - SPARK_LOCAL_IP=spark-new-worker
      - PYSPARK_PYTHON=${PYSPARK_PYTHON}
      - PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON}
      - SPARK_EVENTLOG_DIR=${SPARK_EVENTLOG_DIR}
      - SPARK_HISTORY_DIR=${SPARK_HISTORY_DIR}
      - SPARK_WAREHOUSE_DIR=${SPARK_WAREHOUSE_DIR}
      - SPARK_LAKEHOUSE_DIR=${SPARK_LAKEHOUSE_DIR}
      - SPARK_CHECKPOINT_DIR=${SPARK_CHECKPOINT_DIR}
      - SPARK_LOCAL_DIR=${SPARK_LOCAL_DIR}
    volumes:
      - spark-home:/opt/spark
      - ./spark-data:/opt/spark/work
    ports:
      - "{CHANGE_PORT_HERE}:8081"  # Change this port to avoid conflicts
    deploy:
      resources:
        limits:
          cpus: "2"
          memory: "4G"
```

#### Database Configurations

The following are the default hostname and ports of our Postgres databases.

| Database Name               | Hostname            | Port  |
|-----------------------------|---------------------|-------|
| **nyc_taxi_dev_postgres**    | `postgres-dev`      | 5432  |
| **nyc_taxi_staging_postgres**| `postgres-staging`  | 5433  |
| **nyc_taxi_production_postgres** | `postgres-production` | 5434  |

### Usage

Note: I will not delve deep on how to use Mage. For mage documentation refer to this: https://docs.mage.ai/introduction/overview

After running up the Project Application, navigate to http://localhost:6789/. It will then show you the Mage Dashboard

![NYC Tripdata Overview Page](images/documentation/nyc_tripdata_homepage.JPG)

For populating the databases with Tripdata. We can use the **spark_populate_tripdata_local_infastructure** Pipeline. This Pipeline contains the Orchestration of different pipelines that proccesses tripdata from its raw form to its production-ready data
to be transfered to our Data Warehouse and Data Lakehouse. 

To get started. From the Mage Dashboard Page, Navigate to Pipelines from the Left-Side Panel, and then click the **spark_populate_tripdata_local_infastructure** Pipeline 

![NYC Tripdata Overview Page](images/documentation/pipeline_list_spark_populate_tripdata_local_infastructure.JPG)

After going to the pipeline page, navigate to Trigger from the Left-Side Panel, and then click **Run Pipeline orchestration via API** hyperlink. 

![NYC Tripdata Overview Page](images/documentation/trigger_spark_populate_tripdata_local_infastructure.JPG)

Copy the URL and paste it to your Postman Application

![NYC Tripdata Overview Page](images/documentation/endpoint_spark_populate_tripdata_local_infastructure.JPG)

This is an sample request body:

```json
{
  "pipeline_run": {
    "variables": {
      "dev_limit_rows" : -1,
      "end_month": 12,
      "end_year": 2021,
      "start_month": 1,
      "start_year": 2021,
      "pipeline_run_name": "populate_fhvtripdata_2022",
      "spark_mode" : "cluster",
      "tripdata_type": "fhv_cab_tripdata",
      "data_loss_threshold": "very_strict"
    }
  }
}
```


