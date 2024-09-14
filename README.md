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
This project simulates a **production-grade Data Infrastructure** designed to process NYC trip data through multiple stages: **dev**, **stage**, and **production**. The pipeline handles **millions of trip data records**, ensuring reliability and scalability through techniques like **batch writing**, **disk spill management** and **Parallelization**. The project heavily used Apache Spark's Distributed Computing capabilities to process large datasets effectively.  

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

> **Note**: This guide focuses on the usage of the project and its pipelines. For detailed information on using Mage, please refer to the [Mage documentation](https://docs.mage.ai/introduction/overview).

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

After starting the project, navigate to [http://localhost:6789/](http://localhost:6789/). This will take you to the Mage Dashboard, where you can manage the pipelines and view data processing workflows.

![NYC Tripdata Overview Page](images/documentation/nyc_tripdata_homepage.JPG)

#### Populating the Databases with Tripdata

To populate the databases with NYC trip data, you can use the **spark_populate_tripdata_local_infrastructure** pipeline. This pipeline orchestrates various stages, transforming raw trip data into production-ready formats to be stored in both the Data Warehouse and Data Lakehouse.

#### Getting Started with the Pipeline

1. **Navigate to the Pipelines**:  
   From the Mage Dashboard, click on **Pipelines** in the left-side panel. Then, select the **spark_populate_tripdata_local_infrastructure** pipeline to proceed.

   ![NYC Tripdata Pipeline List](images/documentation/pipeline_list_spark_populate_tripdata_local_infastructure.JPG)

2. **Trigger the Pipeline**:  
   On the pipeline page, navigate to **Trigger** in the left-side panel. Click on the **Run Pipeline orchestration via API** hyperlink to open the pipeline's trigger endpoint.

   ![Pipeline Trigger](images/documentation/trigger_spark_populate_tripdata_local_infastructure.JPG)

3. **Execute the Pipeline via API**:  
   Copy the API URL provided and use a tool like Postman to execute the pipeline.

   ![Trigger Endpoint URL](images/documentation/endpoint_spark_populate_tripdata_local_infastructure.JPG)

##### Example API Request Body

Here’s an example request body that you can use in Postman to run the pipeline:

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
      "data_loss_threshold": "very_strict",
      "is_overwrite_enabled": true
    }
  }
}
```

- **dev_limit_rows**: Set to `-1` to process all rows, or limit the number of rows for testing.
- **start_year / start_month**: Specify the start period for the trip data.
- **end_year / end_month**: Specify the end period for the trip data.
- **pipeline_run_name**: A custom name for the pipeline run.
- **spark_mode**: Choose the Spark execution mode, e.g., `local` or `cluster`.
- **tripdata_type**: The type of trip data to process (e.g., `fhv_cab_tripdata`).
- **data_loss_threshold**: Set to `"very_strict"` for strict error handling during data processing.

Once you send the request, the pipeline will begin processing the data as per the parameters you provided.


##### API Request Body Parameters

| Parameter             | Description                                                                                                                                                            | Example Value                       |
|-----------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-------------------------------------|
| **dev_limit_rows**     | Limit the number of rows for testing. Set to `-1` to process all rows.                                                                                                  | `-1` (process all rows)             |
| **start_year**         | The start year of the trip data to be processed.                                                                                                                        | `2021`                              |
| **start_month**        | The start month of the trip data to be processed.                                                                                                                       | `1` (for January)                   |
| **end_year**           | The end year of the trip data to be processed.                                                                                                                          | `2021`                              |
| **end_month**          | The end month of the trip data to be processed.                                                                                                                         | `12` (for December)                 |
| **pipeline_run_name**  | A custom name for the pipeline run, useful for tracking multiple runs.                                                                                                  | `"populate_fhvtripdata_2022"`       |
| **spark_mode**         | The execution mode for Spark. Set to `"local"` for local execution or `"cluster"` for distributed execution.                                                            | `"cluster"`                         |
| **tripdata_type**      | Specifies the type of trip data to be processed. Possible values: `"yellow_cab_tripdata"`, `"green_cab_tripdata"`, `"fhv_cab_tripdata"`.                                 | `"fhv_cab_tripdata"`                |
| **data_loss_threshold**| Specifies the acceptable level of data loss during processing. Possible values: `"very_strict"` (1% loss), `"strict"` (5% loss), `"moderate"` (10% loss).                | `"very_strict"`                     |
| **is_overwrite_enabled**| Specifies if either to overwrite or update existing data on the PSQL data-warehouse.                                                                                   | `"true"`                     |

**Explanation of Key Parameters:**
- **tripdata_type**: Choose between `yellow_cab_tripdata`, `green_cab_tripdata`, or `fhv_cab_tripdata` based on the dataset you want to process.
- **data_loss_threshold**: Defines how much data loss is acceptable during the data processing pipeline. Here’s how the thresholds break down:
  - `"very_strict"`: Maximum data loss of **1%**.
  - `"strict"`: Maximum data loss of **5%**.
  - `"moderate"`: Maximum data loss of **10%**.
 
#### Structure of the Populate Tripdata Pipeline 

![Trigger Endpoint URL](images/documentation/pipeline_diagram.png)

This diagram is the overview of the Pipeline Orchestration. This follows the Data Proccessing stages that we mentioned on the Project description.

The following is the Pipelines being run by this pipeline Orchestration

```
├── conditional_run_spark_taxi_etl_to_dev_partition # Run Pipeline based on tripdata
├───spark_yellow_taxi_etl_to_dev_partition
├───spark_green_taxi_etl_to_dev_partition
├───spark_fhv_taxi_etl_to_dev_partition
├── check_if_spark_taxi_etil_to_dev_partition_is_done # Run Pipeline based on tripdata
├───spark_load_to_psql_stage
├───spark_psql_stage_to_local_lakehouse_dir
├───spark_psql_stage_to_production
├───data_tmp_cleanup
```

#### Populate Infra Tripdata Pipelines

![Populate Infra Tripdata Pipelines](images/documentation/spark_populate_tripdata_local_infastructure_pipeline_chart.JPG)

The `Populate Infra Tripdata Pipelines` workflow is a series of pipelines that automate the processing of NYC trip data from downloading to production. Each pipeline performs specific tasks to ensure data is correctly processed, cleaned, and loaded into PostgreSQL and the Lakehouse storage.

###### 1. **`spark_taxi_etil_to_dev_partition` Pipeline**

This pipeline is responsible for preparing the trip data for further processing:

- Downloads raw trip data from the NYC Tripdata website.
- Transforms columns by converting data types and renaming columns as necessary.
- Cleans the data for quality assurance.
- Writes the cleaned data to a temporary Parquet directory in the **dev** environment.

###### 2. **`spark_load_to_psql_stage` Pipeline**

This pipeline loads the cleaned trip data into the **staging** PostgreSQL table using an upsert strategy:

- Ingests the cleaned data from the **dev** Parquet directory.
- Runs an additional cleaning process on the data.
- Adds a **dwid** (primary key) to each record in the trip data.
- Appends the data to the **staging** trip data table using an **overwrite** or **upsert** strategy:
  - Writes temporary .csv files that will be copied to the staging table.
  - If overwrite is enabled. It will truncate the selected partitoned table first, and then parallelly copy the csv to the stage table
  - If overwrite is disabled. It will create a temporary table first, and then the temporary table will be populated by parallel PSQL copy command. Finally, we will insert the data using Upsert strategy.

Overwrite Workflow: 
![Spark Load Workflow](images/documentation/psql_load_workflow.gif)

###### 3. **`spark_psql_stage_to_local_lakehouse_dir` Pipeline**

This pipeline moves the trip data from the **staging** PostgreSQL table to the local Lakehouse directory:

- Extracts the data from the **staging** trip data table.
- Writes the data to a **pre-lakehouse** temporary Parquet directory.
- Writes the final data to the **Lakehouse** directory at `/opt/spark/spark-lakehouse/partitioned/{tripdata_type}/data` in Parquet format.

###### 4. **`spark_psql_stage_to_production` Pipeline**

This pipeline transfers data from the **Lakehouse** and **staging** environments to the **production** PostgreSQL table:

- Reads data from both the **Lakehouse** and the **staging** table, writing them into temporary directories.
- Join the two datasets, treating the **Lakehouse** as the source of truth.
- Applies a basic cleaning process to the combined dataset.
- Loads the cleaned and combined data into the **production** trip data table using an **upsert strategy** similar to the one used in the `spark_load_to_psql_stage` pipeline.

### Tripdata Tables

When using Docker Compose, partitioned **Tripdata** tables will be created without data. A series of SQL queries will be executed, and you can review these queries in the deployment folder. The project includes three types of datasets:

| Dataset         | Description                               | Data Dictionary                                                   |
|-----------------|-------------------------------------------|-------------------------------------------------------------------|
| **Yellow Tripdata**  | Data containing trip records for Yellow Cab taxis. | [Yellow Tripdata Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_yellow.pdf) |
| **Green Tripdata**   | Data containing trip records for Green Cab taxis.  | [Green Tripdata Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_green.pdf)  |
| **FHV Tripdata**     | Data containing trip records for For-Hire Vehicles. | [FHV Tripdata Dictionary](https://www.nyc.gov/assets/tlc/downloads/pdf/data_dictionary_trip_records_fhv.pdf)    |



