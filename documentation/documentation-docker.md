### Running on Docker

This project is containerized using **Docker** to simplify deployment across multiple environments. Docker allows for easy distribution and configuration management by simply editing the service declarations in the `docker-compose.yml` file.

The Docker Compose configuration sets up the following services, all connected through a shared network named **`mage-network`**:

```
├── mage_orchestrator     # Manages workflows and pipelines (Mage AI)
├── spark_master          # Master node managing the Spark cluster
│   ├── spark-worker-1    # Spark worker node 1
│   ├── spark-worker-2    # Spark worker node 2
│   ├── spark-worker-3    # Spark worker node 3
│   └── spark-history     # Spark History Server for monitoring completed jobs
├── pg_admin              # PgAdmin web UI for PostgreSQL management
├── postgres-dev          # PostgreSQL for the development environment
├── postgres-staging      # PostgreSQL for the staging environment
├── postgres-production   # PostgreSQL for the production environment
└── github-runner         # Self-hosted GitHub runner for executing workflows
```

#### Github Runner

The github runner is self-hosted. This will be run upon starting of the container using `docker-compose up -d`. For more information refer to .github/DCOUMENTATION.md. 

#### Spark Configuration

The default Spark service configuration includes the following ports:

- **Master Node**: 7077
- **Worker UI**: 7000
- **Web UI**: 9090
- **Worker Web UI**: 9091, 9092, 909* (for workers)
- **History UI**: 18080


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

Furthermore, these are the service account users that are used by the pipeline in order to query with the databases

| Environment | Service Account Email | Database Name | Role (Privileges) |
|-------------|-----------------------|---------------|-------------------|
| Development | `dev-service-account@de-nyctripdata-project.iam.com` | `nyc_taxi_dev_postgres` | - **SELECT**, **INSERT**, **UPDATE**, **DELETE** on all tables in `public` and `temp` schemas<br>- **CREATE** on `public` and `temp` schemas<br>- **USAGE**, **SELECT** on all sequences in `public` and `temp` schemas |
| Staging | `staging-service-account@de-nyctripdata-project.iam.com` | `nyc_taxi_staging_postgres` | - **SELECT**, **INSERT**, **UPDATE**, **DELETE** on all tables in `public` and `temp` schemas<br>- **CREATE** on `public` and `temp` schemas<br>- **USAGE**, **SELECT** on all sequences in `public` and `temp` schemas |
| Production | `production-service-account@de-nyctripdata-project.iam.com` | `nyc_taxi_production_postgres` | - **SELECT**, **INSERT**, **UPDATE**, **DELETE** on all tables in `public` and `temp` schemas<br>- **CREATE** on `public` and `temp` schemas<br>- **USAGE**, **SELECT** on all sequences in `public` and `temp` schemas |

