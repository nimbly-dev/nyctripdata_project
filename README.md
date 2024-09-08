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

You must have the latest version of Docker and docker-compose installed. 

Before running docker-compose up -d command. Please consult first the [Running on Docker](#Running-on-docker) documentation to modify the image resources if running on a low-end Environment. 

### Prerequisites

Before running the `docker-compose up -d` command, please review the [Running on Docker](#Running-on-docker) section to modify the image resources if running on a low-end environment. Take note of the System Requirements below.

### System Requirements

| Specification       | Suggested Requirements                          | Minimum Requirements (for alternative `docker-compose`) |
|---------------------|-------------------------------------------------|----------------------------------------------------------|
| **CPU**             | 6 cores, 3.5 GHz or higher                      | 3 cores                                                   |
| **RAM**             | 32 GB (16 GB allocated to Docker)               | 16 GB (8 GB allocated to Docker)                          |
| **Spark Cluster**    | 3 workers, each with 2 cores and 4 GB of RAM    | 2 workers, each with 2 cores and 2.5 GB of RAM            |

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
Provide a detailed description of the project structure, how to set it up locally, and any necessary configurations.
