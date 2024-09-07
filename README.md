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
- [Documentation](#documentation)

## 🚀 About
This project simulates a **production-grade Data Infrastructure** designed to process NYC trip data through multiple stages: **dev**, **stage**, and **production**. The pipeline handles **millions of trip data records**, ensuring reliability and scalability through techniques like **batch writing** and **disk spill management**.

## 🗂️ Project Infrastructure
![Environment Diagram](images/environment_diagram.png)

The project leverages both **Data Lakehouse** and **Data Warehouse** concepts to ensure efficient data management:
- **Data Lakehouse**: Local storage is organized under the `spark-lakehouse` directory, where temporary files, downloads, and processed trip data are stored.
- **Data Warehouse**: The data workflow transitions data across **dev**, **stage**, and **production** PostgreSQL databases, ensuring a smooth lifecycle management process.

This pipeline demonstrates the capability to handle large datasets with high reliability and efficiency in a production-like environment.

## 📊 Dataset
The data is sourced from the [NYC Taxi & Limousine Commission Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page). Many data engineering principles used in this project are inspired by the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).

## 📚 Documentation
Provide a detailed description of the project structure, how to set it up locally, and any necessary configurations.
