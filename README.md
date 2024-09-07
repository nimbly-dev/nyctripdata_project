![Spark](https://img.shields.io/badge/Spark-3.5.1-orange)
![Python](https://img.shields.io/badge/Python-3.10.14-blue)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-14-blue)
![Docker](https://img.shields.io/badge/Docker-Available-blue)
![Mage](https://img.shields.io/badge/Mage-Orchestration-orange)
[![OS](https://img.shields.io/badge/OS-linux%2C%20windows%2C%20macOS-0078D4)](https://docs.abblix.com/docs/technical-requirements)
---
# Data Engineering Project: NYC Tripdata Data Infrastructure

This project simulates a production-grade Data Infrastructure, designed to process NYC trip data through multiple stages: **dev**, **stage**, and **production**. The pipeline handles **millions of trip data records**, ensuring reliability and scalability through techniques like **batch writing** and **disk spill management**. 

### Dataset
The data used comes from the [NYC Taxi & Limousine Commission Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page), and many data engineering principles were inspired by the [DataTalksClub Data Engineering Zoomcamp](https://github.com/DataTalksClub/data-engineering-zoomcamp).

### Data Architecture
The project leverages both **Data Lakehouse** and **Data Warehouse** concepts:
- **Data Lakehouse**: Local storage is organized under the `spark-lakehouse` directory, where temporary files, downloads, and processed trip data are stored.
- **Data Warehouse**: The workflow transitions data across **dev**, **stage**, and **production** PostgreSQL databases, ensuring smooth data lifecycle management.

This pipeline demonstrates the capability of handling large datasets with high reliability and efficiency in a production-like environment.

