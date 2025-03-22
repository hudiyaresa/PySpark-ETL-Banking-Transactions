# Data Pipeline for ETL Processing

## Project Overview

This project demonstrates the implementation of a scalable data pipeline using Docker and PySpark. It integrates data from multiple sources, including a PostgreSQL transactional database and CSV files, and loads the processed data into a PostgreSQL data warehouse.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Prerequisites](#prerequisites)
3. [Setup Instructions](#setup-instructions)
4. [Running the Project](#running-the-project)
5. [Dataset](#dataset)
6. [Problem Statement](#problem-statement)
7. [Solution Approach](#solution-approach)
8. [Data Pipeline Design](#data-pipeline-design)
9. [Data Processing Steps](#data-processing-steps)
    - [Extract](#extract)
    - [Transform](#transform)
    - [Load](#load)
10. [Logging](#logging)

## Prerequisites

Before you begin, ensure you have met the following requirements:
- Docker installed on your local machine

## Setup Instructions

1. Clone the repository to your local machine.
2. Navigate to the project directory.
3. Run the Docker Compose setup.

## Running the Project

To build and run the project, execute the following commands in your terminal:

```bash
docker compose build --no-cache
docker compose up -d
```

After the containers are up and running, if the required libraries and dependencies are not installed, install them manually:

```bash
docker exec -it pyspark_container pip install sqlalchemy psycopg2-binary
```

To access Jupyter Notebook on the PySpark container, check the container logs to get the token:

```bash
docker logs pyspark_container
```

Example output:
```bash
http://127.0.0.1:8888/lab?token=3fa3b1cf2c67643874054971f23ee59bdee283b373794847
```

## Dataset

### 1. Source Database (`source_db` container)
- A PostgreSQL database containing structured marketing campaign data.

### 2. CSV File (`/script/data/new_bank_transaction.csv`)
- A large dataset containing transactional records.
- Requires transformation before loading into the Data Warehouse.

### 3. Warehouse Database (`data_warehouse` container)
- A PostgreSQL database containing structured schema Tables include `customers`, `transactions`, `marketing_campaign_deposit`, `education_status` and `marital status`.


## Problem Statement

1. Data is spread across multiple sources (PostgreSQL and CSV files).
2. Large file sizes can impact computational efficiency.
3. Data is not in a structured format suitable for analysis.

## Solution Approach

- Implement an **ETL (Extract, Transform, Load) pipeline** using PySpark.
- Use Docker to containerize services (PySpark, PostgreSQL Source DB, and Data Warehouse).
- Standardize and clean data before inserting it into the warehouse.
- Automate logging for tracking pipeline execution.

## Data Pipeline Design

A structured ETL pipeline is implemented with the following steps:

```
![ELT Pipeline](docs/ETL_Pipeline_Flow_Diagram.png)
```

### Source to Target Mapping

A predefined transformation process ensures smooth data movement from the source database and CSV files to the data warehouse. Refer to the [Source-to-Target Mapping Documentation](https://github.com/hudiyaresa/PySpark-ETL-Banking-Transactions/source-to-target-map.md) for details.


## Data Processing Steps

### Extract
- Extract data from the source PostgreSQL database and CSV files.
- Uses `spark.read.csv("data/{filename}", header=True)` for CSV extraction.

### Transform
- **Data Cleaning:** Standardizes and formats data.
- **Data Casting:** Ensures correct data types (e.g., converting currency strings to numeric values).
- **Date Formatting:** Converts date fields into consistent formats.
- **Column Renaming:** Aligns column names with warehouse schema.
- **Data Selection:** Chooses relevant columns for the final dataset.

### Load
- Inserts transformed data into the PostgreSQL Data Warehouse.
- Uses the `append` method to maintain historical records.

## Logging
All logs generated during the execution of the data pipeline are saved in the following file:

```plaintext
/script/log/log.info
```

