# Bitcoin Data Engineering and Analytics Project

This project involves building an end-to-end data pipeline for analyzing Bitcoin market data using CoinAPI and dbt. The pipeline ingests raw Bitcoin data, transforms it using dbt, and performs analytical tasks to extract insights from the data. The project uses Apache Airflow for orchestration and Docker for containerization.

## Project Overview

The Bitcoin Data Pipeline is structured to process historical and real-time data from CoinAPI, perform necessary data transformations, and conduct various analyses such as trend, volatility, and volume analysis on the Bitcoin market. The key stages of the pipeline are as follows:

1. **Data Ingestion**: Fetching raw Bitcoin data from CoinAPI and storing it in a data warehouse.
2. **Data Transformation**: Cleaning and transforming the raw data with dbt models.
3. **Analytics**: Running SQL-based analysis on the transformed data, including momentum, volatility, and volume analysis.
4. **Data Orchestration**: Using Airflow to automate and schedule the pipeline.
5. **Containerization**: Docker is used to create a consistent environment for running the pipeline.

## Table of Contents

1. [Technologies Used](#technologies-used)
2. [Setup Instructions](#setup-instructions)
3. [Folder Structure](#folder-structure)
4. [Pipeline Workflow](#pipeline-workflow)
5. [Sample Outputs](#sample-outputs)
6. [License](#license)

## Technologies Used

- **CoinAPI**: Used for fetching historical and real-time Bitcoin data.
- **dbt (Data Build Tool)**: For transforming and modeling raw Bitcoin data.
- **Apache Airflow**: For orchestrating the pipeline and managing workflows.
- **Docker**: To containerize the project for easy deployment and execution.
- **Python**: For the scripting and ingestion of data.
- **PostgreSQL**: The data warehouse used to store the transformed Bitcoin data.
- **Docker Compose**: For setting up and managing multi-container Docker applications.

## Setup Instructions

### 1. Install Dependencies

Clone the repository:

```bash
git clone https://github.com/your-username/bitcoin-data-pipeline.git
cd bitcoin-data-pipeline
```

Build and start the Docker containers:

```bash
docker-compose up --build
```

This will start all necessary services, including PostgreSQL, Airflow, and dbt.

### 2. Set Up API Key

1. Create an account at [CoinAPI](https://www.coinapi.io/).
2. Obtain your API key and add it to the `.env` file in the root of the project.

### 3. Configure dbt

Make sure to set up the `dbt_project.yml` and `profiles.yml` files with the correct Snowflake/PostgreSQL connection details, depending on where you want to store your data.

### 4. Run the Data Pipeline

Start the Airflow services and schedule the data pipeline:

```bash
docker-compose up airflow
```

The pipeline will be orchestrated by Airflow, which will call the necessary ETL scripts for data ingestion, transformation, and analysis.

### 5. Monitor the Pipeline

You can monitor and manage the pipeline through the Airflow UI:

- Access the Airflow UI at: `http://localhost:8080`
- The default credentials are `admin` for both the username and password.

- ## Folder Structure
│   ├── config/                     # Configuration files for the Airflow setup
│   ├── dags/                       # Airflow Directed Acyclic Graphs (DAGs) to define tasks
│   │   ├── BTC-DBT.py              # Airflow DAG for running dbt tasks
│   │   └── BTC-ETL.py              # Airflow DAG for running the ETL process
│   ├── logs/                       # Logs for Airflow tasks
│   └── plugins/                    # Custom Airflow plugins if needed
│
├── dbt/
│   ├── models/                     # dbt transformation models
│   │   ├── input/                  # Raw data models
│   │   ├── output/                 # Transformed data models
│   │   ├── momentum_analysis.sql   # SQL for momentum analysis model
│   │   ├── trend_analysis.sql      # SQL for trend analysis model
│   │   ├── volatility_analysis.sql # SQL for volatility analysis model
│   │   └── volume_analysis.sql     # SQL for volume analysis model
│   ├── seeds/                      # Static data files for dbt
│   ├── snapshots/                  # dbt snapshot configurations
│   ├── target/                     # dbt target directory (output of runs)
│   ├── macros/                     # dbt macros
│   └── dbt_project.yml             # Configuration for dbt project
│
├── docker-compose.yml              # Docker Compose configuration file
├── Dockerfile                      # Docker configuration for building the container
├── .gitignore                      # Git ignore file
├── .user.yml                       # User configuration for dbt
├── profiles.yml                    # dbt profile configuration
├── README.md                       # Project documentation
└── .env                            # Environment variables (e.g., CoinAPI key)
```

## Pipeline Workflow

1. **Data Ingestion**: 
   - The `BTC-ETL.py` script is responsible for ingesting data from CoinAPI and storing it in the database. 
   - This task is scheduled and managed by Apache Airflow.

2. **Data Transformation**: 
   - The `BTC-DBT.py` script runs the dbt transformations to clean and model the raw data into a consumable format.
   - dbt models like `momentum_analysis.sql`, `trend_analysis.sql`, and others are executed to produce the desired output tables.

3. **Analytics**: 
   - Various analysis queries are executed on the transformed data to generate insights on Bitcoin's price trends, volatility, and trading volume.

4. **Orchestration**: 
   - Apache Airflow manages the entire pipeline, scheduling the data ingestion and transformation tasks.

5. **Containerization**: 
   - Docker ensures the project runs consistently across environments by packaging all dependencies in containers.

## Sample Outputs

- **Momentum Analysis**: SQL analysis for measuring the momentum of Bitcoin prices.
- **Trend Analysis**: Visual representation of Bitcoin's long-term price trends.
- **Volatility Analysis**: Insights on how volatile Bitcoin has been over certain time periods.
- **Volume Analysis**: Correlation between price movements and trading volume.
