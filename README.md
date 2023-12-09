# Data Pipelines with Airflow

The data pipelines with airflow project is the project that ingest the data from AWS S3 to AWS Redshift by using apache airflow as orchestrator. There are 3 main steps in the etl pipeline.

1. Copy song and events data from S3 to table `staging_songs` and `staging_events` in Redshift

2. Load fact table and dimension table from `staging_song` and `staging_events`

3. Check data quality of the fact and dimension table
---
# Fact and Dimension Table
There is one fact table of our data model which is `songplays`. The `songplays` table contains record of event.

There are 4 dimension tables.

1. `users`: Contain information of the users
2. `songs`: Contain information of the songs
3. `time`: Contain timestamp of songplays event
4. `artists`: Contain information of the artists

---
# Implementation
In this etl pipeline, we decided to create new operator for implement the pipeline by customized the operator in the plugins. The operator are following below.

1. `StageToRedshiftOperator`: Extract data from S3 to Redshift
2. `LoadFactOperator`: Transform the data from `staging` table to `fact` table
3. `LoadDimensionOperator`: Transform the data from `staging` table to `dimension` table
4. `DataQualityOperator`: Check data quality if the destination table contains the data 

---
# File Summary
All below file table are python file.
| File_name      | Detail   |
| ------------- | :-----|
| song_play_pipeline| Main DAG file |
|sql_queries|Contain Transform and insert queries from `staging` tables to `fact` and `dimension` tables|
| stage_redshift|Class for `StageToRedshiftOperator`|
| load_fact |Class for `LoadFactOperator`|
|load_dimension|Class for `LoadDimensionOperator`|
|data_quality|Class for `DataQualityOperator`|
|create_tables|Script that uses for create all tables in Redshift|

---
# Airflow ETL Process

The following graph is the ETL process that we create in the airflow.

![alt text](./image/airflow_graph.png)
---
# Bash for Running Airflow in Local
```
echo -e "AIRFLOW_UID=$(id -u)" > .env
docker-compose up
```