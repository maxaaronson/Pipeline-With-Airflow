# Data Pipeline With Airflow

### Purpose
This project uses `Apache Airflow` to schedule a series of tasks to load and stage data in `Redshift`

### Getting Started
- An `Airflow` server must be running and able to access the `Python` code containing the DAG (dynamic acyclic graph).
- A `Redshift` cluster must also be running which `Airflow` can access via a 'hook'

### Requirements
- `Airflow` and its dependencies must be installed
- A `Redshift` cluster
- An AWS `IAM User` with `S3` read permission and permission to call `Redshift` functions
- The 2 staging tables and 5 fact/dimension tables must be created on the cluster prior to triggering the DAG

### To Run
- A connection profile in `Airflow` must be created:  
	- connection type = postgres
	- host = endpoint of the cluster (without the port)
	- schema = the database to connect to
	- login info
	- port
- Run the DAG from the `Airflow` UI

### Testing
- Graph View or Tree View in the `Airflow` UI can be used to check statuses of jobs and rerun if necessary.
- The pipeline has a `DataQualityOperator`, which checks the fact and dimension tables for data.