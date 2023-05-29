# Weather Pipeline
Project Start: 2023.01.20

Objective: Develop a pipeline of weather data.

## Technologies Used
- Terraform
- AWS: EMR, Redshift, S3
- Docker
- Airflow
- Spark

## Data Source
- [Hourly Precipitation Data (HPD)](https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/) - Collection of 15 minute and hourly precipitation data for many locations within a U.S. state.
    - One file per weather station with oldest records going back to 1940. 
    - Full archive is 19GB and 2009 files when unzipped (1940.01.01 to 2022.09.06)
    - File structure is like pivot table with series of columns for each hour. One row per day. 

## Architecture
- Docker is used to run a local implementation of Airflow
  - 4 dags
- S3 serves as data lake of raw and processed data
- EMR provides a compute resource to process raw precipitation data
- Redshift serves as data warehouse with processed precipitation data and aggregated data mart table for analytics reportnig

## Setup Instructions
- AWS
  - set up service account
  - configure aws cli with service account as a profile
  - Create redshift role "redshift-cluster-role" that has AmazonS3ReadOnlyAccess and AmazonRedshiftAllCommandsFullAccess policies
- Terraform
  - Update variables.tf (region, profile, bucket, redshift_cluster_name, redshift_cluster_role)
  - Execute terraform apply to create S3 bucket and redshift cluster
- Airflow
  - Update docker-compose.yaml environment variables under x-airflow-common:
    - AWS_BUCKET
    - AWS_PROFILE
  - Build docker image and run
  - Set up UI config for Redshift connector
    - Connection Id = redshift-ui-1
    - Connection Type = "Amazon Redshift"
    - Host = 
    - Schema = dev
    - Login = 
    - Password = 
    - Port = 5439

## How to Run
- Execute DAGS in order
  - 