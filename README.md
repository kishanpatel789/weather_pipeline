# Weather Pipeline

**Objective**: This project presents data engineering principles by developing a pipeline for weather data. The pipeline extracts, processes, and loads U.S. precipitation data. A cloud data warehouse is implemented for analytics consumption. 

Project Start: 2023.01.20

## Technologies Used
- Terraform
- AWS: EMR, Redshift, S3
- Docker
- Apache Airflow
- Apache Spark

## Data Source
- [Hourly Precipitation Data (HPD)](https://www.ncei.noaa.gov/data/coop-hourly-precipitation/v2/): The National Centers for Environmental Information (NCEI) receives Hourly Precipitation Data (HPD) from the National Weather Service (NWS). 
  - The NCEI collects precipitation measurements at 15-minute and hourly increments at many weather stations within the United States.
  - There is one file per weather station with the oldest records going back to year 1940. 
  - The project makes use of the full archive, which consists of 2009 files covering precipitation from 1940-01-01 to 2022-09-06. When uncompressed, the 2009 CSV files occupy about 19 GB of storage.
  - The file structure is denormalized with series of columns for each hour. Each record of a file represents one day. 

## Architecture and Pipeline Outline
- Raw data in the form of 2,009 compressed CSV files are extracted from the NOAA NCEI website. 
- Each CSV file is converted into a parquet file with appropriate schema and stored in an AWS S3 bucket serving as a data lake. 
- Apache Spark is utilized via AWS EMR to combine the parquet files and cleanse the dataset. The following high-level steps are followed:
  - Create zipped arrays of hourly data with 5 measurements for each hour.
  - Explode zipped arrays to effectively unpivot the dataset into a hourly table. The new unique identifier is a combination of station, date, and hour-of-day. 
  - Replace null and missing values.
  - Coalesce and write parquet files back to data lake (S3).
- Transfer cleaned parquet files into AWS Redshift cluster serving as a data warehouse. 
- 


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