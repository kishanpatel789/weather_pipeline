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
  - The project makes use of the full archive, which consists of 2,009 files covering precipitation from 1940-01-01 to 2022-09-06. When uncompressed, the 2009 CSV files occupy about 19 GB of storage.
  - The file structure is denormalized with a series of columns for each hour. Each record in a file represents one day. 

## Architecture and Pipeline Outline
- Terraform is used to create and maintain a S3 bucket serving as a data lake and a Redshift cluster serving as a data warehouse. 
- Raw precipitation data is processed via Apache Spark on an ephemeral AWS EMR cluster. 
- Orchestration of the pipeline is handled by Apache Airflow running in a custom Docker container on a local machine. A sequence of four DAGS complete the following steps: 
  - Extract raw data in the form of 2,009 compressed CSV files from the NOAA NCEI website. 
  - Uncompress each CSV file and convert into a parquet file with appropriate schema. Transfer the 2,009 corresponding parquet files to the S3 bucket. 
  - Run EMR job flow to combine the parquet files and cleanse the dataset. The following high-level processing steps are followed:
    - Create zipped arrays of hourly data with 5 measurements for each hour.
    - Explode zipped arrays to effectively unpivot the dataset into a hourly table. The new unique identifier is a combination of station, date, and hour-of-day. 
    - Replace null and missing values.
    - Coalesce and write parquet files back to data lake.
  - Copy cleansed parquet files into Redshift cluster and build a fact table of hourly precipitation. 
  - Transfer two dimension tables summarizing state and station to Redshift cluster.
  - Create aggregated data mart table in Redshift that summarizes precipitation by year and state. 
- 
## Setup Instructions
- AWS
  - Create the following roles with AWS managed policies assigned:
    - emr-role: AmazonElasticMapReduceRole
    - emr-ec2-role: AmazonElasticMapReduceforEC2Role
    - redshift-cluster-role: AmazonS3ReadOnlyAccess, AmazonRedshiftAllCommandsFullAccess 
  - Create service user in AWS that will be utilized by Airflow. The user must have the following AWS mananged policies assigned: 
    - AmazonEC2FullAccess	
    - AmazonEMRFullAccessPolicy_v2
    - AmazonRedshiftFullAccess
    - AmazonS3FullAccess  
  - The service user must also have inline policies allowing the action "iam:PassRole" to the following resources:
    - arn:aws:iam::*:role/emr-ec2-role
    - arn:aws:iam::*:role/emr-role
    - arn:aws:iam::*:role/redshift-cluster-role
  - Generate access keys for the service user. Use the access keys to configure AWS CLI on local machine with a profile named "service_wp".
- Terraform
  - Update default values in file variables.tf: region, profile, bucket, redshift_cluster_name, redshift_cluster_role
  - In "terraform" directory, execute `terraform apply` to create S3 bucket and redshift cluster.
- Airflow on Docker
  - The following instructions assume Docker is already installed on the local machine. 
  - In file docker-compose.yaml, update environment variables under x-airflow-common:
    - AWS_BUCKET: value should match variable "bucket" in variables.tf file
    - AWS_PROFILE: value should match variable "profile" in variables.tf file
  - Build docker image and run container by executing following commands in a terminal when in the "airflow" directory:
    - `docker compose build .`
    - `docker compose up`
  - Set up Redshift connector in Airflow UI at localhost:8080. Use the following values:
    - Connection Id: redshift-ui-1
    - Connection Type: "Amazon Redshift"
    - Host: [Enter host found in Redshift service page on AWS console]
    - Schema: dev
    - Login: [Enter "master_username" value from main.tf file]
    - Password: [Enter "master_password" value from main.tf file]
    - Port: 5439
- Dimension Tables
  - Manually process state and station dimension tables and upload to S3 bucket under "dimension" folder. 
  - Notebook `04_process_dimension_tables.ipynb` can be followed for a rough outline of the processing steps. 

## How to Run
- After completing setup described above, go to the Airflow UI at localhost:8080. 
- Execute the four DAGS in order:
  - **01_ingest_data_dag**: Extracts raw precipitation data, converts to parquet, and pushes to S3 bucket under folder "raw". 
  - **02_process_with_spark_dag**: Spins up EMR cluster and executes pyspark script to process raw precipitation data. 
  - **03_s3_to_redshift_dim_dag**: Creates dimension tables for state and station in Redshift cluster.
  - **04_s3_to_redshift_fact_dag**: Creates fact table for precipitation as well as aggregated staging table in Redshift cluster.
- The final staging table can be found in Redshift named `stg_prep_by_year_state`. The table can be used in a data mart for analytics reporting. 