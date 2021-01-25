# Capstone Project

## Contents
* [About](#about)
  * [Purpose](#purpose)
  * [Data Sources](#data-sources)
* [Data Processing](#data-processing)
  * [Output Data Structure](#output-data-structure)
  * [Pipeline Structure](#pipeline-structure)
* [Future Challenges](#future-challenges)
  * [Scaling Data Volume](#scaling-data-volume)
  * [Running Periodically](#running-periodically)
  * [Access For Multiple Users](#access-for-multiple-users)

## About

### Purpose
The purpose of the project is to structure both Earth Surface Temperature and Storm Events
data ([Sources Below](#data-sources)) in order to study possible these phenomena either on 
their own or in relationship with the temperature data. The idea is to use
[Apache Spark](https://spark.apache.org/) to create a pipeline to read the data from the
source csvs (which should be stored on S3), clean and organize it, and  then store the 
resulting data as parquet files on S3.

Spark was chosen for this job because of its speed, versatility, and horizontal scalability.
S3 was chosen for storing the data because it is less expensive for storing data at rest and
requires less administration overhead than storing the data on HDFS or in a database (SQL or
NoSQL). S3 also has good scalability and availability properties that make it a good choice
for data storage.

### Data Sources

- [Earth Surface Temperatures (Kaggle)](https://www.kaggle.com/berkeleyearth/climate-change-earth-surface-temperature-data)
- [NOAA Storm Events Data](https://www.ncdc.noaa.gov/stormevents/ftp.jsp)

## Data Processing

### Output Data Structure

### Pipeline Structure


## Future Challenges

### Scaling Data Volume
When it comes to data scaling (by 100x, for example), the current solution should work well and 
would in principle require no more than an outward scaling of the Spark Cluster to handle the 
additional processing workload. When it comes to storage, S3 remains a good option as it is highly 
scalable.

### Running Periodically
If the pipeline had to run periodically, for instance if it had to run daily at 7 a.m., then it would
be ideal to set it up as an [Airflow](https://airflow.apache.org/) DAG, as this would allow the 
scheduling and execution of the job as well as give traceability and insights into the execution
itself. Here the work would still be performed by a Spark cluster and not by the Airflow workers 
themselves, since these have limited capabilities on their own. This could be achieved via Airflow
Spark or Spark Submit operators. One could even set the DAG to spin up an AWS 
[EMR](https://aws.amazon.com/emr/) cluster for the job and then shut it down once the task is completed!

### Access for Multiple Users
