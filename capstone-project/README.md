# Capstone Project

## Contents
* [About](#about)
  * [Purpose](#purpose)
  * [Data Sources](#data-sources)
* [Data Processing](#data-processing)
  * [Output Data Structure](#output-data-structure)
  * [Pipeline Structure](#pipeline-structure)
* [Running the Pipeline](#running-the-pipeline)    
* [Future Challenges](#future-challenges)
  * [Scaling Data Volume](#scaling-data-volume)
  * [Running Periodically](#running-periodically)
  * [Access For Multiple Users](#access-for-multiple-users)

## About

### Purpose
The purpose of the project is to structure both Earth Surface Temperature and Storm Events
data ([Sources Below](#data-sources)) in order to study these phenomena either on 
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
The data is to be organized as follows: since there are two main objects to study, namely storms
and temperatures, there will be two facts tables in the output: `storms` and `temperatures`. Additionally,
there will be two dimensions along which to analyze these facts: `location` and `time`.

The `storms` table will contain the following facts about the storm events:
- `start_date`: Date in which the storm event started.
- `event_id`: ID of the storm event given by the NWS.
- `episode_id`: ID of the episode given by the NWS. One episode can contain several events.
- `location_id`: ID of the location (state) where the event occurred. This can be used to join to the
  `locations` table.
- `event_type`: Type of event (text).
- `magnitude_type`: Type of magnitude measurement recorded (if magnitude is provided).
- `magnitude`: Magnitude measurement.
- `damage_property`: Estimated damage to property caused (in US dollars).
- `damage_crops`: Estimated damage to property caused (in US dollars).
- `deaths_direct`: Deaths directly caused by the event.
- `injuries_direct`: Injuries directly caused by the event.

The `temperatures` table, on the other hand, will contain the following:
- `date_year_month`: Year and month in which the measurement was taken. This can be used to join to the
  `dates` table.
- `average_temperature`: Average temperature for the given period.
- `average_temperature_uncertainty`: Uncertainty for the temperature measurement.
- `location_id`: ID of the location where the measurement was taken. his can be used to join to the
  `locations` table.

Here the date will consist only of the month and year, since the dataset only contains average
temperatures on a monthly basis. As for the dimension tables, the `location` table contains the 
following fields:
- `location_id`: Unique ID of the location (hash of the state and country).
- `state`: State (within the country).
- `country`: Country.

The dimension is determined by state and country because that is the greates common resolution
common to both datasets. The `time` table has the following fields:
- `date`: Date.
- `year`: Year of the date.
- `month`: Month.
- `year_month`: Year and month of the date in `yyyy-MM` format.

### Pipeline Structure
The pipeline is implemented in pyspark and has the following basic steps:
- Read the raw temperature and storm data from S3.
- Create the facts and dimensions tables.
- Perform some quality checks.
- Write results to S3.

The quality checks performed are the following:
- Verify that the location ids were created correctly by joining the storms and locations tables.
- Verify that all the dates are present by joining the storms and dates tables.

## Running the Pipeline
To run the pipeline you must first upload the raw files to the S3 bucket you will be using. This
can be done by first downloading the `GlobalTemperaturesByState.csv` from the Kaggle link and running
the `download_noaa_files.py` script to get the storm data files, and then uploading the resulting files
to the bucket. Next, fill a `.env` file with the following variables:
```dotenv
BUCKET_NAME=
STORM_DATA_PATH=
OUTPUT_PATH=
TEMP_DATA_FILE=
```
`STORM_DATA_PATH` denotes the prefix in the bucket that contains the noaa files, `TEMP_DATA_FILE` is the path
to `GlobalTemperaturesByState.csv` in the bucket, and `OUTPUT_PATH` is the prefix where the processed files
will be stored in the bucket. If you want to run the job on a cluster, run the `prepare_submit_command.py` script
to prepare the spark-submit command to include the necessary environment variables. Then you can run the pipeline
by using the `spark_process.py` script.

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
When it comes to access for many users, S3 can handle reads well, but perhaps the Spark cluster will have
to be scaled. Another option would be to precompute the most common aggregates to be performed and store
the results in a database with good support for concurrence such as Cassandra.

[Back to top.](#capstone-project)
