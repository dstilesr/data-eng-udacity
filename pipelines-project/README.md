# Sparkify Data Pipelines


## Contents

* [About](#about)
* [Database Structure](#database-structure)  
* [Running the Project](#running-the-project)

## About
This project consists of an [Apache Airflow](https://airflow.apache.org/) data pipeline
whose purpose is to load data from JSON files stored on AWS S3 to the sparkify data
warehouse hosted on AWS Redshift.

As an important note, **I made some modifications to the code in order to run it with Airflow
version `2.0`** as opposed to the version `1.10` used in the workspace. To ensure compatibility,
you may want to install the python packages from the `requirements.txt` file.

## Database Structure
The database structure follows a star schema similar to the one used in previous projects. This
consists of a `songplays` fact table and `users`, `time`, `songs`, and `artists` dimension tables,
designed to facilitate analysis of songplay data.

## Running the Project
In order to run the project locally, you must first set up a local MySQL database for airflow
to store metadata in, which you can do using the `docker-compose.yml` file. This requires you 
to first create a `.env` file with the following information:
```dotenv
MYSQL_ROOT_PASSWORD=
MYSQL_DATABASE=
MYSQL_USER=
MYSQL_PASSWORD=
MYSQL_HOST=localhost
```
Once you have started the database, you can start airflow by running the script:
```shell
bash start-airflow.sh
```
This should start the webserver on port 8080 as well as the scheduler. Note that you will be 
prompted for a password for the airflow user! Then you must configure a connection to your 
redshift cluster (you can set this up with the script from `dwh-project`), which you must call 
`redshift` via the airflow webUI, as well as an AWS connection with S3 read access which you 
must call `aws-conn`. *Remember to create the tables in the redshift database using
the `airflow/create_tables.sql` script before running the DAG!*

To stop the scheduler and webserver, run
```shell
bash stop-airflow.sh
```

[Back to top](#sparkify-data-pipelines)
