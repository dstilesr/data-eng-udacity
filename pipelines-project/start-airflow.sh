#!/bin/bash

################################
# Env. variables
################################
# Airflow home
export AIRFLOW_HOME=$(pwd)/airflow

# Use LocalExecutor to allow parallelism
export AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Connect to locally run mysql
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=mysql+mysqlconnector://airflow:afpw852@localhost:3306/airflowdb


################################
# Start Airflow Services
################################
# Initialize airflow metadata db
if [ -f $(pwd)/dbcheck.txt ]
then
  echo "DB ALREADY STARTED"
else
  # Wait for SQL to initialize
  python wait_mysql.py

  echo "STARTING DB"
  airflow db init \
  && echo "INITIALIZED" > $(pwd)/dbcheck.txt
fi

# Start Airflow Scheduler
airflow scheduler --daemon

# Start webserver
airflow webserver

