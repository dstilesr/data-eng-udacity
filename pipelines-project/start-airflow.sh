#!/bin/bash

################################
# Env. variables
################################
# Airflow home
export AIRFLOW_HOME=$(pwd)/airflow

# Use LocalExecutor to allow parallelism
export AIRFLOW__CORE__EXECUTOR=LocalExecutor

# Load vars from .env
export $(xargs < .env)

# Connection string for locally run mysql
export AIRFLOW__CORE__SQL_ALCHEMY_CONN=mysql+mysqlconnector://$MYSQL_USER:$MYSQL_PASSWORD@$MYSQL_HOST:3306/$MYSQL_DATABASE


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
  && echo "INITIALIZED" > $(pwd)/dbcheck.txt \
  && airflow users create --username admin \
                          --role Admin \
                          --firstname dstilesr \
                          --lastname admin \
                          --email david@factored.ai

fi

# Start Airflow Scheduler
airflow scheduler --daemon

# Start webserver
airflow webserver --daemon

