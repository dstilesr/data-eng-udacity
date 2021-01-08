#!/usr/bin/env bash

# Stop scheduler
cat $(pwd)/airflow/airflow-scheduler.pid | xargs kill

# Stop server
cat $(pwd)/airflow/airflow-webserver.pid | xargs kill
