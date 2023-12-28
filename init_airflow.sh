#!/bin/bash
# This script initializes Airflow DB and creates a user
# It should be run after the first time you run docker-compose up

# Load environment variables from .env file
export $(cat .env | xargs)

# Initialize Airflow DB
docker-compose run airflow-webserver airflow db init

# Create Airflow user
docker-compose run airflow-webserver airflow users create \
  --username $AIRFLOW_USERNAME \
  --password $AIRFLOW_PASSWORD \
  --firstname $AIRFLOW_FIRSTNAME \
  --lastname $AIRFLOW_LASTNAME \
  --role $AIRFLOW_ROLE \
  --email $AIRFLOW_EMAIL
