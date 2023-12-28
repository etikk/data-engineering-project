#!/bin/bash
# This script initializes Airflow DB and creates a user
# It should be run after the first time you run docker-compose up

# Load environment variables from .env file
export $(cat .env | xargs)

echo "AIRFLOW_USERNAME: $AIRFLOW_USERNAME"
echo "AIRFLOW_PASSWORD: $AIRFLOW_PASSWORD"
echo "AIRFLOW_FIRSTNAME: $AIRFLOW_FIRSTNAME"
echo "AIRFLOW_LASTNAME: $AIRFLOW_LASTNAME"
echo "AIRFLOW_ROLE: $AIRFLOW_ROLE"
echo "AIRFLOW_EMAIL: $AIRFLOW_EMAIL"
echo "POSTGRES_DB: $POSTGRES_USER"
echo "POSTGRES_PASSWORD: $POSTGRES_PASSWORD"
echo "POSTGRES_USER : $POSTGRES_USER"
echo "POSTGRES_PASSWORD: $POSTGRES_PASSWORD"

# Initialize Airflow DB
docker-compose exec airflow-webserver airflow db init  

# Create Airflow user
docker-compose exec airflow-webserver airflow users create \
  --username $AIRFLOW_USERNAME \
  --password $AIRFLOW_PASSWORD \
  --firstname $AIRFLOW_FIRSTNAME \
  --lastname $AIRFLOW_LASTNAME \
  --role $AIRFLOW_ROLE \
  --email $AIRFLOW_EMAIL
