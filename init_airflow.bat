@echo off
REM This script initializes Airflow DB and creates a user
REM It should be run after the first time you run docker-compose up

REM Load environment variables from .env file
FOR /F "tokens=*" %%i IN ('type .env') DO set %%i

REM Initialize Airflow DB
docker-compose run airflow-webserver airflow db init

REM Create Airflow user
docker-compose run airflow-webserver airflow users create ^
  --username %AIRFLOW_USERNAME% ^
  --password %AIRFLOW_PASSWORD% ^
  --firstname %AIRFLOW_FIRSTNAME% ^
  --lastname %AIRFLOW_LASTNAME% ^
  --role %AIRFLOW_ROLE% ^
  --email %AIRFLOW_EMAIL%
