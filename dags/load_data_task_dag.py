import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os

def load_data_from_json(**kwargs):
    # Define the path to the JSON file
    json_file_path = '/app/raw_data/chunk_1.json'

    # Load data from the JSON file
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # For testing, we can limit the dataset size
    # data = data[:1000]

    # Pass the data to the next task via XCom
    kwargs['ti'].xcom_push(key='raw_data', value=data)

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': [os.getenv("AIRFLOW_EMAIL")],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'load_data_task',
    default_args=default_args,
    description='Load data from a JSON file and pass to the next task',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Define the PythonOperator for loading data
load_data_operator = PythonOperator(
    task_id='load_data_from_json',
    python_callable=load_data_from_json,
    provide_context=True,
    dag=dag,
)

# Here, you can set up further tasks and define their dependencies
set_downstream >> data_augmentation_dag