import json
import os
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Define the path to the temp data
temp_data_path = '/app/temp_data/chunk_citations.json'

def load_data_from_json(**kwargs):
    json_file_path = '/app/raw_data/chunk_1.json'
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Optionally limit the dataset size for testing
    data = data[:10]

    # Save the raw data to a temp file
    with open(temp_data_path, 'w') as temp_file:
        json.dump(data, temp_file)

    # Push the temp file location to XCom
    kwargs['ti'].xcom_push(key='temp_data_path', value=temp_data_path)

def augment_data_with_citations(**kwargs):
    # Pull the temp file location from XCom
    temp_data_path = kwargs['ti'].xcom_pull(key='temp_data_path', task_ids='load_data_from_json')

    # Load data from the temp file
    with open(temp_data_path, 'r') as file:
        data = json.load(file)

    base_url = "https://api.crossref.org/works/"

    for item in data:
        title = item.get('title', '')
        if title:
            doi_response = requests.get(base_url, params={"query.title": title})
            doi_data = doi_response.json()
            doi = doi_data['message']['items'][0]['DOI']

            count_response = requests.get(base_url + doi)
            count_data = count_response.json()
            citation_count = count_data['message']['is-referenced-by-count']

            item['citation_count'] = citation_count

    # Save the augmented data back to the temp file
    with open(temp_data_path, 'w') as temp_file:
        json.dump(data, temp_file)

    # Pass the augmented data file location to the next task via XCom
    kwargs['ti'].xcom_push(key='augmented_data_path', value=temp_data_path)

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

dag = DAG(
    'data_processing_pipeline',
    default_args=default_args,
    description='Load and augment data from a JSON file',
    schedule_interval=timedelta(days=1),
    catchup=False
)

load_data_operator = PythonOperator(
    task_id='load_data_from_json',
    python_callable=load_data_from_json,
    provide_context=True,
    dag=dag,
)

augment_data_operator = PythonOperator(
    task_id='augment_data_with_citations',
    python_callable=augment_data_with_citations,
    provide_context=True,
    dag=dag,
)

load_data_operator >> augment_data_operator

# Further tasks can be added here that use the augmented data
