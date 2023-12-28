import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def augment_data_with_citations(**kwargs):
    # Fetch data passed from the previous task via XCom
    data = kwargs['ti'].xcom_pull(task_ids='load_data_task')

    # Base URL for the Crossref API
    base_url = "https://api.crossref.org/works/"

    # Loop through the data and augment with citation count
    for item in data:
        title = item.get('title', '')
        if title:
            # Fetch DOI for the title
            doi_response = requests.get(base_url, params={"query.title": title})
            doi_data = doi_response.json()
            doi = doi_data['message']['items'][0]['DOI']

            # Fetch citation count for the DOI
            count_response = requests.get(base_url + doi)
            count_data = count_response.json()
            citation_count = count_data['message']['is-referenced-by-count']

            # Augment the data item with citation count
            item['citation_count'] = citation_count

    # Pass augmented data to the next task via XCom
    kwargs['ti'].xcom_push(key='augmented_data', value=data)

# Define DAG
dag = DAG(
    'data_augmentation_dag',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2023, 12, 20),
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Augment data with citation counts',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Augment data task
augment_data_task = PythonOperator(
    task_id='augment_data_task',
    python_callable=augment_data_with_citations,
    dag=dag,
)

# Here you can define further tasks to save data to databases
