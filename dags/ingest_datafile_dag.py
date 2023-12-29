from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import json
import pandas as pd
import requests
import logging

def load_and_start_pipelines(**kwargs):
    raw_data_path = '/app/raw_data'
    file_names = [f for f in os.listdir(raw_data_path) if f.startswith('chunk') and f.endswith('.json')]
    for file_name in file_names:
        file_path = os.path.join(raw_data_path, file_name)
        kwargs['ti'].xcom_push(key=f"{file_name}_path", value=file_path) # Not strictly necessary, but useful for debugging

def filter_publications(**kwargs):
    file_path = os.path.join('/app/raw_data', kwargs['file_name'])

    with open(file_path, 'r') as file:
        data = json.load(file)

    # Use only part of the data for testing
    data = data[:100]

    filtered_data = [item for item in data if len(item.get('title', '').split()) > 1 and item.get('authors')]
    for item in filtered_data:
        item.pop('abstract', None)

    # temp_filtered_data_path_key = f"{kwargs['file_name']}_filtered_data_path"
    output_json_path = file_path.replace('raw_data', 'temp_data').replace('.json', '_temp.json')
    with open(output_json_path, 'w') as outfile:
        json.dump(filtered_data, outfile)

def augment_with_categories(**kwargs):
    csv_dir_path = '/app/helpers/_Cat_Csv.csv'
    crossref_path = '/app/helpers/Citations_Pubtype.json'
    
    input_json_path = os.path.join('/app/temp_data', kwargs['file_name'].replace('.json', '_temp.json'))

    with open(input_json_path, 'r') as file:
        data = json.load(file)

    df = pd.DataFrame(data)

    # Ensure 'categories' is a string before exploding
    df['categories'] = df['categories'].astype(str)
    df['categories'] = df['categories'].str.split(' ')
    df = df.explode('categories')

    csv_df = pd.read_csv(csv_dir_path, delimiter=";")
    crossref_df = pd.read_json(crossref_path, lines=True)

    # Before merging, ensure there are no list type columns
    # For example, if 'title' is a list, convert it to a string
    df['title'] = df['title'].astype(str)
    crossref_df['Title'] = crossref_df['Title'].astype(str)

    merged_df = pd.merge(df, csv_df, how='left', left_on='categories', right_on='Category_Id')
    grouped_df = merged_df.groupby("title").agg({
        'Broader_Category': lambda x: list(set(x)),
        'Description': lambda x: list(set(x)),
        'Category_Id': lambda x: list(set(x))
    }).reset_index()

    # Convert any lists in grouped_df to string
    for col in ['Broader_Category', 'Description', 'Category_Id']:
        grouped_df[col] = grouped_df[col].apply(lambda x: ', '.join(map(str, x)))

    grouped_df.rename(columns={
        'Broader_Category': 'Disciplines',
        'Description': 'Category_Description',
        'Category_Id': 'Category_List'
    }, inplace=True)

    results_df = pd.merge(merged_df, grouped_df, how='inner', on='title')
    results_df = results_df.drop(columns=['Broader_Category', 'Description', 'Category_Id', 'categories'])
    results_df = results_df.drop_duplicates(subset='title')

    ref_merge = pd.merge(results_df, crossref_df, how='left', left_on='title', right_on='Title')
    ref_merge = ref_merge.drop(columns=['Title'])

    output_json_path = input_json_path
    ref_merge.to_json(output_json_path, orient='records', lines=True)

def augment_with_citations(**kwargs):
    input_json_path = os.path.join('/app/temp_data', kwargs['file_name'].replace('.json', '_temp.json'))
    output_json_path = input_json_path

    try:
        with open(input_json_path, 'r') as file:
            data = json.load(file)

        # Base URL for the Crossref API
        base_url = "https://api.crossref.org/works/"

        # Augment data with citation count
        for item in data:
            title = item.get('title', '')
            if title:
                response = requests.get(base_url, params={"query.title": title})
                if response.status_code == 200:
                    doi_data = response.json()
                    doi = doi_data['message']['items'][0]['DOI']
                    count_response = requests.get(base_url + doi)
                    if count_response.status_code == 200:
                        count_data = count_response.json()
                        citation_count = count_data['message']['is-referenced-by-count']
                        item['citation_count'] = citation_count

        with open(output_json_path, 'w') as outfile:
            json.dump(data, outfile)

    except Exception as e:
        print(f"Error augmenting data for {input_json_path}: {str(e)}")
        os.rename(input_json_path, output_json_path)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email': ['example@email.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ingest_datafile_dag',
    default_args=default_args,
    description='Ingest and process multiple JSON files',
    schedule_interval=timedelta(days=1),
    catchup=False
)

load_and_start_pipelines_task = PythonOperator(
    task_id='load_and_start_pipelines',
    python_callable=load_and_start_pipelines,
    provide_context=True,
    dag=dag,
)

# Define tasks for each file
for file_name in os.listdir('/app/raw_data'):
    if file_name.startswith('chunk') and file_name.endswith('.json'):
        filter_publications_task = PythonOperator(
            task_id=f'filter_publications_{file_name}',
            python_callable=filter_publications,
            provide_context=True,
            op_kwargs={'file_name': file_name},
            dag=dag,
        )
        augment_with_categories_task = PythonOperator(
            task_id=f'augment_with_categories_{file_name}',
            python_callable=augment_with_categories,
            provide_context=True,
            op_kwargs={'file_name': file_name},
            dag=dag,
        )
        augment_with_citations_task = PythonOperator(
            task_id=f'augment_with_citations_{file_name}',
            python_callable=augment_with_citations,
            provide_context=True,
            op_kwargs={'file_name': file_name},
            dag=dag,
        )

        # Set up the task dependencies
        load_and_start_pipelines_task >> filter_publications_task >> augment_with_categories_task >> augment_with_citations_task

