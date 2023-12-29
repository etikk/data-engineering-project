import pandas as pd
import os
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def augment_with_categories(**kwargs):
    # Retrieve the input JSON file location from the previous task via XCom
    ti = kwargs['ti']
    input_json_path = ti.xcom_pull(task_ids='previous_task_id', key='augmented_data_path')

    # Define the paths for additional data
    csv_dir_path = '/path/to/airflow/helpers/_Cat_Csv.csv'
    crossref_path = '/path/to/airflow/helpers/Citations_Pubtype.json'

    # Read the input JSON data
    with open(input_json_path, 'r') as file:
        data = json.load(file)

    # Convert JSON to DataFrame
    df = pd.DataFrame(data)

    # Explode the categories and merge with additional data
    df['categories'] = df['categories'].str.split(' ')
    df = df.explode('categories')

    csv_df = pd.read_csv(csv_dir_path, delimiter=";")
    crossref_df = pd.read_json(crossref_path, lines=True)

    merged_df = pd.merge(df, csv_df, how='left', left_on='categories', right_on='Category_Id')
    grouped_df = merged_df.groupby("title").agg({
        'Broader_Category': lambda x: list(set(x)),
        'Description': lambda x: list(set(x)),
        'Category_Id': lambda x: list(set(x))
    }).reset_index()
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
    ref_merge[['first_name', 'last_name']] = ref_merge['submitter'].str.rsplit(' ', n=1, expand=True)
    ref_merge['submitter_parsed'] = ref_merge[['last_name', 'first_name']].values.tolist()
    ref_merge = ref_merge.drop(columns =['last_name','first_name'])

    # Define the path for the output JSON file
    output_json_path = os.path.join('/app/temp_data', f'chunk_categories.json')

    # Save the enriched data to the output JSON file
    ref_merge.to_json(output_json_path, orient='records', lines=True)

    # Push the output file location to XCom for the next task
    ti.xcom_push(key='enriched_data_path', value=output_json_path)

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
    'category_enrichment_dag',
    default_args=default_args,
    description='Enrich JSON files with Category information and pass to next task',
    schedule_interval=timedelta(days=1),
    catchup=False
)

augment_with_categories_task = PythonOperator(
    task_id='augment_with_categories',
    python_callable=augment_with_categories,
    provide_context=True,
    dag=dag,
)

# Assuming 'augment_data_with_citations_task' is the task ID of the previous task in the pipeline
# augment_data_with_citations_task >> enrich_with_categories_task
