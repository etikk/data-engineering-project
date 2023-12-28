import pandas as pd
import numpy as np
import os
import json
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def process_json_files():
    input_folder = '/path/to/airflow/Enriched_Data'
    output_folder = '/path/to/airflow/Enriched_Data'


# Get the current working directory
cwd = os.getcwd()

# Get the parent directory
parent_dir = os.path.dirname(cwd)

# Specify the json directory path
json_dir_path = os.path.join(parent_dir, 'raw_data')

# Specify the csv directory path
csv_dir_path = os.path.join(parent_dir, 'helpers','_Cat_Csv.csv')


# Get a list of all files in the directory
json_files = os.listdir(json_dir_path)



# Loop through each JSON file
for i,json_file in enumerate(json_files):
    # Construct the full path to the JSON file
    json_file_path = os.path.join(json_dir_path, json_file)

    # Read the JSON file into a DataFrame
    json_df = pd.read_json(json_file_path)

#json_df = os.path.join(json_dir_path,'chunk_1.json')
#print(json_df['categories'])

#json_df = pd.read_json(json_df)

#print(json_df.count())
    json_df['categories'] = json_df['categories'].str.split(' ')

    json_df = json_df.explode('categories')

    csv_df = pd.read_csv(csv_dir_path, delimiter=";")
    merged_df = pd.merge(json_df, csv_df,how='left', left_on='categories', right_on='Category_Id')


#merged_df = merged_df[merged_df["title"] == "|V_{us}| from Lattice QCD"]

    grouped_df = merged_df.groupby(["title"])

#grouped_df_bc = grouped_df['Broader_Category'].apply(lambda x: list(set(np.array(x)))).reset_index()
    grouped_df = merged_df.groupby(["title"]).agg({'Broader_Category': lambda x: list(set(x)), 'Description': lambda x: list(set(x)),'Category_Id': lambda x: list(set(x))}).reset_index()

#grouped_df.rename(columns={'Broader_Category': 'Broader_Categories'}, inplace=True)
    grouped_df.rename(columns={'Broader_Category': 'Disciplines', 'Description': 'Category_Description','Category_Id' : 'Category_List' }, inplace=True)

    results_df = pd.merge(merged_df, grouped_df,how='inner', left_on='title', right_on='title')

    results_df = results_df.drop(columns = ['Broader_Category', 'Description','Category_Id','categories'])

    results_df = results_df.drop_duplicates(subset='title')

    output_file = f'new_partition_{i}.json'
    results_df.to_json(output_file, orient='records', lines=True)

#with open('katse.json', 'w') as f:
#    json.dump(results_df, f)
#print(grouped_df[grouped_df['Broader_Categories'].apply(lambda x: len(x) > 1)])


# Define DAG
dag = DAG('category_enrichment', description='Enrich JSON files with Category information',
          schedule_interval='0 12 * * *',
          start_date=datetime(2023, 12, 20), catchup=False)

# Define PythonOperator
process_json_operator = PythonOperator(task_id='category_json_files', python_callable=process_json_files, dag=dag)

