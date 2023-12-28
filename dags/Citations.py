import requests
import json
import os
import pandas as pd
#from Get_Samples_For_Citations import extract_values
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def process_json_files():
    input_folder = '/path/to/airflow/Raw_Data'
    output_folder = '/path/to/airflow/Enriched_Data'

def extract_values(cat, titl):
    directory = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'raw_data')
    results = []
    for filename in os.listdir(directory):
        if filename.endswith('.json'):
            if filename == 'chunk_1.json':
                with open(os.path.join(directory, filename), 'r') as f:
                    data = json.load(f)
                    df = pd.json_normalize(data)
                    if cat in df.columns and titl in df.columns:
                        categories = df[cat].unique()
                        for category in categories:
                            if category not in results:
                                results.append(df[df[cat] == category][titl].iloc[0])
                            if len(results) >= 20:
                                return results
    return results


# List of titles for the articles you're interested in
titles = extract_values('categories', 'title')
#print(titles)

# Base URL for the Crossref API
base_url = "https://api.crossref.org/works/"

# Empty dictionary to store the results
results = {}

# Loop through the titles
for title in titles:
    # Send a GET request to the Crossref API to search for the DOI
    doi_response = requests.get(base_url, params={"query.title": title})
    
    # Parse the response as JSON
    doi_data = doi_response.json()
    
    # Extract the DOI of the first result
    doi = doi_data['message']['items'][0]['DOI']
    
    # Send another GET request to the Crossref API to get the 'is-referenced-by-count'
    count_response = requests.get(base_url + doi)
    
    # Parse the response as JSON
    count_data = count_response.json()
    
    # Extract the 'is-referenced-by-count' and store it in the results dictionary
    results[title] = count_data['message']['is-referenced-by-count']

# Create directory if it doesn't exist
if not os.path.exists('Enriched_Data'):
    os.makedirs('Enriched_Data')

# Write the results to a JSON file in the new directory
with open('Enriched_Data/Nbr_Of_Citations.json', 'w') as f:
    json.dump(results, f)





# Define DAG
dag = DAG('enrich', description='Enrich JSON files',
          schedule_interval='0 12 * * *',
          start_date=datetime(2023, 12, 20), catchup=False)

# Define PythonOperator
process_json_operator = PythonOperator(task_id='enrich_json_files', python_callable=process_json_files, dag=dag)
