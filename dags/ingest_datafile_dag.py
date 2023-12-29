from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import json
import pandas as pd
import requests
# import logging
import psycopg2

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

def create_postgres_tables():
    # Set up database connection
    dbname = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = "db"  # Hostname for the database, change if necessary
    port = "5432"  # Port for the database, change if necessary

    # Connect to the database
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    # Create tables
    # Create the article table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS article (
            article_id SERIAL PRIMARY KEY,
            title TEXT,
            abstract TEXT,
            update_date DATE
        );
    """)

    # Create the author table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS author (
            author_id SERIAL PRIMARY KEY,
            full_name TEXT,
            is_submitter BOOLEAN
        );
    """)

    # Create the reference table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reference (
            reference_id SERIAL PRIMARY KEY,
            journal_ref TEXT,
            reference_type TEXT
        );
    """)

    # Create the subcategory table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS subcategory (
            subcategory_id SERIAL PRIMARY KEY,
            subcategory TEXT,
            main_category TEXT
        );
    """)

    # Create the publications table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS publications (
            publication_id SERIAL PRIMARY KEY,
            article_id INTEGER REFERENCES article(article_id),
            author_id INTEGER REFERENCES author(author_id),
            reference_count INTEGER
        );
    """)

    # Create the version table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS version (
        version_id SERIAL PRIMARY KEY,
        creation_date DATE,
        version_number SMALLINT
    )
    """)

    # Commit changes and close the connection
    conn.commit()
    cur.close()
    conn.close()

def save_to_postgres(**kwargs):
    json_file_path = os.path.join('/app/temp_data', kwargs['file_name'].replace('.json', '_temp.json'))

    # Set up database connection
    dbname = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = "db"
    port = "5432"

    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    try:
        with open(json_file_path, 'r') as file:
            for line in file:
                try:
                    item = json.loads(line)

                    # Define the expected keys
                    article_keys = ['title', 'abstract', 'update_date']
                    author_keys = ['full_name', 'is_submitter']
                    publication_keys = ['article_id', 'author_id', 'reference_count']
                    subcategory_keys = ['subcategory', 'main_category']
                    reference_keys = ['journal_ref', 'reference_type']
                    version_keys = ['creation_date', 'version_number']

                    # Insert data into each table
                    # Prepare data for 'article' table
                    article_data = {key: item.get(key) for key in article_keys}
                    # Insert data into 'article' table and get the generated 'article_id'
                    cur.execute("""
                        INSERT INTO article (title, abstract, update_date) VALUES (%s, %s, %s) RETURNING article_id;
                    """, (article_data['title'], article_data['abstract'], article_data['update_date']))
                    article_id = cur.fetchone()[0]

                    # Insert data into the 'author' table and get the generated 'author_id'
                    for author in item.get('authors_parsed', []):
                        author_data = {key: author.get(key) for key in author_keys}
                        full_name = " ".join(author)
                        is_submitter = (full_name == item.get('submitter'))
                        cur.execute("""
                            INSERT INTO author (full_name, is_submitter) VALUES (%s, %s) RETURNING author_id;
                        """, (full_name, is_submitter))
                        author_id = cur.fetchone()[0]

                        # Insert data into the 'publications' table
                        publication_data = {key: item.get(key) for key in publication_keys}
                        publication_data['article_id'] = article_id
                        publication_data['author_id'] = author_id
                        cur.execute("""
                            INSERT INTO publications (article_id, author_id, reference_count) VALUES (%s, %s, %s);
                        """, (publication_data['article_id'], publication_data['author_id'], publication_data['reference_count']))

                    # Insert data into the 'subcategory' table
                    for category in item.get('Category_List', []):
                        cur.execute("""
                            INSERT INTO subcategory (subcategory, main_category) VALUES (%s, %s) RETURNING subcategory_id;
                        """, (category, item.get('Disciplines')))
                        subcategory_id = cur.fetchone()[0]

                    # Insert data into the 'reference' table
                    reference_data = {key: item.get(key) for key in reference_keys}
                    cur.execute("""
                        INSERT INTO reference (journal_ref, reference_type) VALUES (%s, %s);
                    """, (reference_data['journal_ref'], reference_data['reference_type']))

                    # Insert into 'version' table
                    for version in item.get('versions', []):
                        version_data = {key: version.get(key) for key in version_keys}
                        cur.execute("""
                            INSERT INTO version (creation_date, version_number) VALUES (%s, %s) RETURNING version_id;
                        """, (version_data['creation_date'], version_data['version_number']))
                        version_id = cur.fetchone()[0]

                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON line: {e}")
                    continue

    except Exception as e:
        print(f"Error reading file or inserting into database: {e}")

    conn.commit()
    cur.close()
    conn.close()

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
        create_postgres_tables_task = PythonOperator(
            task_id=f'create_postgres_tables_{file_name}',
            python_callable=create_postgres_tables,
            provide_context=True,
            op_kwargs={'file_name': file_name},
            dag=dag,
        )
        save_to_postgres_task = PythonOperator(
            task_id=f'save_to_postgres_{file_name}',
            python_callable=save_to_postgres,
            provide_context=True,
            op_kwargs={'file_name': file_name},
            dag=dag,
        )

        # Set up the task dependencies
        load_and_start_pipelines_task >> filter_publications_task >> augment_with_categories_task >> augment_with_citations_task >> create_postgres_tables_task >> save_to_postgres_task

