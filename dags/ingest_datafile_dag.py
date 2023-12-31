from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import os
import json
import pandas as pd
import requests
# import logging
import psycopg2
from neo4j import GraphDatabase
from datetime import datetime, timedelta

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
    # data = data[:1000]

    filtered_data = [item for item in data if len(item.get('title', '')) > 1 and item.get('authors')]
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
        print(f"Error augmenting data for {input_json_path}: {e}")
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
            doi TEXT NULL DEFAULT NULL,
            update_date DATE
        );
    """)

    # add index to article title
    cur.execute("""
        CREATE INDEX IF NOT EXISTS article_title_idx ON article (title);
    """)


    # Create the author table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS author (
            author_id SERIAL PRIMARY KEY,
            full_name TEXT,
            is_submitter BOOLEAN
        );
    """)

    # add index to author full_name
    cur.execute("""
        CREATE INDEX IF NOT EXISTS author_full_name_idx ON author (full_name);
    """)


    # Create the reference table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS reference (
            reference_id SERIAL PRIMARY KEY,
            article_id INTEGER REFERENCES article(article_id),
            journal_ref TEXT,
            reference_type TEXT
        );
    """)

    # add index to reference journal_ref
    cur.execute("""
        CREATE INDEX IF NOT EXISTS reference_journal_ref_idx ON reference (journal_ref);
        CREATE INDEX IF NOT EXISTS reference_article_id_idx ON reference (article_id);
    """)


    # Create the subcategory table
    cur.execute("""
        CREATE TABLE IF NOT EXISTS subcategory (
            subcategory_id SERIAL PRIMARY KEY,
            subcategory TEXT,
            main_category TEXT
        );
    """)

    # add index to subcategory subcategory
    cur.execute("""
        CREATE INDEX IF NOT EXISTS subcategory_subcategory_idx ON subcategory (subcategory);
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

    # add index to publications (article_id, author_id)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS publications_article_id_author_id_idx ON publications (article_id, author_id);
    """)

    # Create the version table
    cur.execute("""
    CREATE TABLE IF NOT EXISTS version (
        version_id SERIAL PRIMARY KEY,
        author_id INTEGER REFERENCES author(author_id),
        creation_date DATE,
        version_number TEXT
    )
    """)

    # add index to version (creation_date, version_number)
    cur.execute("""
        CREATE INDEX IF NOT EXISTS version_creation_date_version_number_idx ON version (creation_date, version_number);
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

                    # Check and insert for article
                    cur.execute("SELECT article_id FROM article WHERE title = %s;", (item.get('title'),))
                    article_id = cur.fetchone()
                    if not article_id:
                        cur.execute("INSERT INTO article (title, abstract, update_date) VALUES (%s, %s, %s) RETURNING article_id;",
                                    (item.get('title'), item.get('abstract'), item.get('update_date')))
                        article_id = cur.fetchone()

                    # Check and insert for each author
                    for author in item.get('authors_parsed', []):
                        cur.execute("SELECT author_id FROM author WHERE full_name = %s;", (" ".join(author),))
                        author_id = cur.fetchone()
                        if not author_id:
                            cur.execute("INSERT INTO author (full_name, is_submitter) VALUES (%s, %s) RETURNING author_id;",
                                        (" ".join(author), author[0] == item.get('submitter')))
                            author_id = cur.fetchone()

                        # Check and insert for publications
                        cur.execute("SELECT publication_id FROM publications WHERE article_id = %s AND author_id = %s;",
                                    (article_id[0], author_id[0]))
                        if not cur.fetchone():
                            cur.execute("INSERT INTO publications (article_id, author_id, reference_count) VALUES (%s, %s, %s);",
                                        (article_id[0], author_id[0], item.get('ReferencedByCount')))

                    # Check and insert for subcategory
                    for category in item.get('Category_List', []):
                        cur.execute("SELECT subcategory_id FROM subcategory WHERE subcategory = %s;", (category,))

                        if not cur.fetchone():
                            cur.execute("INSERT INTO subcategory (subcategory, main_category) VALUES (%s, %s) RETURNING subcategory_id;",
                                        (category, item.get('Disciplines')))
                            subcategory_id = cur.fetchone()[0]

                    # Check and insert for reference
                    cur.execute("SELECT reference_id FROM reference WHERE journal_ref = %s;", (item.get('journal-ref'),))
                    if cur.fetchone() is None:
                        cur.execute("INSERT INTO reference (journal_ref, reference_type) VALUES (%s, %s);",
                                    (item.get('journal-ref'), item.get('publication_type')))

                    # Check and insert for version
                    for version in item.get('versions', []):
                        cur.execute("SELECT version_id FROM version WHERE creation_date = %s AND version_number = %s;",
                                    (version.get('created'), version.get('version')))
                        if cur.fetchone() is None:
                            cur.execute("INSERT INTO version (creation_date, version_number) VALUES (%s, %s) RETURNING version_id;",
                                        (version.get('created'), version.get('version')))
                            version_id = cur.fetchone()[0]

                    # finish transaction
                    conn.commit()

                except json.JSONDecodeError as e:
                    print(f"Error parsing JSON line: {e}")
                    continue

    except Exception as e:
        # print stacktrace
        print(f"Error reading file or inserting into database: {e}")

    conn.commit()
    cur.close()
    conn.close()

def save_to_neo4j(**kwargs):
    uri = "bolt://neo4j:7687"  # Replace with your Neo4j instance URI
    user = "neo4j"  # Default username for Neo4j
    password = os.getenv("NEO4J_PASSWORD")

    driver = GraphDatabase.driver(uri, auth=(user, password))

    json_file_path = os.path.join('/app/temp_data', kwargs['file_name'].replace('.json', '_temp.json'))

    try:
        with open(json_file_path) as f:
            data = [json.loads(line) for line in f]
    except FileNotFoundError:
        print("The file was not found.")
        return

    data = data[:1000]  # For testing purposes

    with driver.session() as session:
        for item in data:
            item_with_underscores = {key.replace('-', '_'): value for key, value in item.items()}

            if 'authors_parsed' in item_with_underscores:
                session.run("""
                    UNWIND $authors_parsed AS author
                    MERGE (c:Author {FirstName: author[1], LastName: author[0], Name: author[0] + ' ' + author[1]})
                    MERGE (b:Title {Title: $title})
                    MERGE (c)-[:AUTHORED]->(b)
                """, title=item_with_underscores['title'], authors_parsed=item_with_underscores['authors_parsed'])

            if 'submitter' in item_with_underscores:
                session.run("""
                    MERGE (c:Author {LastName: $submitter, FirstName: $submitter, Name: $submitter})
                    MERGE (b:Title {Title: $title})
                    MERGE (c)-[:SUBMITTED]->(b)
                """, title=item_with_underscores['title'], submitter=item_with_underscores['submitter'])

            if 'Category_List' in item_with_underscores:
                session.run("""
                    MERGE (b:Title {Title: $title})
                    WITH b, $Category_List AS categories
                    UNWIND categories AS category
                    MERGE (d:Category_List {Category_List: category}) 
                    MERGE (b)-[:IS_CATEGORIZED_AS]->(d)
                """, title=item_with_underscores['title'], Category_List=item_with_underscores['Category_List'])

            if 'Disciplines' in item_with_underscores:
                session.run("""
                    MERGE (b:Title {Title: $title})
                    WITH b, $Disciplines AS disciplines
                    UNWIND disciplines AS discipline
                    MERGE (f:Discipline {Discipline: discipline})
                    MERGE (b)-[:IS_IN_DISCIPLINE]->(f)
                """, title=item_with_underscores['title'], Disciplines=item_with_underscores['Disciplines'])

            if 'journal_ref' in item_with_underscores:
                session.run("""
                    MERGE (b:Title {Title: $title})
                    WITH b, $journal_ref AS journal_ref, $PublicationType AS publication_type, $ReferencedByCount AS referenced_by_count
                    MERGE (e:Journal_Reference {Journal_Reference: coalesce(journal_ref, 'Unknown'), Publication_Type: coalesce(publication_type, 'Unknown')})
                    MERGE (b)-[r:Referenced_In_Journal]->(e)
                    ON CREATE SET r.weight = referenced_by_count
                """, title=item_with_underscores['title'],
                    journal_ref=item_with_underscores.get('journal_ref'),
                    PublicationType=item_with_underscores.get('PublicationType'),
                    ReferencedByCount=item_with_underscores.get('ReferencedByCount'))

    driver.close()


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
    catchup=False,
    concurrency=6,
)

load_and_start_pipelines_task = PythonOperator(
    task_id='load_and_start_pipelines',
    python_callable=load_and_start_pipelines,
    provide_context=True,
    dag=dag,
)

create_postgres_tables_task = PythonOperator(
    task_id=f'create_postgres_tables',
    python_callable=create_postgres_tables,
    provide_context=True,
    dag=dag,
)

metadata_setup_task = load_and_start_pipelines_task >> create_postgres_tables_task


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
        save_to_postgres_task = PythonOperator(
            task_id=f'save_to_postgres_{file_name}',
            python_callable=save_to_postgres,
            provide_context=True,
            op_kwargs={'file_name': file_name},
            dag=dag,
        )
        save_to_neo4j_task = PythonOperator(
            task_id=f'save_to_neo4j_{file_name}',
            python_callable=save_to_neo4j,
            provide_context=True,
            op_kwargs={'file_name': file_name},
            dag=dag,
        )

        # Set up the task dependencies
        preprocess_task = metadata_setup_task >> filter_publications_task >> augment_with_categories_task >> augment_with_citations_task
        preprocess_task >> save_to_postgres_task
        preprocess_task >> save_to_neo4j_task
