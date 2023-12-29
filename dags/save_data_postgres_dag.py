import os
import json
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

# Function to create the necessary tables according to the star schema
def create_tables():
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

def insert_data(**kwargs):
    # Retrieve the filename from the previous DAG via XCom
    ti = kwargs['ti']
    json_file_path = ti.xcom_pull(task_ids='previous_dag_task_id', key='filename_key')

    # Open the JSON file and load the data
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Set up database connection
    dbname = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = "db"
    port = "5432"

    # Connect to the database
    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    # Define the expected keys
    article_keys = ['title', 'abstract', 'update_date']
    author_keys = ['full_name', 'is_submitter']
    publication_keys = ['article_id', 'author_id', 'reference_count']
    subcategory_keys = ['subcategory', 'main_category']
    reference_keys = ['journal_ref', 'reference_type']
    version_keys = ['creation_date', 'version_number']

    # Insert data into each table
    for item in data:
        try:
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

                # Prepare data for 'publications' table
                publication_data = {key: item.get(key) for key in publication_keys}
                publication_data['article_id'] = article_id
                publication_data['author_id'] = author_id
                # Insert data into the 'publications' table
                cur.execute("""
                    INSERT INTO publications (article_id, author_id, reference_count) VALUES (%s, %s, %s);
                """, (publication_data['article_id'], publication_data['author_id'], publication_data['reference_count']))

            # Insert data into the 'subcategory' table and get the generated 'subcategory_id'
            for category in item.get('Category_List', []):
                cur.execute("""
                    INSERT INTO subcategory (subcategory, main_category) VALUES (%s, %s) RETURNING subcategory_id;
                """, (category, item.get('Disciplines')))
                subcategory_id = cur.fetchone()[0]

            # Prepare data for 'reference' table
            reference_data = {key: item.get(key) for key in reference_keys}
            # Insert data into the 'reference' table
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
        except Exception as e:
            print(f"An error occurred: {e}")
            # Handle the error as needed, e.g., log to a file, send a notification, etc.
            # Optionally, you could choose to skip the current record and continue with the next:
            continue

    # Commit changes and close the connection
    conn.commit()
    cur.close()
    conn.close()

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
    'save_data_to_postgres',
    default_args=default_args,
    description='A simple DAG to save data from a JSON file into a PostgreSQL database according to a star schema',
    schedule_interval=timedelta(days=1),
)

# Define the PythonOperators for creating tables and inserting data
create_tables_task = PythonOperator(
    task_id='create_tables',
    python_callable=create_tables,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id='insert_data_to_postgres',
    python_callable=insert_data,
    provide_context=True,
    dag=dag,
)

# Set the task sequence
create_tables_task >> insert_data_task
