import os
import json
import psycopg2
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def load_data():
    dbname = os.getenv("POSTGRES_DB")
    user = os.getenv("POSTGRES_USER")
    password = os.getenv("POSTGRES_PASSWORD")
    host = "db"
    port = "5432"

    conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    cur = conn.cursor()

    # SQL statement to create the table if it doesn't exist
    # Adjust column data types according to your data
    create_table_query = """
    CREATE TABLE IF NOT EXISTS test_table (
        id TEXT,
        submitter TEXT,
        authors TEXT,
        title TEXT,
        comments TEXT,
        journal_ref TEXT,
        doi TEXT,
        report_no TEXT,
        categories TEXT,
        license TEXT,
        abstract TEXT,
        update_date TEXT
    )
    """
    cur.execute(create_table_query)

    with open('/app/raw_data/chunk_1.json') as f:
        data = json.load(f)

    data = data[:10]

    keys = ['id', 'submitter', 'authors', 'title', 'comments', 'journal_ref', 'doi', 'report_no', 'categories', 'license', 'abstract', 'update_date']
    columns = ', '.join(keys)
    placeholders = ', '.join(['%s'] * len(keys))
    query = "INSERT INTO test_table (%s) VALUES (%s)" % (columns, placeholders)

    for item in data:
        values = tuple(item.get(key) for key in keys)
        cur.execute(query, values)

    conn.commit()
    cur.close()
    conn.close()

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
    'load_data_dag',
    default_args=default_args,
    description='A simple DAG to load data from a JSON file into a PostgreSQL database',
    schedule_interval=timedelta(days=1),
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    dag=dag,
)
