import os
import json
from neo4j import GraphDatabase
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

import json

def serialize_non_primitive(item):
    for key, value in item.items():
        if isinstance(value, dict) or (isinstance(value, list) and any(isinstance(i, dict) for i in value)):
            item[key] = json.dumps(value)
        elif isinstance(value, list):
            item[key] = [str(i) for i in value]  # Convert list items to strings if they are not primitive
    return item

def load_data_to_neo4j():
    uri = "bolt://neo4j:7687"  # Replace with your Neo4j instance URI
    user = "neo4j"  # Default username for Neo4j
    password = os.getenv("NEO4J_PASSWORD")

    driver = GraphDatabase.driver(uri, auth=(user, password))

    with open('/app/raw_data/chunk_1.json') as f:
        data = json.load(f)

    data = data[:10]

    with driver.session() as session:
        for item in data:
            # Handle non-primitive types
            item = serialize_non_primitive(item)
            # Replace hyphens with underscores in keys
            item_with_underscores = {key.replace('-', '_'): value for key, value in item.items()}
            # Dynamically construct properties part of the query
            properties = ', '.join([f"{key}: ${key}" for key in item_with_underscores.keys()])
            query = f"CREATE (a:Article {{ {properties} }})"
            session.run(query, **item_with_underscores)

    driver.close()

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
    'load_data_to_neo4j_dag',
    default_args=default_args,
    description='A simple DAG to load data from a JSON file into a Neo4j database',
    schedule_interval=timedelta(days=1),
)

load_data_to_neo4j_task = PythonOperator(
    task_id='load_data_to_neo4j',
    python_callable=load_data_to_neo4j,
    dag=dag,
)
