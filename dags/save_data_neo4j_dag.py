import os
import json
from neo4j import GraphDatabase
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def serialize_non_primitive(item):
    for key, value in item.items():
        if isinstance(value, dict) or (isinstance(value, list) and any(isinstance(i, dict) for i in value)):
            item[key] = json.dumps(value)
        elif isinstance(value, list):
            item[key] = [str(i) for i in value]  # Convert list items to strings if they are not primitive
    return item

def save_data_to_neo4j():
    uri = "bolt://neo4j:7687"  # Replace with your Neo4j instance URI
    user = "neo4j"  # Default username for Neo4j
    password = os.getenv("NEO4J_PASSWORD")

    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        with open('/app/raw_data/new_partition_0.json') as f:
            data = [json.loads(line) for line in f]
    except FileNotFoundError:
        print("The file was not found.")
        return

    # data = data[:1000]

    with driver.session() as session:
        for item in data:
            # Handle non-primitive types and hyphens
            #item = serialize_non_primitive(item)
            item_with_underscores = {key.replace('-', '_'): value for key, value in item.items()}
            if item_with_underscores.get('authors_parsed'):
                authors_parsed_query = """
                    WITH $authors_parsed AS authors
                    UNWIND authors AS author
                    WITH author WHERE author IS NOT NULL
                    WITH author WHERE author[0] IS NOT NULL
                    MERGE (c:Author {FirstName: author[1], LastName: author[0], Name: author[0] + ' ' + author[1]})
                    MERGE (b:Title {Title: $title})
                    MERGE (c)-[:AUTHORED]->(b)
                """

                # UNWIND author_parsed AS final_author_parse
                #MERGE (a:Author {firstName: final_author_parse[1], lastName: final_author_parse[0],Name:final_author_parse})

                session.run(authors_parsed_query, title=item_with_underscores['title'], authors_parsed=item_with_underscores['authors_parsed'])


            if item_with_underscores.get('submitter'):
                submitter_parsed_query = """
                    WITH $submitter_parsed AS sub_parsed
                    UNWIND range(0, size(sub_parsed) - 2, 2) AS i
                    WITH sub_parsed,i  WHERE sub_parsed IS NOT NULL
                    WITH sub_parsed,i  WHERE sub_parsed[i] IS NOT NULL
                    MERGE (c:Author {LastName: sub_parsed[i], FirstName: sub_parsed[i + 1], Name: sub_parsed[i] + ' ' + sub_parsed[i + 1]})
                    MERGE (b:Title {Title: $title})
                    MERGE (c)-[:SUBMITTED]->(b)
                """
                #MERGE (c:Author {firstName: submitter_parsed[1], lastName: submitter_parsed[0]})
                session.run(submitter_parsed_query,title=item_with_underscores['title'], submitter_parsed=item_with_underscores['submitter_parsed'])

            # MERGE for Category_List
            if item_with_underscores.get('Category_List'):
                category_list = [item for item in item_with_underscores['Category_List'] if item is not None]
                category_list_query = """
                    MERGE (b:Title {Title: $title})
                    WITH b, $Category_List AS categories
                    WITH b, categories WHERE categories IS NOT NULL
                    MERGE (d:Category_List {Category_List: categories}) 
                    ON CREATE SET d.Category_List = categories
                    MERGE (b)-[:IS_CATEGORIZED_AS]->(d)
                """
                session.run(category_list_query,title=item_with_underscores['title'], Category_List=category_list)

            # Cypher query for Disciplines
            if item_with_underscores.get('Disciplines'):
                discipline_list = [item for item in item_with_underscores['Disciplines'] if item is not None]
                disciplines_query = """
                    MERGE (b:Title {Title: $title})
                    WITH b, $disciplines AS disciplines
                    WITH b, disciplines WHERE disciplines IS NOT NULL
                    UNWIND disciplines AS discipline
                    MERGE (f:Discipline {Discipline: discipline})
                    ON CREATE SET f.Discipline = discipline
                    MERGE (b)-[:IS_IN_DISCIPLINE]->(f)
                """
                session.run(disciplines_query, title=item_with_underscores['title'],disciplines=discipline_list)

            # Cypher query for Referenced_In_Journal with weight
            if item_with_underscores.get('journal_ref'):
                referenced_in_journal_query = """
                    MERGE (b:Title {Title: $title})
                    WITH b, $journal_ref AS journal_ref, $Publication_Type AS publication_type, $ReferencedByCount AS referenced_by_count
                    MERGE (e:Journal_Reference {Journal_Reference: coalesce(journal_ref, 'Unknown'), Publication_Type: coalesce(publication_type, 'Unknown')})
                    MERGE (b)-[r:Referenced_In_Journal]->(e)
                    SET r.weight = referenced_by_count
                """
                session.run(referenced_in_journal_query, 
                            journal_ref=item_with_underscores['journal_ref'],
                            Publication_Type=item_with_underscores['PublicationType'],
                            ReferencedByCount=item_with_underscores['ReferencedByCount'],
                            title=item_with_underscores['title'])

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
    'save_data_to_neo4j_dag',
    default_args=default_args,
    description='A simple DAG to save data from a JSON file into a Neo4j database',
    schedule_interval=timedelta(days=1),
)

save_data_to_neo4j_task = PythonOperator(
    task_id='save_data_to_neo4j',
    python_callable=save_data_to_neo4j,
    dag=dag,
)
