import requests
import json
import os
import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

import asyncio
import aiohttp
import time

POSTGRES_USER = os.getenv('POSTGRES_USER', 'postgres')
POSTGRES_PASSWORD = os.getenv('POSTGRES_PASSWORD', 'postgrespassword')
POSTGRES_DB = os.getenv('POSTGRES_DB', 'data-engineering-project-db')

from sqlalchemy import create_engine

engine = create_engine(f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432/{POSTGRES_DB}')
with engine.connect() as conn:
    titles = pd.read_sql_query('select title from article', conn)


async def fetch(session, url, params, backoff=1):
    if backoff > 10:
        print(f"Too many retries for {url} {params}")
        return None
        # raise ValueError(f"Too many retries for {url} {params}")
    try:
        async with session.get(url, params=params) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientResponseError as e:
        if e.status == 404:
            return None
        elif e.status == 429 or 500 <= e.status < 600:
            await asyncio.sleep(backoff)
            return await fetch(session, url, params, backoff * 1.5)
        else:
            print(f"Error for {url} {params}", e.with_traceback())
            return None
    except asyncio.TimeoutError:
        await asyncio.sleep(backoff)
        return await fetch(session, url, params, backoff * 1.5)


# Base URL for the Crossref API
base_url = "https://api.crossref.org/works/"
results = []


async def process_title(session, semaphore, title):
    async with semaphore:
        doi_data = await fetch(session, base_url, {"query.title": title, "rows": 1})
        if doi_data is not None:
            doi = doi_data['message']['items'][0]['DOI']
            count_data = await fetch(session, base_url + doi, {})
            if count_data is not None:
                res = {
                    "title": title,
                    "citation_count": count_data['message']['is-referenced-by-count'],
                    "doi": doi
                }
                results.append(res)
                # print(".", end="")
                return res

    results.append({"title": title, "citation_count": None, "doi": None})


from tqdm.asyncio import tqdm_asyncio


async def process_titles(titles, span=range(0, 1000)):
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(25)
        tasks = [process_title(session, semaphore, title) for title in titles.loc[span, "title"]]
        await tqdm_asyncio.gather(*tasks)


for i in range(40000, len(titles), 10000):
    print(f"Processing {i} to {i + 10000}")
    results = []
    asyncio.run(process_titles(titles, range(i, i + 10000)))
    with open(f'Enriched_Data/citations_{i}.json', 'w') as f:
        json.dump(results, f)

# Create directory if it doesn't exist
if not os.path.exists('Enriched_Data'):
    os.makedirs('Enriched_Data')

# Write the results to a JSON file in the new directory


# # Define DAG
# dag = DAG('enrich', description='Enrich JSON files',
#           schedule_interval='0 12 * * *',
#           start_date=datetime(2023, 12, 20), catchup=False)
#
# # Define PythonOperator
# process_json_operator = PythonOperator(task_id='enrich_json_files', python_callable=process_json_files, dag=dag)
