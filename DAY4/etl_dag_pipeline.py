from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

import pandas as pd
import requests
import os

# ------------------------------
# ETL Task Functions
# ------------------------------

def extract_data(**context):
    """Extract data from Random User API"""
    url = "https://randomuser.me/api/?results=10"
    response = requests.get(url)
    data = response.json()
    # Push raw data to XCom (Airflow's internal message passing)
    context['ti'].xcom_push(key='raw_data', value=data)
    print("Extracted data from API")


def transform_data(**context):
    """Transform raw JSON data into a clean DataFrame"""
    raw_data = context['ti'].xcom_pull(key='raw_data', task_ids='extract_task')
    users = raw_data['results']

    # Extract selected fields
    records = []
    for user in users:
        records.append({
            'name': f"{user['name']['first']} {user['name']['last']}",
            'email': user['email'],
            'country': user['location']['country'],
            'age': user['dob']['age']
        })

    df = pd.DataFrame(records)
    context['ti'].xcom_push(key='clean_data', value=df.to_json())
    print(" Transformed data successfully")


def load_data(**context):
    """Load cleaned data into a CSV file"""
    clean_json = context['ti'].xcom_pull(key='clean_data', task_ids='transform_task')
    df = pd.read_json(clean_json)
    os.makedirs('/tmp/airflow_output', exist_ok=True)
    output_path = '/tmp/airflow_output/users_data.csv'
    df.to_csv(output_path, index=False)
    print(f" Data loaded into {output_path}")


# ------------------------------
# DAG Definition
# ------------------------------
with DAG(
    dag_id="real_etl_pipeline",
    start_date=datetime(2025, 11,6),
    schedule_interval="@daily",
    catchup=False,
    tags=["ETL", "example", "pandas"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_task",
        python_callable=extract_data,
        provide_context=True
    )

    transform_task = PythonOperator(
        task_id="transform_task",
        python_callable=transform_data,
        provide_context=True
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=load_data,
        provide_context=True
    )

    # Set dependency order: Extract → Transform → Load
extract_task >> transform_task >> load_task
