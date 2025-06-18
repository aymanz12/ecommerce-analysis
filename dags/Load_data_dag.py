from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
from kaggle.api.kaggle_api_extended import KaggleApi

RAW_DATA_PATH = "/opt/airflow/data/raw"
CLEANED_DATA_PATH = "/opt/airflow/data/cleaned"
DATASET_NAME = "carrie1/ecommerce-data"
CSV_FILE = os.path.join(RAW_DATA_PATH, "data.csv")
CLEANED_FILE = os.path.join(CLEANED_DATA_PATH, "cleaned_data.csv")

def download_from_kaggle():
    api = KaggleApi()
    api.authenticate()
    api.dataset_download_files(DATASET_NAME, path=RAW_DATA_PATH, unzip=True)

def clean_data():
    df = pd.read_csv(CSV_FILE, encoding='ISO-8859-1')
    df_clean = df[(df['Quantity'] > 0) & 
                  (df['UnitPrice'] > 0) & 
                  (df['CustomerID'].notnull())]

    os.makedirs(CLEANED_DATA_PATH, exist_ok=True)
    df_clean.to_csv(CLEANED_FILE, index=False)

with DAG(
    dag_id="load_clean_data_ecommerce",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["kaggle", "etl", "cleaning"],
) as dag:

    download_task = PythonOperator(
        task_id="download_data",
        python_callable=download_from_kaggle,
    )

    clean_task = PythonOperator(
        task_id="clean_data",
        python_callable=clean_data,
    )

    download_task >> clean_task

