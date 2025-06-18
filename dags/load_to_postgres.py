import pandas as pd
import psycopg2
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def load_csv_to_postgres(table_name, csv_path):
    conn = psycopg2.connect(
        host="example-database",
        database="e-commerce",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    with open(csv_path, 'r', encoding='utf-8') as f:
        next(f)  # skip header
        cur.copy_expert(f"COPY {table_name} FROM STDIN WITH CSV", f)
    
    conn.commit()
    cur.close()
    conn.close()

def load_all_tables():
    base_path = "/opt/airflow/data/outputs"
    load_csv_to_postgres("dim_customer", f"{base_path}/customers.csv")
    load_csv_to_postgres("dim_product", f"{base_path}/products.csv")
    load_csv_to_postgres("dim_date", f"{base_path}/dates.csv")
    load_csv_to_postgres("fact_sales", f"{base_path}/sales.csv")

with DAG(
    dag_id="load_csvs_to_postgres",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    load_task = PythonOperator(
        task_id="load_csv_files",
        python_callable=load_all_tables
    )

