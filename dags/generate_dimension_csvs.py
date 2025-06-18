from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import logging
import gc

def generate_dimension_csvs():
    """Generate dimension and fact tables from cleaned data"""
    input_path = "/opt/airflow/data/cleaned/cleaned_data.csv"
    output_path = "/opt/airflow/data/outputs"

    os.makedirs(output_path, exist_ok=True)

    try:
        logging.info(f"Reading data from {input_path}")
        chunk_size = 50000

        unique_customers = set()
        unique_products = set()
        unique_dates = set()
        total_rows = 0

        logging.info("First pass: collecting unique dimension values")
        for chunk_df in pd.read_csv(input_path, chunksize=chunk_size):
            chunk_df['InvoiceDate'] = pd.to_datetime(chunk_df['InvoiceDate'])

            customers = chunk_df[['CustomerID', 'Country']].dropna(subset=['CustomerID'])
            for _, row in customers.iterrows():
                unique_customers.add((str(row['CustomerID']), row['Country']))

            products = chunk_df[['StockCode', 'Description']].dropna(subset=['StockCode'])
            for _, row in products.iterrows():
                unique_products.add((row['StockCode'], row['Description']))

            dates = chunk_df['InvoiceDate'].dt.date.unique()
            unique_dates.update(dates)

            total_rows += len(chunk_df)

        logging.info(f"Total rows processed: {total_rows}")

        # Generate dim_customer WITHOUT customer_key
        logging.info("Generating customers dimension table")
        dim_customer = pd.DataFrame(list(unique_customers), columns=['CustomerID', 'Country'])
        dim_customer['CustomerID'] = pd.to_numeric(dim_customer['CustomerID'], errors='coerce')
        dim_customer = dim_customer.dropna(subset=['CustomerID'])
        dim_customer['CustomerID'] = dim_customer['CustomerID'].astype(int)
        dim_customer = dim_customer.drop_duplicates(subset=['CustomerID']).sort_values('CustomerID')
        dim_customer.to_csv(f"{output_path}/customers.csv", index=False)
        logging.info(f"Created customers.csv with {len(dim_customer)} records")

        logging.info("Generating products dimension table")
        dim_product = pd.DataFrame(list(unique_products), columns=['StockCode', 'Description'])
        dim_product = dim_product.drop_duplicates(subset=['StockCode']).sort_values('StockCode')
        dim_product.to_csv(f"{output_path}/products.csv", index=False)
        logging.info(f"Created products.csv with {len(dim_product)} records")

        logging.info("Generating dates dimension table")
        unique_dates_list = sorted(list(unique_dates))

        date_records = []
        for i, date_val in enumerate(unique_dates_list, 1):
            date_obj = pd.to_datetime(date_val)
            date_records.append({
                'date_id': i,
                'date': date_val,
                'year': date_obj.year,
                'month': date_obj.month,
                'day': date_obj.day,
                'weekday': date_obj.day_name(),
                'quarter': date_obj.quarter
            })

        dim_date = pd.DataFrame(date_records)
        dim_date.to_csv(f"{output_path}/dates.csv", index=False)
        logging.info(f"Created dates.csv with {len(dim_date)} records")

        date_lookup = {row['date']: row['date_id'] for _, row in dim_date.iterrows()}

        del unique_customers, unique_products, unique_dates, date_records
        gc.collect()

        logging.info("Generating sales fact table")
        sales_records = []
        sales_id = 1

        for chunk_df in pd.read_csv(input_path, chunksize=chunk_size):
            chunk_df['InvoiceDate'] = pd.to_datetime(chunk_df['InvoiceDate'])
            chunk_df['InvoiceDate_date'] = chunk_df['InvoiceDate'].dt.date
            chunk_df['CustomerID'] = chunk_df['CustomerID'].astype(int)
            chunk_df['date_id'] = chunk_df['InvoiceDate_date'].map(date_lookup)

            fact_chunk = chunk_df[['InvoiceNo', 'StockCode', 'CustomerID', 'date_id', 'Quantity', 'UnitPrice']].dropna(subset=['InvoiceNo', 'StockCode', 'CustomerID', 'date_id']).copy()
            fact_chunk.insert(0, 'sales_id', range(sales_id, sales_id + len(fact_chunk)))
            sales_id += len(fact_chunk)

            sales_records.extend(fact_chunk.to_dict('records'))

            del chunk_df, fact_chunk
            gc.collect()

            if len(sales_records) >= 100000:
                temp_df = pd.DataFrame(sales_records)
                if not os.path.exists(f"{output_path}/sales.csv"):
                    temp_df.to_csv(f"{output_path}/sales.csv", index=False)
                else:
                    temp_df.to_csv(f"{output_path}/sales.csv", mode='a', header=False, index=False)
                logging.info(f"Written {len(sales_records)} sales records to file")
                sales_records = []
                del temp_df
                gc.collect()

        if sales_records:
            temp_df = pd.DataFrame(sales_records)
            if not os.path.exists(f"{output_path}/sales.csv"):
                temp_df.to_csv(f"{output_path}/sales.csv", index=False)
            else:
                temp_df.to_csv(f"{output_path}/sales.csv", mode='a', header=False, index=False)
            logging.info(f"Written final {len(sales_records)} sales records to file")

        final_sales_count = sales_id - 1
        logging.info(f"Created sales.csv with {final_sales_count} records")

        logging.info("Successfully generated all dimension and fact tables")

    except FileNotFoundError:
        logging.error(f"Input file not found: {input_path}")
        raise
    except Exception as e:
        logging.error(f"Error generating dimension tables: {str(e)}")
        raise


# DAG definition
with DAG(
    dag_id="generate_dimension_tables",
    description="Generate dimension and fact tables from cleaned retail data",
    schedule_interval=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["etl", "dimensions", "data-warehouse"],
    max_active_runs=1,
) as dag:

    dimension_task = PythonOperator(
        task_id="generate_dimensions",
        python_callable=generate_dimension_csvs,
        pool='default_pool',
        doc_md="""
        ### Generate Dimension Tables Task

        This task reads the cleaned retail data and generates:
        - **customers.csv**: Customer dimension (CustomerID, Country)
        - **products.csv**: Product dimension (StockCode, Description)
        - **dates.csv**: Date dimension with date parts
        - **sales.csv**: Sales fact table with foreign key `date_id` and customer as `CustomerID`

        **Memory Optimized**: Processes data in chunks to handle large datasets.
        """,
    )

