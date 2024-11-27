from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import storage

# Functions for the pipeline
def extract(filename: str, first_sheet: str, second_sheet: str) -> pd.DataFrame:
    """Extract and merge data from two Excel sheets."""
    orders_1 = pd.read_excel(filename, sheet_name=first_sheet)
    Returns_data = pd.read_excel(filename, sheet_name=second_sheet)
    Returns_data = Returns_data.rename(columns={'Order ID': 'order_id', 'Market': 'market'})
    combine_data = pd.merge(orders_1, Returns_data, how='right', on=['order_id', 'market'])
    print("Extract Successfully")
    combine_data.to_csv('/tmp/merged_data.csv', index=False)  # Save to temporary storage
    return combine_data

def transform():
    """Clean and transform the raw data."""
    raw_data = pd.read_csv('/tmp/merged_data.csv')
    raw_data['order_date'] = pd.to_datetime(raw_data['order_date'], format='%Y-%m-%d')
    raw_data['ship_date'] = pd.to_datetime(raw_data['ship_date'], format='%Y-%m-%d')
    raw_data.fillna(
        {
            'order_date': raw_data['order_date'].mode()[0],
            'ship_date': raw_data['ship_date'].mode()[0],
            'ship_mode': raw_data['ship_mode'].mode()[0],
            'customer_name': 'Unknown Customer',
            'segment': raw_data['segment'].mode()[0],
            'state': 'Unknown State',
            'country': 'Unknown Country',
            'region': 'Unknown Region',
            'product_id': 'Unnamed Product',
            'category': raw_data['category'].mode()[0] if raw_data['category'].mode().any() else 'Uncategorized',
            'sub_category': raw_data['sub_category'].mode()[0] if raw_data['sub_category'].mode().any() else 'Unknown Sub-Category',
            'product_name': 'Unnamed Product',
            'sales': raw_data['sales'].mean(),
            'quantity': 1,
            'discount': raw_data['discount'].median(),
            'profit': raw_data['profit'].mean(),
            'shipping_cost': raw_data['shipping_cost'].median(),
            'order_priority': raw_data['order_priority'].mode()[0],
            'year': raw_data['order_date'].dt.year.median()  
        },
        inplace=True
    )
    raw_data['Processing_time_(Days)'] = (raw_data['ship_date'] - raw_data['order_date']).dt.days
    print(f"Missing values cleared: {raw_data.isna().sum()}")
    raw_data.to_csv('/tmp/cleaned_data.csv', index=False)  # Save to temporary storage

def load_to_cloud(destination_bucket: str, destination_file: str):
    """Load the DataFrame to GCP Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(destination_bucket)
    blob = bucket.blob(destination_file)
    with open('/tmp/cleaned_data.csv', 'rb') as data:
        blob.upload_from_file(data)
    print(f"Data loaded to GCP bucket: {destination_bucket}, file: {destination_file}")

# Airflow DAG definition
default_args = {
    'owner': 'YOUR_NAME',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='data_pipeline_dag',
    default_args=default_args,
    description='SUPERSTORESALES_PIPLINE',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    # Task 1: Extract
    task_extract = PythonOperator(
        task_id='extract_data',
        python_callable=extract,
        op_kwargs={
            'filename': 'superstore_sales.xlsx',
            'first_sheet': 'Orders',
            'second_sheet': 'Returns',
        },
    )

    # Task 2: Transform
    task_transform = PythonOperator(
        task_id='transform_data',
        python_callable=transform,
    )

    # Task 3: Load
    task_load = PythonOperator(
        task_id='load_data_to_cloud',
        python_callable=load_to_cloud,
        op_kwargs={
            'destination_bucket': 'BUCKET_NAME',
            'destination_file': 'FILE_NAME.csv',
        },
    )

    # Define task dependencies
    task_extract >> task_transform >> task_load
