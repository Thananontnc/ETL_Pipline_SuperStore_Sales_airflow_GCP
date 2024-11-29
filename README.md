# ETL_Pipline_SuperStore_Sales_airflow_DataFusion_GCP
I use dataset from kaggle.
Here is the link : https://www.kaggle.com/datasets/mahmoudashraf1/superstore-sales/data

## Here is the Architecture : 
![SuperStoreSales_Architecture](https://github.com/user-attachments/assets/8f219088-da8e-4a02-82e5-ea37e3128e1f)

The project aims to perform the following tasks:

1. Data Extraction: Extract data using python.
2. Data Masking: Apply data masking & encoding techniques to sensitive information in Cloud Data Fusion before loading it into BigQuery.
3. Data Loading: Load transformed data into Google BigQuery tables.
4. Orchestration: Automate complete Data pipeline using Airflow ( Cloud Composer )

Step-by-Step Flow:
SuperStore Sales Data: This is the source data for your ETL pipeline "CSV".

Apache Airflow DAG:
Airflow is used to schedule and manage the ETL pipeline tasks.
Airflow's DAG will handle the extraction of sales data, triggering transformation tasks, and loading data into GCS and BigQuery.

DataFusion:
DataFusion is used to handle any complex data processing or transformations. You could use it to merge data, clean data, or aggregate information from different sources.
It may also interact with GCS or other storage systems.

Google Cloud Storage (GCS):
GCS acts as a staging area for raw and processed data.
Raw data extracted from the sources is uploaded to GCS before processing.
Processed or transformed data is stored in GCS before being loaded into BigQuery.

BigQuery:
BigQuery is where the transformed data is loaded and where you perform analytics.
It could be used to run queries for reporting, analysis, or dashboards.
