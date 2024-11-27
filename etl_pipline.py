import pandas as pd 
from google.cloud import storage 
import os 


def Extract(filename:str,first_sheet:str,second_sheet:str):
    orders_1 = pd.read_excel(f"{filename}",sheet_name=f'{first_sheet}')
    Returns_data = pd.read_excel(f'{filename}',sheet_name=f"{second_sheet}")
    Returns_data = Returns_data.rename(columns={'Order ID':'order_id','Market':'market'})
    combine_data = pd.merge(orders_1,Returns_data,how='right',on=['order_id','market'])
    print("Extract Successfully")
    return combine_data


def Transform(raw_data):
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
    return raw_data

'''''
def load_data_to_db(merged_data,db_file):
    engine = create_engine(f'sqlite:///{db_file}')
    merged_data.to_sql("sales_and_returns_data",engine,index=False,if_exists='replace')
    print(f"Data loaded into SQLite database: {db_file}")'''''


def load_data_to_cloud(df, destination_bucket, destination_file):
    """Loads the data to GCP Cloud Storage."""
    client = storage.Client()
    bucket = client.bucket(destination_bucket)
    blob = bucket.blob(destination_file)
    blob.upload_from_string(df.to_csv(index=False), content_type='text/csv')
    

def load_data_to_csv(full_data,full_path):
    full_data.to_csv(full_path,index=False)


def main():
    # Extract data 
    merge_df = Extract("superstore_sales.xlsx",'Orders','Returns')
    # Transform 
    clean_data = Transform(merge_df)
    # Load data to data lake
    load_data_to_cloud(clean_data,'BUCKET_NAME','FILE_NAME.csv')
    print('Data pipline run completed successfully')


if  __name__ == '__main__':
    main()
