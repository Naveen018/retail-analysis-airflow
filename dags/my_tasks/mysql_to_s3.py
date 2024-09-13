import mysql.connector # type: ignore
import pandas as pd # type: ignore
import boto3 # type: ignore
from io import StringIO
from io import BytesIO
import pyarrow as pa # type: ignore
import pyarrow.parquet as pq # type: ignore
from datetime import datetime, timedelta

from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from dotenv import load_dotenv # type: ignore
import os

# Load environment variables from .env file
load_dotenv()

# MySQL and AWS credentials from environment variables
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')
AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')

def transfer_mysql_to_s3(table_name: str, s3_file_name: str):
    # 1. MySQL connection
    mysql_conn = mysql.connector.connect(
        host=MYSQL_HOST,
        port=MYSQL_PORT,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        database=MYSQL_DATABASE
    )

    # 2. Query to extract data
    query = f"SELECT * FROM {table_name}"
    df = pd.read_sql(query, mysql_conn)
    mysql_conn.close()  # Close MySQL connection after fetching data

    print(f"Data fetched from MySQL table '{table_name}':")
    print(df.head())  # Display first 5 rows for verification

    # 3. S3 client configuration
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # S3 bucket and file name
    bucket_name = 'my-retail-analysis'

    # 4. Save DataFrame to an in-memory buffer as CSV
    # csv_buffer = StringIO()
    # df.to_csv(csv_buffer, index=False)
    
    parquet_buffer = BytesIO()
    table = pa.Table.from_pandas(df)
    pq.write_table(table, parquet_buffer)

    # 5. Upload CSV to S3 from the buffer
    s3_client.put_object(Bucket=bucket_name, Key=s3_file_name, Body=parquet_buffer.getvalue())

    print(f"Data uploaded to S3 bucket '{bucket_name}' as '{s3_file_name}'.")
    
# if __name__ == '__main__':
#      transfer_mysql_to_s3('customers', 'input/customers.parquet')
#      transfer_mysql_to_s3('orders', 'input/orders.parquet')
