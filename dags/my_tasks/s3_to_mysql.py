import pandas as pd # type: ignore
import pyarrow.parquet as pq # type: ignore
from sqlalchemy import create_engine # type: ignore
import boto3 # type: ignore
import io
import os
import mysql.connector # type: ignore
from dotenv import load_dotenv # type: ignore

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
mysql_db = 'analytics_db'

s3_bucket = 'my-retail-analysis'
s3_prefixes = ['output/customer_orders/',
               'output/state_order_count/',
               'output/status_count_data/'
               ]  # S3 Key for the Parquet file

# Function to map pandas dtypes to MySQL types
def map_dtype_to_mysql(dtype):
    if pd.api.types.is_integer_dtype(dtype):
        return "INT"
    elif pd.api.types.is_float_dtype(dtype):
        return "FLOAT"
    elif pd.api.types.is_bool_dtype(dtype):
        return "BOOLEAN"
    elif pd.api.types.is_datetime64_any_dtype(dtype):
        return "DATETIME"
    else:
        return "VARCHAR(255)"  # Default to VARCHAR for string types

def s3_to_mysql_transfer():
    # Create an S3 client
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY
    )

    # Connect to MySQL using mysql.connector
    conn = mysql.connector.connect(
        host=MYSQL_HOST,
        user=MYSQL_USER,
        password=MYSQL_PASSWORD,
        port=MYSQL_PORT
    )
    cursor = conn.cursor()

    # Create the database if it does not exist
    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {mysql_db}")
    conn.database = mysql_db  # Select the database

    # Loop through each s3_prefix
    for s3_prefix in s3_prefixes:
        # List all files in the S3 directory (s3_prefix)
        response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_prefix)
        files = [content['Key'] for content in response.get('Contents', [])]

        # Check if files exist in the directory
        if not files:
            print(f"No files found in {s3_prefix}. Skipping...")
            continue

        # Create a table name based on the s3_prefix
        table_name = os.path.basename(s3_prefix.rstrip('/'))

        # Loop through each file and process
        for s3_key in files:
            if s3_key.endswith('.parquet'):  # Only process parquet files
                print(f"Processing file: {s3_key}")

                # Download the parquet file from S3
                obj = s3.get_object(Bucket=s3_bucket, Key=s3_key)
                parquet_data = io.BytesIO(obj['Body'].read())

                # Read the Parquet file into a Pandas DataFrame
                df = pd.read_parquet(parquet_data)

                # Create the table schema dynamically
                columns = df.columns
                column_defs = ", ".join([f"`{col}` {map_dtype_to_mysql(df[col].dtype)}" for col in columns])

                # Drop the table if it exists, and create a new one
                cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
                create_table_query = f"CREATE TABLE `{table_name}` ({column_defs})"
                cursor.execute(create_table_query)
                print(f"Table `{table_name}` created successfully.")
                
                # Insert data into MySQL using cursor
                for index, row in df.iterrows():
                    # Create an insert query dynamically
                    insert_query = f"INSERT INTO `{table_name}` ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(row))})"
                    cursor.execute(insert_query, tuple(row))

                print(f"Data from {s3_key} inserted successfully into table `{table_name}`.")

    # Close MySQL connection
    conn.commit()
    cursor.close()
    conn.close()
    
if __name__ == '__main__':
     s3_to_mysql_transfer()