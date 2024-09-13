import pandas as pd # type: ignore
import mysql.connector # type: ignore
from mysql.connector import Error # type: ignore
from dotenv import load_dotenv # type: ignore
import os

# Load environment variables from .env file
load_dotenv()

# scp -i C:\Users\Dell\Documents\Ubuntu-awsVM\ub2404.pem C:\Users\Dell\Downloads\spark-3.5.2-bin-hadoop3.tgz ubuntu@3.111.157.188:/home/ubuntu/Downloads

# MySQL connection details
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_PORT = os.getenv('MYSQL_PORT')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

# Path to your CSV files
customers_csv = '/home/ubuntu/retaildata/customers.csv'
orders_csv = '/home/ubuntu/retaildata/orders.csv'

# Table names
customers_table = 'customers'
orders_table = 'orders'

def insert_csv_to_table(csv_file, table_name, columns):
    # Read CSV file into DataFrame
    df = pd.read_csv(csv_file)

    try:
        # Connect to MySQL database
        connection = mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Iterate over each row in the DataFrame and insert into MySQL
            for _, row in df.iterrows():
                # Create an insert query dynamically using the column names
                query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s'] * len(columns))})"

                # Get the data from the row to insert
                data = tuple(row)

                # Execute the query
                cursor.execute(query, data)

            # Commit the transaction
            connection.commit()

            print(f"Data from {csv_file} has been successfully inserted into the table '{table_name}'.")

    except Error as e:
        print(f"Error: {e}")

    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

# Columns for each table
customers_columns = ["customerid", "customer_fname", "customer_lname", "username", "password", "address", "city", "state", "pincode"]
orders_columns = ["order_id", "order_date", "customer_id", "order_status"]

# Insert data from customers CSV into customers table
insert_csv_to_table(customers_csv, customers_table, customers_columns)

# Insert data from orders CSV into orders table
insert_csv_to_table(orders_csv, orders_table, orders_columns)
