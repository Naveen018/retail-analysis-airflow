from pyspark.sql import SparkSession # type: ignore
import os
from dotenv import load_dotenv # type: ignore
# import boto3 # type: ignore

# Load environment variables from .env file
load_dotenv()

# Get AWS credentials from environment variables
aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# def run_spark_transformation():
spark = SparkSession.builder \
     .appName("S3_Parquet") \
     .config("spark.hadoop.fs.s3a.impl",  "org.apache.hadoop.fs.s3a.S3AFileSystem") \
     .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
     .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
     .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.115") \
     .getOrCreate()

# S3 bucket information
bucket_name = 'my-retail-analysis'
customers_s3_path = f's3a://{bucket_name}/input/customers.parquet'
orders_s3_path = f's3a://{bucket_name}/input/orders.parquet'

# Read Parquet files from S3
customers_df = spark.read.parquet(customers_s3_path)
orders_df = spark.read.parquet(orders_s3_path)

# Register DataFrames as temporary views for SQL
customers_df.createOrReplaceTempView("customers")
orders_df.createOrReplaceTempView("orders")

order_status_count_df = spark.sql("""SELECT order_status, COUNT(*) AS status_count FROM orders GROUP BY order_status""")

order_count_df = spark.sql("""SELECT c.customerid, c.customer_fname, c.customer_lname, COUNT(o.order_id) AS order_count
                         FROM customers c 
                         JOIN orders o ON c.customerid = o.customer_id
                         GROUP BY c.customerid, c.customer_fname, c.customer_lname
                         ORDER BY order_count DESC""")

state_wise_df = spark.sql("""SELECT state, COUNT(*) AS customer_count FROM customers
          GROUP BY state ORDER BY customer_count DESC""")

# S3 output paths for each transformation
output_path_status_count = f's3a://{bucket_name}/output/status_count_data'
output_path_customer_orders = f's3a://{bucket_name}/output/customer_orders'
output_path_state_order_count = f's3a://{bucket_name}/output/state_order_count'

# Save the transformed DataFrames to S3
order_status_count_df.write.mode('overwrite').parquet(output_path_status_count)
order_count_df.write.mode('overwrite').parquet(output_path_customer_orders)
state_wise_df.write.mode('overwrite').parquet(output_path_state_order_count)

# Optional: Print a confirmation message
print(f"Transformations saved to S3 at:\n {output_path_status_count}\n {output_path_customer_orders}\n {output_path_state_order_count}")

# Stop the Spark session
spark.stop()
     
# if __name__ == '__main__':
#      run_spark_transformation()