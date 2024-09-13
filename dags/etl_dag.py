from datetime import datetime, timedelta
from airflow import DAG # type: ignore
from airflow.operators.python import PythonOperator # type: ignore
from airflow.operators.bash import BashOperator # type: ignore
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator # type: ignore
from dotenv import load_dotenv # type: ignore
import os

# Load environment variables from .env file
load_dotenv()

# Import the transfer function from the data_transfer module
# from spark_transformation import run_spark_transformation
from my_tasks.s3_to_mysql import s3_to_mysql_transfer
from my_tasks.mysql_to_s3 import transfer_mysql_to_s3

aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

# Airflow DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 9),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag = DAG(
    'ETL_dag',
    default_args=default_args,
    description='Spark ETL',
    schedule_interval=timedelta(minutes=10),  # Runs every 10 minutes
    catchup=False  # Only run the latest scheduled job, no backfilling
)

# Task to transfer MySQL data to S3
transfer_customers_task = PythonOperator(
    task_id='transfer_customers_to_s3',
    python_callable=transfer_mysql_to_s3,
    op_args=['customers', 'input/customers.parquet'],  # Table name and s3 file name
    dag=dag,
)

transfer_orders_task = PythonOperator(
    task_id='transfer_orders_to_s3',
    python_callable=transfer_mysql_to_s3,
    op_args=['orders', 'input/orders.parquet'],  # Pass arguments to the function
    dag=dag,
)

# spark_job = BashOperator(
#     task_id='submit_spark_job',
#     bash_command="/home/ubuntu/retail-analysis/script/spark-job.sh",
#     dag=dag,
#     execution_timeout=timedelta(minutes=5)
# )

transfer_s3_task = PythonOperator(
    task_id='transfer_s3_to_mysql',
    python_callable=s3_to_mysql_transfer,
    dag=dag,
)

# spark_transformation_task = PythonOperator(
#     task_id='spark_transformation',
#     python_callable=run_spark_transformation,
#     dag=dag,
# )

submit_spark_job = SparkSubmitOperator(
    application="/opt/spark/jobs/spark_transformation.py",  # Path to your PySpark script
    task_id="spark_job",
    conn_id="spark_default",  # Connection ID for Spark
    dag=dag,
    # conf={
    #     "spark.hadoop.fs.s3a.access.key": aws_access_key,
    #     "spark.hadoop.fs.s3a.secret.key": aws_secret_key,
    #     "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.115",
    # },
)

# Set task dependencies
transfer_customers_task >> transfer_orders_task >> submit_spark_job # This makes 'orders' task dependent on 'customers' task

# transfer_customers_task >> transfer_orders_task >> >> spark_job >> transfer_s3_task   'This should be actual flow'
# transfer_customers_task >> transfer_orders_task >> transfer_s3_task
# transfer_customers_task >> transfer_orders_task >> spark_transformation_task