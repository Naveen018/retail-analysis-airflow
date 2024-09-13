#!/bin/bash

# Load environment variables from the .env file
export $(grep -v '^#' /home/ubuntu/retail-analysis/.env | xargs)

spark-submit \
  --master spark://spark-master:7077 \
  --packages org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.115 \
  --conf spark.hadoop.fs.s3a.access.key=$AWS_ACCESS_KEY_ID \
  --conf spark.hadoop.fs.s3a.secret.key=$AWS_SECRET_ACCESS_KEY \
  --conf spark.executor.cores=1 \
  --conf spark.executor.memory=1g \
  /home/ubuntu/retail-analysis/jobs/spark_transformation.py
#   /home/ubuntu/retail-analysis/jobs/spark_transformation.py
