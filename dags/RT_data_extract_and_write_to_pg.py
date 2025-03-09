from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import time
import psycopg2
import psycopg2.extras as extras
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,col,from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

spark=SparkSession.builder.appName('RTKafkaSpark')\
    .config('spark.sql.streaming.schemaInference','true')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
    .config('spark.driver.extraClassPath', '/home/shamim/spark/jars/postgresql-42.3.8.jar')\
    .config('spark.executor.extraClassPath', '/home/shamim/spark/jars/postgresql-42.3.8.jar')\
    .getOrCreate()

#defin dag
dag=DAG(
    dag_id='real_time_e2e_dag',
    #start_date=datetime(2025,3,5),
    schedule_interval=None
)


def write_to_postgres(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:postgresql://localhost:5432/postgres") \
        .option("dbtable", "temperature_details") \
        .option("user", "postgres") \
        .option("password", "1418") \
        .mode("append") \
        .save()

# Step 1: Extract Data
def extract_data(**kwargs):
    ##read csv file from url
    df = spark.readStream.format('kafka')\
                .option('kafka.bootstrap.servers', 'localhost:9092')\
                .option('subscribe', 'sensor_data')\
                .option('startingOffsets', 'latest')\
                .load()

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("date", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("humidity", DoubleType(), True)
        ])

    parsed_df = df.selectExpr('CAST(value AS STRING)')\
                 .select(from_json(col("value"), schema).alias("data"))\
                 .select("data.*")
    
    parsed_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start() \
    .awaitTermination()
    


task_extract_data= PythonOperator(
    task_id ='extract_data_task',
    python_callable=extract_data,
    #provide_context=True,
    dag=dag
)




task_create_table=SQLExecuteQueryOperator(
    task_id='create_table_task',
    conn_id='postgres',
    sql="""
    DROP TABLE IF EXISTS temperature_details CASCADE;
    CREATE TABLE IF NOT EXISTS temperature_details(
    id INT,
    date VARCHAR(100),
    temperature NUMERIC(10,2),
    humidity NUMERIC(10,2)
    );
    """,
    dag=dag
)



    
task_create_table >> task_extract_data