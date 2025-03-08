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
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

spark=SparkSession.builder.appName('RTKafkaSpark')\
    .config('spark.sql.streaming.schemaInference','true')\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")\
    .config('spark.driver.extraClassPath', '/home/shamim/spark/jars/postgresql-42.3.8.jar')\
    .config('spark.executor.extraClassPath', '/home/shamim/spark/jars/postgresql-42.3.8.jar')\
    .getOrCreate()

#defin dag
dag=DAG(
    dag_id='real_time_e2e_dag',
    start_date=datetime(2025,3,5),
    schedule_interval='0 23 * * *'        #runs every day morning at 05:00 am
)

# Step 1: Extract Data
def extract_data(**kwargs):
    ##read csv file from url
    df = spark.readStream.format('kafka')\
    .option('kafka.bootstrap.servers', 'localhost:9092')\
    .option('subscribe', 'sensor_data')\
    .option('startingOffsets', 'latest')\
    .load()
    
    kwargs['ti'].xcom_push(key='DataFrame', value=df)
    


task_extract_data= PythonOperator(
    task_id ='extract_data_task',
    python_callable=extract_data,
    provide_context=True,
    dag=dag
)



##Load transformed data from Pandas dataframe to PostgreSQL
####create staging table supermarket_sales
task_create_table=SQLExecuteQueryOperator(
    task_id='create_table_task',
    conn_id='postgres',
    sql="""
    DROP TABLE IF EXISTS temperature_details CASCADE;
    CREATE TABLE IF NOT EXISTS temperature(
    id VARCHAR(10),
    date VARCHAR(100),
    temperature NUMERIC(10,2),
    humidity NUMERIC(10,2)
    );
    """,
    dag=dag
)
#Establish connection to postgreSQL database
conn = psycopg2.connect(
    database='postgres', # databse name
    user='postgres',      #databse user
    password='1418',   #password
    host='localhost',
    port='5432'
)

def insert_df_pg(conn, table, **kwargs):
    start_time=time.time()
    df = kwargs['ti'].xcom_pull(key='DataFrame', task_ids='extract_data_task')
    tuples=[tuple(x) for x in df.to_numpy()]
    columns_name = df.columns  # Join column names as a single string (incorrect approach)
    formatted_columns = [s.lower().replace(" ", "_").replace("%","_percent") for s in df.columns]  # Correct approach on actual column names
    columns_name = ','.join(formatted_columns)  # Convert back to a single string
    query = 'insert into %s(%s) values %%s' % (table,columns_name)
    print(query)
    cursor=conn.cursor()
    try:
        cursor.execute(f'Delete From {table}')
        conn.commit()
        print(f'Existing records from {table} deleted successfully!')
        extras.execute_values(cursor,query,tuples)
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print('Error: %s'% error)
        conn.rollback()
        conn.close()
        return 1

    end_time=time.time()
    elapsed_time=end_time-start_time
    print('Dataframe is inserted successfully!')
    print(f'Insert Time: {elapsed_time} seconds.')
    cursor.close()


task_load_clean_data_into_postgresql=PythonOperator(
    task_id='load_clean_data_pg_task',
    python_callable=insert_df_pg,
    op_kwargs={
        'conn':conn,
        'table':'temperature_details'
    },
    provide_context=True,
    dag=dag
)


task_extract_data  >> task_create_table >>task_load_clean_data_into_postgresql