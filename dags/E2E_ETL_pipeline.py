from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import time
import psycopg2
import pandas as pd
dag = DAG(
    dag_id = 'E2E_kafka_producer_data_ETL_dag',
    start_date=datetime(2024, 3, 9),
    schedule_interval= None
)

conn = psycopg2.connect(
    database='postgres',
    user='postgres',
    host='localhost',
    password='1418',
    port='5432'
)


def extract_data_from_kafka_producer(**kwargs):
    cursor = conn.cursor()
    query = 'SELECT * FROM weather_info'
    df = pd.read_sql(query,conn)
    cursor.close()
    print(df.head(5))


task_extract_data = PythonOperator(
    task_id = 'extract_data_task',
    python_callable=extract_data_from_kafka_producer,
    provide_context=True,
    dag=dag
)

task_create_fact_dim_table=SQLExecuteQueryOperator(
    task_id = 'create_fact_dim_table_task',
    conn_id='postgres',
    sql="""
        DROP TABLE dim_city CASCADE;
        CREATE TABLE IF NOT EXISTS dim_city(
        city_id SERIAL PRIMARY KEY,
        city VARCHAR(100)
        );

        DROP TABLE dim_wind_direction CASCADE;
        CREATE TABLE IF NOT EXISTS dim_wind_direction(
        wind_dir_id SERIAL PRIMARY KEY,
        wind_direction VARCHAR(100)
        );

        DROP TABLE dim_precipitation CASCADE;
        CREATE TABLE IF NOT EXISTS dim_precipitation(
        precipitation_id SERIAL PRIMARY KEY,
        preipitation VARCHAR(100)
        );

        DROP TABLE fact_weather CASCADE;
        CREATE TABLE IF NOT EXISTS fact_weather(
        id INT,
        date_time TIMESTAMP,
        city_id INT REFERENCES dim_city(city_id),
        wind_dir_id INT REFERENCES dim_wind_direction(wind_dir_id),
        precipitation_id INT REFERENCES dim_precipitation(precipitation_id),
        temperature NUMERIC(10,2),
        humidity NUMERIC(10,2),
        pressure NUMERIC(10,2),
        wind_speed NUMERIC(10,2),
        cloud_cover NUMERIC(10,2)
        );
    """
)


def load_fact_dim_table():
    return


task_extract_data >> task_create_fact_dim_table 