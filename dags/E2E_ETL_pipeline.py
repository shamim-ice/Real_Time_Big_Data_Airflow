from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import time
import psycopg2, psycopg2.extras as extras
import pandas as pd


dag = DAG(
    dag_id = 'E2E_kafka_producer_data_ETL_dag',
    start_date=datetime(2025, 3, 9),
    schedule_interval= '0 16 * * *'
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
    df = pd.DataFrame(df)
    cursor.close()
    print(df.head(5))
    kwargs['ti'].xcom_push(key='DataFrame', value=df)
    


task_extract_data = PythonOperator(
    task_id = 'extract_data_task',
    python_callable=extract_data_from_kafka_producer,
    provide_context=True,
    dag=dag
)

def transform_data(**kwargs):
    df = kwargs['ti'].xcom_pull(key='DataFrame', task_ids='extract_data_task')
    df = df.dropna()
    df = df.drop_duplicates()
    df = df.loc[:, ~df.T.duplicated()]
    df = df.loc[:, df.apply(pd.Series.nunique)>1]
    kwargs['ti'].xcom_push(key='DataFrame', value=df)



task_transform_data=PythonOperator(
    task_id='transform_data_task',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

task_create_transformed_fact_dim_table=SQLExecuteQueryOperator(
    task_id = 'create_transformed_fact_dim_table_task',
    conn_id='postgres',
    sql="""
        DROP TABLE IF EXISTS transformed_weather_info CASCADE;
        create table if not exists transformed_weather_info(
        id INT,
        date_time TIMESTAMP,
        city VARCHAR(100),
        temperature NUMERIC(10,2),
        humidity NUMERIC(10,2),
        pressure NUMERIC(10,2),
        wind_speed NUMERIC(10,2),
        wind_direction VARCHAR(100),
        precipitation VARCHAR(100),
        cloud_cover NUMERIC(10,2)
        );

        DROP TABLE IF EXISTS dim_city CASCADE;
        CREATE TABLE IF NOT EXISTS dim_city(
        city_id SERIAL PRIMARY KEY,
        city VARCHAR(100)
        );

        DROP TABLE IF EXISTS dim_wind_direction CASCADE;
        CREATE TABLE IF NOT EXISTS dim_wind_direction(
        wind_dir_id SERIAL PRIMARY KEY,
        wind_direction VARCHAR(100)
        );

        DROP TABLE IF EXISTS dim_precipitation CASCADE;
        CREATE TABLE IF NOT EXISTS dim_precipitation(
        precipitation_id SERIAL PRIMARY KEY,
        precipitation VARCHAR(100)
        );

        DROP TABLE IF EXISTS fact_weather CASCADE;
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
    """,
    dag=dag
)

def insert_df_into_pg(conn, table, **kwargs):
    start_time = time.time()
    df = kwargs['ti'].xcom_pull(key='DataFrame', task_ids='transform_data_task')
    tuples = [tuple(x) for x in df.to_numpy()]
    columns_name = ','.join(list(df.columns))
    query = 'insert into %s(%s) values %%s' % (table,columns_name)
    print(query)
    cursor = conn.cursor()
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
    
    end_time= time.time()
    print('Dataframe is inserted successfully!')
    print(f'Insert Time: {end_time-start_time} seconds.' )
    cursor.close()

task_load_clean_data_into_postgres=PythonOperator(
    task_id='load_clean_data_into_pg_task',
    python_callable=insert_df_into_pg,
    op_kwargs={
        'conn': conn,
        'table': 'transformed_weather_info'
    },
    provide_context=True,
    dag=dag
)




task_load_fact_dim_data=SQLExecuteQueryOperator(
    task_id = 'load_fact_dim_table_task',
    conn_id='postgres',
    sql="""
    INSERT INTO dim_city(city)
    SELECT DISTINCT city from transformed_weather_info;

    INSERT INTO dim_wind_direction(wind_direction)
    SELECT DISTINCT wind_direction FROM transformed_weather_info;

    INSERT INTO dim_precipitation(precipitation)
    SELECT DISTINCT precipitation FROM transformed_weather_info;

    INSERT INTO fact_weather(id, date_time, city_id, wind_dir_id, precipitation_id,\
        temperature, humidity, pressure, wind_speed, cloud_cover)
    SELECT DISTINCT (twi.id), twi.date_time, dc.city_id, dwd.wind_dir_id, dp.precipitation_id,\
        twi.temperature, twi.humidity, twi.pressure, twi.wind_speed, twi.cloud_cover\
        FROM transformed_weather_info twi\
        join dim_city dc on twi.city=dc.city \
            join dim_wind_direction dwd on twi.wind_direction=dwd.wind_direction\
               join dim_precipitation dp on twi.precipitation=dp.precipitation;
    """,
    dag=dag
)


task_extract_data >> task_transform_data >> task_create_transformed_fact_dim_table\
>> task_load_clean_data_into_postgres >> task_load_fact_dim_data