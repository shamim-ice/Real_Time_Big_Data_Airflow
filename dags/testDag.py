from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

dag = DAG(
    dag_id='test_dag',
    start_date=datetime(2025, 3, 4),
    schedule_interval=None
)

    

task_print=PythonOperator(
    task_id='print_task',
    python_callable=lambda: print(f'Test for realtime E2E airflow pipeline'),
    dag=dag
)

task_print