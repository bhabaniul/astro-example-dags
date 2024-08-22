from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
 
# Define default arguments for the DAG
default_args = {
    'owner': 'user',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}
 
# Define a simple Python function for a task
def print_hello():
    return "Hello, Airflow!"
 
# Instantiate the DAG
dag = DAG(
    'sample_dag',
    default_args=default_args,
    description='A simple test DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 1),
    catchup=False
)
 
# Define tasks
start = DummyOperator(
    task_id='start',
    dag=dag
)
 
hello_task = PythonOperator(
    task_id='hello_task',
    python_callable=print_hello,
    dag=dag
)
 
end = DummyOperator(
    task_id='end',
    dag=dag
)
 
# Define task dependencies
start >> hello_task >> end