from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def print_hello():
    print("hello vinay")

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 27),
    'retries': 1,
    'catchup': False  # Don't run missed DAG runs
}

# Define DAG
dag = DAG(
    'print_hello_dag',
    default_args=default_args,
    description='A simple DAG to print hello',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
)

# Define task
print_hello_task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag,
)

print_hello_task
