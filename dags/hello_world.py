from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def print_hello():
    print("hello")

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 27),
    'retries': 1
}

# Define DAG
dag = DAG(
    'print_hello_dag',
    default_args=default_args,
    description='A simple DAG to print hello',
    schedule_interval=None,  # Run once
)

# Define task
print_hello_task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag,
)

print_hello_task
