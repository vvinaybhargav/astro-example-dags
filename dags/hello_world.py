from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

def print_hello():
    print("hello")

# Define DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 27),
    'retries': 1,
    'catchup': False  # Don't run missed DAG runs
}

# Define DAG
dag = DAG(
    'print_hello_and_echo_hello_world_dag',
    default_args=default_args,
    description='A DAG to print hello and echo hello world',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
)

# Define PythonOperator task
print_hello_task = PythonOperator(
    task_id='print_hello_task',
    python_callable=print_hello,
    dag=dag,
)

# Define BashOperator task
echo_hello_world_task = BashOperator(
    task_id='echo_hello_world_task',
    bash_command='echo "hello world"',
    dag=dag,
)

# Set up task dependencies
print_hello_task >> echo_hello_world_task
