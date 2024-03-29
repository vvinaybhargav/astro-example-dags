from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.git.operators.git import GitCloneOperator, GitOperator
from datetime import datetime

def convert_csv_to_json():
    import pandas as pd
    
    # Read CSV from local clone
    df = pd.read_csv('/path/to/local/clone/data.csv')
    
    # Convert DataFrame to JSON and save to local clone
    df.to_json('/path/to/local/clone/data.json', orient='records')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 27),
    'retries': 1,
}

with DAG('csv_to_json_and_git_push', default_args=default_args, schedule_interval=None) as dag:
    # Clone repository with authentication
    git_clone = GitCloneOperator(
        task_id='git_clone',
        git_repo='https://github.com/vvinaybhargav/airflow.git',
        target_directory='/path/to/local/clone',
        git_token='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'  # Replace with your personal access token
    )

    # Convert CSV to JSON
    convert_to_json = PythonOperator(
        task_id='convert_to_json',
        python_callable=convert_csv_to_json,
    )

    # Push changes to GitHub with authentication
    git_push = GitOperator(
        task_id='git_push',
        repo='/path/to/local/clone',
        command='push',
        remote='origin',
        ref='main',
        git_token='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'  # Replace with your personal access token
    )

    # Define task dependencies
    git_clone >> convert_to_json >> git_push
