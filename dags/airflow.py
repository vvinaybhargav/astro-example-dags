from airflow import DAG
from airflow.operators.python import PythonOperator  # Shorten import
from airflow.providers.github.operators.github import GitHubCloneOperator, GitHubOperator  # Correct import 
from datetime import datetime

def convert_csv_to_json():
    import pandas as pd

    # **Adjust these paths to match your setup within the cloned repo**
    df = pd.read_csv('/path/to/local/clone/data.csv')
    df.to_json('/path/to/local/clone/data.json', orient='records')

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 27),  
    'retries': 1, 
}

with DAG('csv_to_json_and_git_push', default_args=default_args, schedule_interval=None) as dag:

    # **Ensure 'target_directory' points to a valid local path**
    git_clone = GitHubCloneOperator(
        task_id='git_clone',
        repository='https://github.com/vvinaybhargav/airflow.git',  # Update with the correct repo URL
        target_directory='/path/to/local/clone',
        github_token='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'  
    )

    convert_to_json = PythonOperator(
        task_id='convert_to_json',
        python_callable=convert_csv_to_json,
    )

    # Use GitHubOperator for the push
    git_push = GitHubOperator(
        task_id='git_push',
        repository='/path/to/local/clone', 
        github_token='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',
    )

    git_clone >> convert_to_json >> git_push 
