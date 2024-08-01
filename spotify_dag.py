from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from spotify_etl import extract_and_store_data
import spotipy

#define the default argument
default_args = {
    'owner': 'Kenny',
    'email': ['adewuyi136@gmail.com'],
    'email_on_retry': True,
    'email_on_failure': True,
    'retries':1,
    'retry_delay': timedelta(seconds=10)
}

#define a dag
dag = DAG(
    'Spotify_ETL_dag',
    default_args= default_args,
    start_date= days_ago(0),
    schedule_interval= timedelta(days=1)
)
#define the extract transform task
extract_transform_task = PythonOperator(
    task_id = 'extract_and_save_data',
    python_callable= extract_and_store_data,
    dag = dag
)

#set task dependencies
extract_transform_task