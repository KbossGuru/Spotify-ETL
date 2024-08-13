from airflow import DAG
from datetime import timedelta
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from spotify_etl import extract_and_store_data, transform_data, load_data_to_s3

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
#define the extract task
extract_task = PythonOperator(
    task_id = 'extract_and_store_data',
    python_callable= extract_and_store_data,
    dag = dag
)

#define the transform task
transform_task = PythonOperator(
    task_id = 'transform_data',
    python_callable= transform_data,
    dag = dag
)

#define the load the data into an s3 bucket task
load_to_s3_task = PythonOperator(
    task_id = 'load_into_s3_bucket',
    python_callable= load_data_to_s3,
    dag = dag
)

#set task dependencies
extract_task >> transform_task >> load_to_s3_task