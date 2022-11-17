from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from OpenSkyDataExtractor.get_states import DataIngestion

DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_day': days_ago(0, 0, 0, 0, 0)
}

with DAG(
        dag_id='ingest_data_from_OpenSkyApi_v1',
        default_args=DEFAULT_ARGS,
        description='This is a dag that logs into https://opensky-network.org/api/states/all, and then prints all '
                    'the states',
        schedule="@daily",
        catchup=False,
        start_date=days_ago(0, 0, 0, 0, 0),
) as dag:
    task_1 = PythonOperator(
        task_id='ingest_data_using_python_operator',
        python_callable=DataIngestion().print_states
    )

task_1
