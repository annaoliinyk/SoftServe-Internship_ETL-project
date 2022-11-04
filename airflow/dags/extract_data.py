import os.path
import sys
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from OpenSkyDataExtractor.get_states import DataIngestion

sys.path.insert(0, os.path.abspath(os.path.dirname(os.path.join(os.getcwd() + "OpenSkyDataExtractor"))))

DEFAULT_ARGS = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'start_day': days_ago(0, 0, 0, 0, 0)
}


def print_states():
    DataIngestion().print_states()


with DAG(
        dag_id='ingest_data_from_OpenSkyApi_v1',
        default_args=DEFAULT_ARGS,
        description='This is a dag that logs into https://opensky-network.org/api/states/all, and then prints all '
                    'the states',
        start_date=days_ago(0, 0, 0, 0, 0),
) as dag:
    extract_data = PythonOperator(
        task_id='print_states',
        python_callable=print_states
    )

extract_data
