from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago

from extract_data_helpers import extract_data


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
    task_1 = BashOperator(
        task_id='task_1',
        bash_command="cd C:/Users/anoliinyk/Documents/SoftServe_Internship/SoftServe-Internship_ETL-project"
    ),
    task_2 = BashOperator(
        task_id='task_2',
        bash_command=
        "python -c '''from OpenSkyDataExtractor.get_states import DataIngestion\nDataIngestion().print_states()''' "
    )
    task_3 = SimpleHttpOperator(
        task_id='get_data_using_http_operator',
        method='GET',
        endpoint='https://opensky-network.org/api/states/all',
        # response_filter=lambda response: json.loads(response.text)
        # TODO: add credentials = get_credentials_from_file() as data
    ),
    task_4 = PythonOperator(
        task_id='ingest_data_using_python_operator',
        python_callable=extract_data.get_states
    ),
    task_5 = BashOperator(
        task_id='ingest_data_using_bash_operator_python_command',
        bash_command="python -c 'OpenSkyDataExtractor.get_states.DataIngestion().print_states'"
    )

task_1 >> task_2
