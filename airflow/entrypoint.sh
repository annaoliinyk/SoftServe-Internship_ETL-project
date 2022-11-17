#!/bin/sh

echo "Start the entrypoint"
if [ -e "../requirements.txt" ]; then
    pip install --upgrade pip
    $(command -v pip) install -r ../requirements.txt
echo "Cryptography was installed"

export \
  AIRFLOW_HOME \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \

fi

echo "Airflow_home is $AIRFLOW_HOME"
echo "Airflow_core_fernet_key is $AIRFLOW__CORE__FERNET_KEY"
echo "Airflow_core_executor is $AIRFLOW__CORE__EXECUTOR"

if [ -z "$AIRFLOW__CORE__SQL_ALCHEMY_CONN" ]; then
    : "${POSTGRES_HOST:="postgres"}"
    : "${POSTGRES_PORT:="5432"}"
    : "${POSTGRES_USER:="airflow"}"
    : "${POSTGRES_PASSWORD:="airflow"}"
    : "${POSTGRES_DB:="airflow"}"
    : "${POSTGRES_EXTRAS:-""}"
fi

AIRFLOW__DATABASE__SQL_ALCHEMY_CONN="postgresql+psycopg2://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}${POSTGRES_EXTRAS}"
export AIRFLOW__DATABASE__SQL_ALCHEMY_CONN

echo "Airflow database is $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"

case "$1" in
  webserver)
    echo "Starting webserver"
    airflow db init
    echo "Database initialized"
    airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
    echo "Admin user is created"
    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ]; then
      airflow scheduler &
      echo "Scheduler has started"
    fi
    exec airflow webserver
    echo "Webserver started"
    ;;
esac
