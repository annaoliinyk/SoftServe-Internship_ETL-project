#!/bin/sh
echo "start"

if [ -e "/requirements.txt" ]; then
    $(command -v pip) install --user --upgrade pip
    $(command -v pip) install --user -r /requirements.txt
fi

: "${AIRFLOW_HOME:="/usr/local/airflow"}"
Executor='LocalExecutor'
Fernet_key=''

export AIRFLOW_HOME
export AIRFLOW__CORE__EXECUTOR=$Executor
export AIRFLOW__CORE__FERNET_KEY=$Fernet_key

echo "AIRFLOW_HOME is $AIRFLOW_HOME"
echo "AIRFLOW__CORE__EXECUTOR is $AIRFLOW__CORE__EXECUTOR"
echo "AIRFLOW__CORE__FERNET_KEY is $AIRFLOW__CORE__FERNET_KEY"

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

echo "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN is $AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"

case "$1" in
  webserver)
    echo "Webserver started"
    airflow db init
    echo "Database initialized"
    airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
    echo "User admin created"
    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ]; then
      airflow scheduler &
      echo "Started scheduler"
    fi
    exec airflow webserver
    echo "Webserver executed"
    ;;
esac

read -rn1
