#!/bin/sh

# Global defaults and back-compat
: "${AIRFLOW_HOME:="/usr/local/airflow"}"
# Fernet key used for password encryption, like "secret key" authenticated cryptography.
# Using script to solve the frequent Fernet Key cryptography error:
: "${AIRFLOW__CORE__FERNET_KEY:=${FERNET_KEY:=$(python -c "from cryptography.fernet import Fernet; FERNET_KEY = Fernet.generate_key().decode(); print(FERNET_KEY)")}}"
: "${AIRFLOW__CORE__EXECUTOR:=${EXECUTOR:-Local}Executor}"

export \
  AIRFLOW_HOME \
  AIRFLOW__CORE__EXECUTOR \
  AIRFLOW__CORE__FERNET_KEY \

if [ -e "/requirements.txt" ]; then
    $(command -v pip) install --user -r /requirements.txt
fi

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

case "$1" in
  webserver)
    airflow db init
    airflow users  create --role Admin --username admin --email admin --firstname admin --lastname admin --password admin
    if [ "$AIRFLOW__CORE__EXECUTOR" = "LocalExecutor" ]; then
      airflow scheduler &
    fi
    exec airflow webserver
    ;;
esac
