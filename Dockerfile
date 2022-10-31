FROM apache/airflow:2.4.0

USER root

ENV AIRFLOW_HOME=/usr/local/airflow

COPY ./entrypoint.sh ./entrypoint.sh

RUN chown -R airflow: ${AIRFLOW_HOME}
RUN chmod +x ./entrypoint.sh

USER airflow
WORKDIR ${AIRFLOW_HOME}

EXPOSE 8080

ENTRYPOINT ["./entrypoint.sh"]

# to fix warning https://github.com/apache/airflow/issues/14266
RUN pip uninstall --yes azure-storage && pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0

CMD ["webserver"]
