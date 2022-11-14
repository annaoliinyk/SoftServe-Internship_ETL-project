FROM apache/airflow:2.4.0

USER root

RUN export AIRFLOW_HOME=../
ENV AIRFLOW_HOME=../

WORKDIR ${AIRFLOW_HOME}

COPY entrypoint.sh ./entrypoint.sh

RUN chown -R airflow: $AIRFLOW_HOME
RUN chmod +x ./entrypoint.sh

USER airflow

EXPOSE 8080

ENTRYPOINT ["./entrypoint.sh"]

# to fix warning https://github.com/apache/airflow/issues/14266
RUN pip uninstall --yes azure-storage && pip install -U azure-storage-blob apache-airflow-providers-microsoft-azure==1.1.0

CMD ["webserver"]
