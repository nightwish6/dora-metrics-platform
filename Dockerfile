FROM apache/airflow:2.10.5-python3.12

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

USER root

RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/configs \
             /opt/airflow/plugins /opt/airflow/data/duckdb \
             /opt/airflow/dbt/models/staged \
             /opt/airflow/dbt/models/marts \
             /opt/airflow/dbt/models/metrics \
    && chown -R airflow:0 /opt/airflow

USER airflow
