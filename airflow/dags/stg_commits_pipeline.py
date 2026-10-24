from datetime import datetime, timedelta

from dbt_run_operator import DbtRunOperator

from airflow import DAG

default_args = {
    "owner": "dora-metrics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 10, 1),
}

dag = DAG(
    "dora_stg_commits",
    default_args=default_args,
    description="Run dbt staging model for commits (stg_commits)",
    schedule_interval="@daily",
    catchup=False,
    tags=["dora", "staging", "commits"],
)

run_dbt_stg_commits = DbtRunOperator(
    task_id="run_dbt_stg_commits",
    dbt_model="stg_commits",
    dbt_project_dir="/opt/airflow/dbt",
    dag=dag,
)
