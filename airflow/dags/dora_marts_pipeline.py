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
    "dora_marts_pipeline",
    default_args=default_args,
    description="Run all dbt marts models sequentially",
    schedule_interval="@daily",
    catchup=False,
    tags=["dora", "marts"],
)

marts_models = [
    "core_cfr",
    "core_deployment_frequency",
    "core_lead_time",
    "core_time_to_restore",
    "supporting_flow",
    "supporting_health",
    "supporting_quality",
]

tasks = []
for model in marts_models:
    task = DbtRunOperator(
        task_id=f"run_dbt_{model}",
        dbt_model=f"3_marts.{model}",
        dbt_project_dir="/opt/airflow/dbt",
        dag=dag,
    )
    tasks.append(task)

# Link tasks sequentially
for i in range(len(tasks) - 1):
    tasks[i] >> tasks[i + 1]
