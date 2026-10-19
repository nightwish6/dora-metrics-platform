from datetime import datetime, timedelta

from config_loader import load_all_endpoints
from dbt_run_operator import DbtRunOperator
from github_api_load_operator import GitHubApiLoadOperator

from airflow import DAG

default_args = {
    "owner": "dora-metrics",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "start_date": datetime(2026, 10, 1),
}

dag = DAG(
    "dora_github_pipeline",
    default_args=default_args,
    description="Load raw data from GitHub API into DuckDB via dbt",
    schedule_interval="@daily",
    catchup=False,
    tags=["dora", "github", "raw-data"],
)

# Load endpoint configurations from YAML
endpoints = load_all_endpoints()

# Deduplicate by table_name (keep last occurrence)
seen = {}
for ep in endpoints:
    seen[ep["table_name"]] = ep
endpoints = list(seen.values())

# Create tasks dynamically from config
#
# Architecture:
#   1. load_* tasks — all run in parallel
#   2. run_dbt_raw_* tasks — all run sequentially, BUT only after ALL load tasks complete
#
# load_repo_metadata ──┐
# load_releases ───────┤
# load_issues ─────────┤
# load_pull_requests ──┤
# load_commits ────────┼──→ run_dbt_repo_metadata ─→ run_dbt_releases ─→ ...
# load_workflow_runs ──┘
# load_milestones ─────┘

load_tasks = []
dbt_tasks = []

for endpoint in endpoints:
    table_name = endpoint["table_name"]

    # Task 1: Load from API (run in parallel)
    load_task = GitHubApiLoadOperator(
        task_id=f"load_{table_name}",
        owner="microsoft",
        repo="vscode",
        table_name=table_name,
        raw_data_dir="/opt/airflow/data/raw",
        duckdb_path="/opt/airflow/data/duckdb/data.duckdb",
        dag=dag,
    )
    load_tasks.append(load_task)

    # Task 2: Run dbt raw layer (run sequentially after all load tasks)
    dbt_task = DbtRunOperator(
        task_id=f"run_dbt_raw_{table_name}",
        table_name=table_name,
        repo_name="vscode",
        dbt_project_dir="/opt/airflow/dbt",
        raw_data_dir="/opt/airflow/data/raw",
        dag=dag,
    )
    dbt_tasks.append(dbt_task)

# All load tasks run in parallel

# All run_dbt_raw tasks run sequentially
for i in range(len(dbt_tasks) - 1):
    dbt_tasks[i] >> dbt_tasks[i + 1]

# All load tasks must complete before the first run_dbt_raw
if load_tasks and dbt_tasks:
    for load_task in load_tasks:
        load_task >> dbt_tasks[0]

# Run dbt tests after all raw tables are loaded
run_dbt_tests = DbtRunOperator(
    task_id="run_dbt_tests",
    table_name="all",
    command="test",
    dbt_project_dir="/opt/airflow/dbt",
    dag=dag,
)

# Tests run after the last raw table (milestones)
dbt_tasks[-1] >> run_dbt_tests
