"""
Load GitHub Actions History DAG (Single Task with Pagination)

Один таск, который выгружает всю историю workflow_runs с 2015 года
используя нормальный оператор GitHubHook из плагина.
В базу данные НЕ отправляются, сохраняются в JSON файлы.
"""

import os
from datetime import datetime, timedelta

from github_hook import GitHubHook

from airflow import DAG
from airflow.operators.python import PythonOperator

# Конфиг
GITHUB_OWNER = "microsoft"
GITHUB_REPO = "vscode"

# Папка для сохранения файлов
EXPORT_DIR = "/opt/airflow/data/raw/github_actions"


def load_full_history(**kwargs):
    """
    Один таск, который выгружает ВСЮ историю через пагинацию.
    GitHub API actions/runs не поддерживает 'created' как start-date filter,
    поэтому берём все записи без фильтра.
    """
    import json

    hook = GitHubHook()
    print("🚀 Начинаем полную выгрузку workflow_runs (без date filter)")

    data = hook.get(
        f"/repos/{GITHUB_OWNER}/{GITHUB_REPO}/actions/runs",
        paginate=True,
        data_array_key="workflow_runs",
        max_records=2000000,
    )

    if not data:
        print("❌ Данные не получены")
        return

    # Сохраняем в JSON файл
    os.makedirs(EXPORT_DIR, exist_ok=True)
    filepath = f"{EXPORT_DIR}/{GITHUB_OWNER}_{GITHUB_REPO}_full_history.json"
    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    print(f"✅ Выгружено {len(data)} записей в {filepath}")


# DAG
with DAG(
    dag_id="load_github_actions_history",
    description="Выгрузка полной истории GitHub Actions (1 таск, пагинация с 2019-11-13)",
    schedule=None,
    start_date=datetime(2019, 11, 13),
    catchup=False,
    dagrun_timeout=timedelta(hours=24),
    default_args={
        "owner": "airflow",
        "retries": 1,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    load_full_task = PythonOperator(
        task_id="load_workflow_runs_2015_to_today",
        python_callable=load_full_history,
        provide_context=True,
    )
