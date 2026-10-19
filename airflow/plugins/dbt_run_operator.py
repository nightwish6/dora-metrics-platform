"""
DBT Run Operator - runs dbt models for a specific table.

This operator:
1. Reads JSON file path (from XCom or config)
2. Runs dbt model to load/transform data into DuckDB using dbtRunner
3. Returns structured result with success status and record count

If this operator fails, it is retried WITHOUT re-calling the API,
because the JSON file already exists on disk.
"""

from __future__ import annotations

import json
import os
from typing import Any

from config_loader import load_endpoint_config

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DbtRunOperator(BaseOperator):
    """
    Run dbt model for a specific table using dbtRunner API.

    Example usage::

        DbtRunOperator(
            task_id="run_dbt_pull_requests",
            table_name="pull_requests",
            repo_name="vscode",
            dbt_project_dir="/opt/airflow/dbt",
            raw_data_dir="/opt/airflow/data/raw",
            dag=dag,
        )
    """

    @apply_defaults
    def __init__(
        self,
        table_name: str,
        repo_name: str = "vscode",
        dbt_project_dir: str = "/opt/airflow/dbt",
        raw_data_dir: str = "/opt/airflow/data/raw",
        duckdb_path: str = "/opt/airflow/data/duckdb/data.duckdb",
        config_path: str | None = None,
        command: str = "run",
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = table_name
        self.repo_name = repo_name
        self.dbt_project_dir = dbt_project_dir
        self.raw_data_dir = raw_data_dir
        self.duckdb_path = duckdb_path
        self.config_path = config_path
        self.command = command

        # Load endpoint configuration (only for "run" command)
        if command == "run":
            self.endpoint_config = load_endpoint_config(table_name, config_path)
            if self.endpoint_config is None:
                raise ValueError(
                    f"No endpoint config found for table_name='{table_name}'. " f"Check config/github_incremental.yml"
                )

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """
        Execute dbt command using dbtRunner API.

        Supports:
        - "run": run dbt models
        - "test": run dbt tests

        Returns:
            Dict with status, record count, and dbt_model name.
        """
        # Set env vars so dbt finds profiles.yml and dbt_project.yml
        os.environ["DBT_PROFILES_DIR"] = self.dbt_project_dir
        os.environ["DBT_PROJECT_DIR"] = self.dbt_project_dir

        from dbt.cli.main import dbtRunner

        runner = dbtRunner()

        # ============================================================
        # Step 1: Run dbt tests
        # ============================================================
        if self.command == "test":
            self.log.info("Running dbt tests for raw layer...")

            result = runner.invoke(
                [
                    "test",
                ]
            )

            if not result.success:
                error_msg = result.exception if result.exception else "dbt test failed with unknown error"
                self.log.error("dbt test failed: %s", error_msg)
                raise RuntimeError(f"dbt test failed: {error_msg}")

            self.log.info("dbt tests completed successfully")
            return {
                "status": "success",
                "command": "test",
            }

        # ============================================================
        # Step 2: Run dbt models (default behavior)
        # ============================================================
        # Determine JSON file path
        json_file = None

        ti = context.get("ti")
        if ti:
            xcom_data = ti.xcom_pull(
                task_ids=f"load_{self.table_name}",
                key="return_value",
            )
            if xcom_data and isinstance(xcom_data, dict):
                json_file = xcom_data.get("json_file")

        if not json_file:
            json_file = os.path.join(self.raw_data_dir, f"{self.table_name}.json")

        if not os.path.exists(json_file):
            self.log.error(
                "JSON file not found: %s. API load may have failed.",
                json_file,
            )
            raise FileNotFoundError(f"JSON file not found: {json_file}. " f"Run the API load task first.")

        with open(json_file) as f:
            data = json.load(f)
            record_count = len(data) if isinstance(data, list) else 1

        self.log.info(
            "Running dbt for %s: %d records in %s",
            self.table_name,
            record_count,
            json_file,
        )

        dbt_model = f"raw_{self.table_name}"
        vars_json = {
            "json_path": json_file,
            "repo_name": self.repo_name,
        }

        result = runner.invoke(
            [
                "run",
                "--select",
                dbt_model,
                "--vars",
                json.dumps(vars_json),
            ],
        )

        if not result.success:
            error_msg = result.exception if result.exception else "dbt run failed with unknown error"
            self.log.error("dbt run failed: %s", error_msg)
            raise RuntimeError(f"dbt run failed: {error_msg}")

        self.log.info("dbt run completed successfully for model: %s", dbt_model)

        return {
            "count": record_count,
            "status": "success",
            "dbt_model": dbt_model,
            "json_file": json_file,
        }
