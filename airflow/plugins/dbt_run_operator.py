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

    Supports two modes:

    1. **Raw mode** (default): reads JSON file, runs `raw_{table_name}`, passes JSON path via vars.
       Used for `raw` models.

    2. **Generic mode**: runs any dbt model by name (e.g. `stg_commits`).
       No JSON file needed. Used for `staged`, `marts`, `metrics`, etc.

    Example usage — raw mode::

        DbtRunOperator(
            task_id="run_dbt_raw_commits",
            table_name="commits",
            repo_name="vscode",
            dbt_project_dir="/opt/airflow/dbt",
            raw_data_dir="/opt/airflow/data/raw",
            dag=dag,
        )

    Example usage — generic mode (staging, marts, etc.)::

        DbtRunOperator(
            task_id="run_stg_commits",
            dbt_model="stg_commits",  # ← specify model name directly
            dag=dag,
        )
    """

    @apply_defaults
    def __init__(
        self,
        table_name: str | None = None,
        dbt_model: str | None = None,
        repo_name: str = "vscode",
        dbt_project_dir: str = "/opt/airflow/dbt",
        raw_data_dir: str = "/opt/airflow/data/raw",
        duckdb_path: str = "/opt/airflow/data/duckdb/data.duckdb",
        config_path: str | None = None,
        command: str = "run",
        dbt_layer: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)

        # Determine mode: generic mode if dbt_model is provided, else raw mode
        self.generic_mode = dbt_model is not None
        self.table_name = table_name
        self.dbt_model = dbt_model
        self.repo_name = repo_name
        self.dbt_project_dir = dbt_project_dir
        self.raw_data_dir = raw_data_dir
        self.duckdb_path = duckdb_path
        self.config_path = config_path
        self.command = command
        self.dbt_layer = dbt_layer

        # Load endpoint configuration only for raw mode
        if command == "run" and not self.generic_mode:
            if not self.table_name:
                raise ValueError("table_name is required when dbt_model is not specified")
            self.endpoint_config = load_endpoint_config(self.table_name, config_path)
            if self.endpoint_config is None:
                raise ValueError(
                    f"No endpoint config found for table_name='{self.table_name}'. "
                    f"Check config/github_incremental.yml"
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
            self.log.info("Running dbt tests...")

            # If dbt_layer is specified, run tests for all models in that layer
            if self.dbt_layer:
                layer_models = {
                    "raw": "raw_commits raw_issues raw_milestones raw_pull_requests raw_releases raw_workflow_runs",
                    "staged": "stg_commits",
                    "marts": "",
                    "metrics": "",
                }
                models = layer_models.get(self.dbt_layer)
                if models:
                    self.log.info("Running tests for layer '%s': %s", self.dbt_layer, models)
                    result = runner.invoke(["test", "--models"] + models.split())
                else:
                    self.log.info("Running all tests")
                    result = runner.invoke(["test"])
            else:
                self.log.info("Running all tests")
                result = runner.invoke(["test"])

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
        # Step 2: Run dbt models
        # ============================================================

        # --- Generic mode (staging, marts, etc.) ---
        if self.generic_mode:
            self.log.info("Running dbt model in generic mode: %s", self.dbt_model)

            result = runner.invoke(
                [
                    "run",
                    "--select",
                    self.dbt_model,
                ]
            )

            if not result.success:
                error_msg = result.exception if result.exception else "dbt run failed with unknown error"
                self.log.error("dbt run failed: %s", error_msg)
                raise RuntimeError(f"dbt run failed: {error_msg}")

            self.log.info("dbt run completed successfully for model: %s", self.dbt_model)
            return {
                "status": "success",
                "dbt_model": self.dbt_model,
            }

        # --- Raw mode (reads JSON file) ---
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

        try:
            with open(json_file) as f:
                data = json.load(f)
                record_count = len(data) if isinstance(data, list) else 1
        except json.JSONDecodeError as e:
            self.log.error(
                "JSON decode error in %s: %s (line %d, column %d)",
                json_file,
                e.msg,
                e.lineno,
                e.colno,
            )
            try:
                with open(json_file, errors="replace") as f:
                    lines = f.readlines()
                    line_start = max(0, e.lineno - 3)
                    line_end = min(len(lines), e.lineno + 2)
                    self.log.error(
                        "Problematic context (lines %d-%d):",
                        line_start + 1,
                        line_end,
                    )
                    for i in range(line_start, line_end):
                        self.log.error("  %d: %s", i + 1, lines[i].rstrip())
            except Exception:
                self.log.error("Could not read problematic context from file")
            raise

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
