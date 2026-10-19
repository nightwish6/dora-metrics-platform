"""
GitHub to dbt Operator - Config-driven incremental loading.

This operator:
1. Reads config from github_incremental.yml
2. Connects to DuckDB to determine last_loaded_at
3. Fetches data from GitHub API with pagination
4. Saves raw JSON to files
5. Runs dbt models to load and transform data via dbtRunner
"""

from __future__ import annotations

import json
import os
from typing import Any

from config_loader import load_endpoint_config
from duckdb_hook import DuckDBHook
from github_hook import GitHubHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GitHubToDbtOperator(BaseOperator):
    """
    Fetch data from GitHub API and load into DuckDB via dbt.

    Reads configuration from config/github_incremental.yml to determine:
    - Which API endpoint to call
    - Whether to use incremental loading
    - Which field to use for since-based filtering

    Example usage::

        GitHubToDbtOperator(
            task_id="load_pull_requests",
            owner="microsoft",
            repo="vscode",
            table_name="pull_requests",
            dbt_project_dir="/opt/airflow/dbt",
            raw_data_dir="/opt/airflow/data/raw",
            duckdb_path="/opt/airflow/data/duckdb/data.duckdb",
            dag=dag,
        )
    """

    @apply_defaults
    def __init__(
        self,
        owner: str,
        repo: str,
        table_name: str,
        github_conn_id: str = "github_default",
        dbt_project_dir: str = "dbt",
        raw_data_dir: str = "data/raw",
        duckdb_path: str = "data/duckdb/data.duckdb",
        config_path: str | None = None,
        **kwargs,
    ) -> None:
        super().__init__(**kwargs)
        self.owner = owner
        self.repo = repo
        self.table_name = table_name
        self.github_conn_id = github_conn_id
        self.dbt_project_dir = dbt_project_dir
        self.raw_data_dir = raw_data_dir
        self.duckdb_path = duckdb_path
        self.config_path = config_path

        # Load endpoint configuration
        self.endpoint_config = load_endpoint_config(table_name, config_path)
        if self.endpoint_config is None:
            raise ValueError(
                f"No endpoint config found for table_name='{table_name}'. " f"Check config/github_incremental.yml"
            )

        # Extract config fields
        self.api_endpoint = self.endpoint_config.get("api_endpoint", "")
        self.api_params = self.endpoint_config.get("api_params", {})
        self.since_field = self.endpoint_config.get("since_field", "")
        self.api_since_param = self.endpoint_config.get("api_since_param")
        self.api_until_param = self.endpoint_config.get("api_until_param")
        self.incremental = self.endpoint_config.get("incremental", False)
        self.full_load_when_empty = self.endpoint_config.get("full_load_when_empty", False)

        # Will be set during execute()
        self.last_loaded_at: str | None = None

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """
        Execute the GitHub to dbt pipeline.

        Steps:
        1. Read config (already done in __init__)
        2. Connect to DuckDB and determine last_loaded_at
        3. Fetch data from GitHub API with pagination
        4. Save JSON to file
        5. Run dbt model via dbtRunner
        """
        # ============================================================
        # Step 1: Config is already loaded in __init__
        # ============================================================
        self.log.info(
            "Loading %s from %s/%s (incremental=%s)",
            self.table_name,
            self.owner,
            self.repo,
            self.incremental,
        )

        # ============================================================
        # Step 2: Connect to DuckDB and determine last_loaded_at
        # ============================================================
        last_loaded_at = None

        if self.incremental:
            self.log.info(
                "Checking DuckDB for last_loaded_at in raw.%s",
                self.table_name,
            )

            duckdb_hook = DuckDBHook(
                db_path=self.duckdb_path,
                schema="raw",
            )
            try:
                # Check if table exists
                if not duckdb_hook.table_exists(self.table_name):
                    self.log.info(
                        "Table raw.%s does not exist - full load",
                        self.table_name,
                    )
                    if self.full_load_when_empty:
                        last_loaded_at = None  # Full load
                    else:
                        self.log.warning(
                            "Table raw.%s does not exist and full_load_when_empty=False",
                            self.table_name,
                        )
                        return {
                            "count": 0,
                            "status": "skipped",
                            "reason": "table_not_exists",
                        }
                # Check if table is empty
                elif duckdb_hook.check_if_empty(self.table_name):
                    self.log.info(
                        "Table raw.%s is empty - full load",
                        self.table_name,
                    )
                    if self.full_load_when_empty:
                        last_loaded_at = None  # Full load
                    else:
                        self.log.warning(
                            "Table raw.%s is empty and full_load_when_empty=False",
                            self.table_name,
                        )
                        return {
                            "count": 0,
                            "status": "skipped",
                            "reason": "table_empty",
                        }
                # Table has data - get last_loaded_at
                else:
                    row_count = duckdb_hook.get_row_count(self.table_name)
                    self.log.info(
                        "Table raw.%s has %d rows - checking for incremental update",
                        self.table_name,
                        row_count,
                    )

                    last_loaded_at = duckdb_hook.get_last_loaded_at(
                        table_name=self.table_name,
                        since_field=self.since_field,
                    )

                    if last_loaded_at:
                        self.log.info("Found last_loaded_at: %s", last_loaded_at)
                    else:
                        self.log.warning("Could not extract last_loaded_at - falling back to full load")
                        if self.full_load_when_empty:
                            last_loaded_at = None
                        else:
                            return {
                                "count": 0,
                                "status": "skipped",
                                "reason": "no_last_loaded_at",
                            }
            finally:
                duckdb_hook.close()
        else:
            self.log.info("Full load mode (incremental=False) - skipping last_loaded_at check")

        # Save last_loaded_at for use in pagination
        self.last_loaded_at = last_loaded_at

        # ============================================================
        # Step 3: Fetch data from GitHub API with pagination
        # ============================================================
        github_hook = GitHubHook(github_conn_id=self.github_conn_id)

        # Build API params from config
        api_params = dict(self.api_params) if self.api_params else {}

        # Add since parameter for incremental loads
        if self.incremental and self.api_since_param and last_loaded_at:
            api_params[self.api_since_param] = last_loaded_at
            self.log.info(
                "Using %s=%s for incremental load",
                self.api_since_param,
                last_loaded_at,
            )

            # For commits: also add until param (date range)
            if self.api_until_param:
                api_params[self.api_until_param] = "now()"
                self.log.info(
                    "Using %s=now() for incremental load",
                    self.api_until_param,
                )

        # Build full endpoint URL
        endpoint = self.api_endpoint.format(
            owner=self.owner,
            repo=self.repo,
        )

        self.log.info("Fetching from GitHub API: %s", endpoint)

        # Call GitHub API with pagination
        data = github_hook.get(
            endpoint,
            params=api_params if api_params else None,
            paginate=True,
            since_param=last_loaded_at if (self.incremental and self.api_since_param) else None,
        )

        if not data:
            self.log.warning("No data returned from GitHub API")
            return {"count": 0, "status": "empty"}

        if isinstance(data, dict):
            data = [data]

        self.log.info("Fetched %d records from GitHub API", len(data))

        # ============================================================
        # Step 4: Save JSON to file
        # ============================================================
        os.makedirs(self.raw_data_dir, exist_ok=True)
        json_file = os.path.join(self.raw_data_dir, f"{self.table_name}.json")

        self.log.info("Saving %d records to %s", len(data), json_file)
        with open(json_file, "w") as f:
            json.dump(data, f, indent=2, default=str)

        # ============================================================
        # Step 5: Run dbt model via dbtRunner
        # ============================================================
        dbt_model = f"raw_{self.table_name}"
        vars_json = {
            "json_path": json_file,
            "repo_name": self.repo,
        }

        # Set env vars so dbt finds profiles.yml and dbt_project.yml
        os.environ["DBT_PROFILES_DIR"] = self.dbt_project_dir
        os.environ["DBT_PROJECT_DIR"] = self.dbt_project_dir

        from dbt.cli.main import dbtRunner

        runner = dbtRunner()
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
            "count": len(data),
            "status": "success",
            "dbt_model": dbt_model,
            "json_file": json_file,
            "last_loaded_at": last_loaded_at,
            "incremental": self.incremental,
        }
