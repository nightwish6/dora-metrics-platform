"""
GitHub API Load Operator - fetches data and saves to JSON.

This operator:
1. Reads config from github_incremental.yml
2. Connects to DuckDB to determine last_loaded_at (MAX query)
3. Fetches data from GitHub API with pagination
4. Saves raw JSON to file

If this operator fails, the task is retried from the beginning (API call).
"""

from __future__ import annotations

import json
import os
from datetime import UTC, datetime
from typing import Any

from config_loader import load_endpoint_config
from duckdb_hook import DuckDBHook
from github_hook import GitHubHook

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class GitHubApiLoadOperator(BaseOperator):
    """
    Fetch data from GitHub API and save to JSON file.

    Example usage::

        GitHubApiLoadOperator(
            task_id="load_pull_requests",
            owner="microsoft",
            repo="vscode",
            table_name="pull_requests",
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
        self.data_array_key = self.endpoint_config.get("data_array_key")
        self.incremental = self.endpoint_config.get("incremental", False)
        self.full_load_when_empty = self.endpoint_config.get("full_load_when_empty", False)

    def execute(self, context: dict[str, Any]) -> dict[str, Any]:
        """
        Execute API load.

        Returns:
            Dict with json_file path and record count (pushed to XCom).
        """
        self.log.info(
            "Loading %s from %s/%s (incremental=%s)", self.table_name, self.owner, self.repo, self.incremental
        )

        # ============================================================
        # Step 1: Connect to DuckDB and determine last_loaded_at
        #         (MAX query — always fresh)
        # ============================================================
        last_loaded_at = None

        if self.incremental:
            self.log.info("Checking DuckDB for last_loaded_at in raw.%s", self.table_name)

            with DuckDBHook(
                db_path=self.duckdb_path,
                schema="raw",
            ) as duckdb_hook:
                # Check if table exists
                if not duckdb_hook.table_exists(self.table_name):
                    self.log.info("Table raw.%s does not exist - full load", self.table_name)
                    if self.full_load_when_empty:
                        last_loaded_at = None
                    else:
                        self.log.warning("Table raw.%s does not exist and full_load_when_empty=False", self.table_name)
                        return {
                            "json_file": None,
                            "count": 0,
                            "status": "skipped",
                            "reason": "table_not_exists",
                        }
                # Check if table is empty
                elif duckdb_hook.check_if_empty(self.table_name):
                    self.log.info("Table raw.%s is empty - full load", self.table_name)
                    if self.full_load_when_empty:
                        last_loaded_at = None
                    else:
                        self.log.warning("Table raw.%s is empty and full_load_when_empty=False", self.table_name)
                        return {
                            "json_file": None,
                            "count": 0,
                            "status": "skipped",
                            "reason": "table_empty",
                        }
                # Table has data - get last_loaded_at (fresh MAX query)
                else:
                    row_count = duckdb_hook.get_row_count(self.table_name)
                    self.log.info(
                        "Table raw.%s has %d rows - checking for incremental update", self.table_name, row_count
                    )

                    last_loaded_at = duckdb_hook.get_last_loaded_at(
                        table_name=self.table_name,
                        since_field=self.since_field,
                    )

                    self.log.info(
                        "get_last_loaded_at result: table=%s, since_field=%s, value=%s",
                        self.table_name,
                        self.since_field,
                        last_loaded_at,
                    )

                    if last_loaded_at:
                        self.log.info("Found last_loaded_at: %s", last_loaded_at)
                    else:
                        self.log.warning("Could not extract last_loaded_at - falling back to full load")
                        if self.full_load_when_empty:
                            last_loaded_at = None
                        else:
                            return {
                                "json_file": None,
                                "count": 0,
                                "status": "skipped",
                                "reason": "no_last_loaded_at",
                            }
        else:
            self.log.info("Full load mode (incremental=False) - skipping last_loaded_at check")

        # ============================================================
        # Step 2: Fetch data from GitHub API with pagination
        # ============================================================
        github_hook = GitHubHook(github_conn_id=self.github_conn_id)

        # Build API params from config
        api_params = dict(self.api_params) if self.api_params else {}

        # Add since parameter for incremental loads
        since_value = None
        until_value = None
        if self.incremental and self.api_since_param and last_loaded_at:
            # GitHub API 'created' param accepts only date (YYYY-MM-DD), not full timestamp
            if self.api_since_param == "created":
                since_value = last_loaded_at[:10]
            else:
                since_value = last_loaded_at
            api_params[self.api_since_param] = since_value
            self.log.info("Using %s=%s for incremental load", self.api_since_param, since_value)

        # Add until parameter (always "now()" for date-range queries like commits)
        if self.api_until_param:
            until_value = datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")
            api_params[self.api_until_param] = until_value
            self.log.info("Using %s=%s for incremental load", self.api_until_param, until_value)

        # Build full endpoint URL
        endpoint = self.api_endpoint.format(
            owner=self.owner,
            repo=self.repo,
        )

        self.log.info("Fetching from GitHub API: %s", endpoint)

        # Call GitHub API with pagination
        self.log.info("Calling github_hook.get: data_array_key=%s", self.data_array_key)
        data = github_hook.get(
            endpoint,
            params=api_params if api_params else None,
            paginate=True,
            since_param_name=self.api_since_param if self.api_since_param else "since",
            since_param_value=since_value if self.incremental and self.api_since_param else None,
            until_param_name=self.api_until_param if self.api_until_param else "until",
            until_param_value=until_value if self.api_until_param else None,
            data_array_key=self.data_array_key,
        )

        if not data:
            self.log.warning("No data returned from GitHub API")
            return {
                "json_file": None,
                "count": 0,
                "status": "empty",
            }

        if isinstance(data, dict):
            data = [data]

        self.log.info("Fetched %d records from GitHub API", len(data))

        # ============================================================
        # Step 3: Save JSON to file
        # ============================================================
        os.makedirs(self.raw_data_dir, exist_ok=True)
        json_file = os.path.join(self.raw_data_dir, f"{self.table_name}.json")

        self.log.info("Saving %d records to %s", len(data), json_file)
        with open(json_file, "w") as f:
            json.dump(data, f, indent=2, default=str)

        self.log.info("API load complete: %d records saved to %s", len(data), json_file)

        return {
            "json_file": json_file,
            "count": len(data),
            "status": "success",
        }
