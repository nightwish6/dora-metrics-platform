"""
DBT Run Operator - runs dbt models for a specific table.

This operator:
1. Reads JSON file path (from XCom or config)
2. Runs dbt model to load/transform data into DuckDB using dbtRunner
3. Returns structured result with success status and record count

If this operator fails, it is retried WITHOUT re-calling the API,
because the JSON file already exists on disk.

For staging models, if dbt fails with OOM, falls back to direct
Python/DuckDB batch loading from raw table.
"""

from __future__ import annotations

import json
import os
from typing import Any

from config_loader import load_endpoint_config
from staging_loader import StagingDataLoader

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# OOM error patterns to detect
OOM_ERROR_PATTERNS = [
    "out of memory",
    "OutOfMemory",
    "MEMORY_LIMIT_EXCEEDED",
    "heap space",
    "unable to allocate",
    "capacity",
    "duckdb::OutOfMemoryException",
    "MemoryError",
]


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

        # Fallback mode for staging models (triggered on OOM)
        self.fallback_mode = False

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

    # ================================================================
    # OOM Detection
    # ================================================================

    def _detect_oom_error(self, exception: Exception | str) -> bool:
        """
        Detect if the error is an Out of Memory error.

        Args:
            exception: The exception object or error message string

        Returns:
            True if OOM detected, False otherwise
        """
        error_msg = str(exception).lower()
        return any(pattern.lower() in error_msg for pattern in OOM_ERROR_PATTERNS)

    # ================================================================
    # Fallback Loader (uses StagingDataLoader)
    # ================================================================

    def _load_staging_fallback(self, model_name: str) -> dict[str, Any]:
        """
        Load staging data from raw table using batch processing.

        This is used as a fallback when dbt fails with OOM during
        JSON flattening.

        Args:
            model_name: Staging model name (e.g. 'stg_commits')

        Returns:
            Dict with loading statistics
        """
        self.log.info("Starting staging fallback loader for model: %s", model_name)

        loader = StagingDataLoader(
            model_name=model_name,
            duckdb_path=self.duckdb_path,
            config_path=self.config_path,
            log_callback=self.log.info,
        )

        return loader.load()

    # ================================================================
    # Execute
    # ================================================================

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

            # ── Try dbt run with OOM detection ────────────────────
            dbt_exception = None
            result = None

            try:
                result = runner.invoke(
                    [
                        "run",
                        "--select",
                        self.dbt_model,
                    ]
                )
            except Exception as exc:
                dbt_exception = exc
                self.log.error("dbtRunner.invoke raised exception: %s", exc)

            # Check if dbt succeeded
            if result and result.success:
                self.log.info("dbt run completed successfully for model: %s", self.dbt_model)
                return {
                    "status": "success",
                    "dbt_model": self.dbt_model,
                }

            # ── OOM detection and fallback ────────────────────────
            # Try multiple sources for error message:
            # 1. Exception from try-except
            # 2. result.exception
            # 3. result.response / result.result (if available)
            error_msg = None
            debug_info = {}

            if dbt_exception:
                error_msg = str(dbt_exception)
                debug_info["exception_type"] = type(dbt_exception).__name__

            if result:
                debug_info["result_success"] = result.success
                debug_info["has_result"] = result.result is not None if hasattr(result, "result") else False

                if not error_msg and result.exception:
                    error_msg = str(result.exception)

                # Try to get error from result.response or result.result
                if not error_msg:
                    for attr in ["response", "result"]:
                        val = getattr(result, attr, None)
                        if val:
                            val_str = str(val)
                            if "error" in val_str.lower() or "error" in type(val).__name__.lower():
                                error_msg = val_str
                                debug_info[f"from_{attr}"] = True
                                break

                # --- Parse result.result for staging models ---
                # result.result is RunExecutionResult containing RunResult
                if not error_msg and self.dbt_model and self.dbt_model.startswith("stg_"):
                    if hasattr(result, "result") and result.result:
                        # RunExecutionResult.results is a list of RunResult
                        results = getattr(result.result, "results", [])
                        if isinstance(results, list):
                            for r in results:
                                msg = getattr(r, "message", None)
                                if msg and "error" in str(msg).lower():
                                    error_msg = str(msg)
                                    debug_info["from_result_results"] = True
                                    break

            if not error_msg:
                error_msg = "dbt run failed (no exception message available)"

            self.log.debug(
                "dbt run debug info for '%s': %s",
                self.dbt_model,
                debug_info,
            )

            self.log.warning(
                "dbt run failed for model '%s'. Error: %s",
                self.dbt_model,
                error_msg,
            )

            is_oom = self._detect_oom_error(error_msg)

            # For staging models with unknown error, assume OOM
            # and try fallback. This is safe because fallback simply
            # reloads data from the raw layer.
            if not is_oom and self.dbt_model.startswith("stg_"):
                unknown_patterns = [
                    "no exception message",
                    "unknown error",
                    "failed (no exception",
                ]
                is_oom = any(p in error_msg.lower() for p in unknown_patterns)
                if is_oom:
                    self.log.warning(
                        "Unknown error for staging model '%s'. " "Assuming OOM and triggering fallback...",
                        self.dbt_model,
                    )

            if is_oom:
                self.log.warning(
                    "OOM detected for model '%s'. Triggering fallback loader...",
                    self.dbt_model,
                )
                if self.dbt_model.startswith("stg_"):
                    try:
                        fallback_result = self._load_staging_fallback(self.dbt_model)
                        return fallback_result
                    except Exception as fallback_error:
                        self.log.error(
                            "Fallback loader failed for '%s': %s",
                            self.dbt_model,
                            fallback_error,
                        )
                        raise RuntimeError(f"Fallback loader failed: {fallback_error}") from fallback_error
                else:
                    # Not a staging model, re-raise original error
                    self.log.error("dbt run failed (non-staging model): %s", error_msg)
                    raise RuntimeError(f"dbt run failed: {error_msg}") from dbt_exception

            # Non-OOM error, re-raise
            raise RuntimeError(f"dbt run failed: {error_msg}") from dbt_exception

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
