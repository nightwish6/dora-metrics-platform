"""
Staging model configuration loader.

Loads staging model definitions from config/staging_models.yml
and provides column structure for the fallback batch loader.

Example YAML structure:
    staging_models:
      stg_commits:
        raw_table: raw_commits
        unique_key: id
        batch_size: 5000
        columns:
          - name: id
            type: md5
            raw_column: sha
          - name: sha
            type: json_extract
            json_path: "$.sha"
          - name: commit_date
            type: json_extract_typed
            json_path: "$.commit.date"
            sql_type: TIMESTAMP
          - name: repo_name
            type: passthrough
          - name: loaded_at
            type: static
            expression: "CURRENT_TIMESTAMP"

Column types:
    - json_extract:      data->>'$.path'  → varchar
    - json_extract_typed: try_cast(data->>'$.path' as TYPE)
    - md5:               md5(CAST(t.raw_column AS VARCHAR))
    - passthrough:       copy column as-is from raw table
    - static:            fixed expression (e.g. CURRENT_TIMESTAMP)
"""

from __future__ import annotations

import os
from typing import Any

import yaml

# Config file search paths
DEFAULT_CONFIG_PATHS = [
    "config/staging_models.yml",
    "/opt/airflow/config/staging_models.yml",
    "/opt/airflow/configs/staging_models.yml",
]


def _find_config_path() -> str:
    """Find the first existing config file from known locations."""
    for path in DEFAULT_CONFIG_PATHS:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(f"Staging config file not found. Searched: {DEFAULT_CONFIG_PATHS}")


def load_staging_model_config(
    model_name: str,
    config_path: str | None = None,
) -> dict[str, Any] | None:
    """
    Load configuration for a specific staging model.

    Args:
        model_name: Staging model name (e.g. 'stg_commits')
        config_path: Path to YAML config. Auto-detected if None.

    Returns:
        Dict with keys: raw_table, unique_key, batch_size, columns
        or None if model not found.
    """
    if config_path is None:
        config_path = _find_config_path()

    if not os.path.exists(config_path):
        return None

    with open(config_path) as f:
        config = yaml.safe_load(f)

    staging_models = config.get("staging_models", {})
    return staging_models.get(model_name)


def build_insert_sql(
    model_name: str,
    config_path: str | None = None,
) -> tuple[str, list[str]]:
    """
    Build INSERT SQL statement from staging model config.

    Args:
        model_name: Staging model name (e.g. 'stg_commits')
        config_path: Optional path to config file

    Returns:
        Tuple of (INSERT SQL string, list of column names)

    Example:
        sql, cols = build_insert_sql("stg_commits")
        # sql = "INSERT INTO stg_commits (id, repo_name, ...) VALUES (?, ?, ...)"
        # cols = ["id", "repo_name", ...]
    """
    staging_config = load_staging_model_config(model_name, config_path)
    if not staging_config:
        raise ValueError(f"Staging model '{model_name}' not found in config")

    columns = staging_config["columns"]
    col_names = [col["name"] for col in columns]
    placeholders = ", ".join(["?"] * len(col_names))
    cols_str = ", ".join(col_names)

    # Use schema-qualified table name
    table_name = model_name.replace("stg_", "staging.")
    if table_name == model_name:
        table_name = f"staging.{model_name}"

    sql = f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})"

    return sql, col_names


def get_staging_columns(model_name: str, config_path: str | None = None) -> list[dict]:
    """
    Get column definitions for a staging model.

    Args:
        model_name: Staging model name
        config_path: Optional config path

    Returns:
        List of column definition dicts
    """
    staging_config = load_staging_model_config(model_name, config_path)
    if not staging_config:
        raise ValueError(f"Staging model '{model_name}' not found in config")

    return staging_config["columns"]


def get_unique_key(model_name: str, config_path: str | None = None) -> str:
    """
    Get unique key for a staging model.

    Args:
        model_name: Staging model name
        config_path: Optional config path

    Returns:
        Unique key column name (e.g. 'id')
    """
    staging_config = load_staging_model_config(model_name, config_path)
    if not staging_config:
        raise ValueError(f"Staging model '{model_name}' not found in config")

    return staging_config["unique_key"]


def get_batch_size(model_name: str, config_path: str | None = None) -> int:
    """
    Get batch size for a staging model.

    Args:
        model_name: Staging model name
        config_path: Optional config path

    Returns:
        Batch size (default 5000)
    """
    staging_config = load_staging_model_config(model_name, config_path)
    if not staging_config:
        raise ValueError(f"Staging model '{model_name}' not found in config")

    return staging_config.get("batch_size", 5000)


def get_raw_table(model_name: str, config_path: str | None = None) -> str:
    """
    Get raw table name for a staging model.

    Args:
        model_name: Staging model name
        config_path: Optional config path

    Returns:
        Raw table name (e.g. 'raw_commits')
    """
    staging_config = load_staging_model_config(model_name, config_path)
    if not staging_config:
        raise ValueError(f"Staging model '{model_name}' not found in config")

    return staging_config["raw_table"]
