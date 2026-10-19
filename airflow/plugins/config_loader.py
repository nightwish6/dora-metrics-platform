"""
Configuration loader for GitHub incremental loads.

Reads github_incremental.yml and provides endpoint configurations
for each GitHub API resource type.
"""

from __future__ import annotations

import os
from typing import Any

import yaml

# Default config file path — search multiple locations
DEFAULT_CONFIG_PATHS = [
    "config/github_incremental.yml",  # Relative to CWD
    "/opt/airflow/config/github_incremental.yml",
    "/opt/airflow/configs/github_incremental.yml",
    "/opt/airflow/plugins/config/github_incremental.yml",
]


def _find_config_path() -> str:
    """Find the first existing config file from known locations."""
    for path in DEFAULT_CONFIG_PATHS:
        if os.path.exists(path):
            return path
    raise FileNotFoundError(f"Config file not found. Searched: {DEFAULT_CONFIG_PATHS}")


def load_endpoint_config(
    table_name: str,
    config_path: str | None = None,
) -> dict[str, Any] | None:
    """
    Load configuration for a specific endpoint by table_name.

    Args:
        table_name: Target table name (e.g. 'pull_requests', 'issues')
        config_path: Path to the YAML config file. Auto-detected if None.

    Returns:
        Endpoint configuration dict, or None if not found
    """
    if config_path is None:
        config_path = _find_config_path()

    endpoints = _load_all_endpoints(config_path)

    # Deduplicate by table_name (last occurrence wins)
    endpoint_map = {}
    for ep in endpoints:
        endpoint_map[ep["table_name"]] = ep

    return endpoint_map.get(table_name)


def load_all_endpoints(
    config_path: str | None = None,
) -> list[dict[str, Any]]:
    """
    Load all endpoint configurations from the YAML file.

    Args:
        config_path: Path to the YAML config file. Auto-detected if None.

    Returns:
        List of endpoint configuration dicts
    """
    if config_path is None:
        config_path = _find_config_path()
    return _load_all_endpoints(config_path)


def _load_all_endpoints(
    config_path: str,
) -> list[dict[str, Any]]:
    """
    Load all endpoints from the YAML config file.

    Args:
        config_path: Path to the YAML config file

    Returns:
        List of endpoint configuration dicts
    """
    if not os.path.exists(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path) as f:
        config = yaml.safe_load(f)

    endpoints = config.get("endpoints", [])
    return endpoints


def get_endpoint_field(
    table_name: str,
    field: str,
    config_path: str | None = None,
) -> Any:
    """
    Get a specific field from an endpoint configuration.

    Args:
        table_name: Target table name
        field: Field name to retrieve (e.g. 'api_endpoint', 'since_field')
        config_path: Path to the YAML config file

    Returns:
        Field value, or None if not found
    """
    endpoint = load_endpoint_config(table_name, config_path)
    if endpoint is None:
        return None
    return endpoint.get(field)
