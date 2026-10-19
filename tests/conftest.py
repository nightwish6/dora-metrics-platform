"""Pytest fixtures for tests."""

import os
import tempfile

import pytest
import yaml


@pytest.fixture
def sample_config():
    """Sample github_incremental.yml content."""
    return {
        "endpoints": [
            {
                "table_name": "pull_requests",
                "api_endpoint": "/repos/{owner}/{repo}/pulls",
                "api_params": {"state": "closed", "base": "main"},
                "since_field": "data->>'updated_at'",
                "api_since_param": "since",
                "incremental": True,
                "full_load_when_empty": True,
            },
            {
                "table_name": "issues",
                "api_endpoint": "/repos/{owner}/{repo}/issues",
                "api_params": {"state": "closed"},
                "since_field": "data->>'updated_at'",
                "api_since_param": "since",
                "incremental": True,
                "full_load_when_empty": True,
            },
            {
                "table_name": "releases",
                "api_endpoint": "/repos/{owner}/{repo}/releases",
                "api_params": {"sort": "created"},
                "since_field": "data->>'published_at'",
                "api_since_param": None,
                "incremental": False,
                "full_load_when_empty": False,
            },
        ]
    }


@pytest.fixture
def config_file(sample_config):
    """Create a temporary YAML config file."""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
        yaml.dump(sample_config, f)
        temp_path = f.name

    yield temp_path

    os.unlink(temp_path)


@pytest.fixture
def sample_pr_json():
    """Sample pull request JSON data."""
    return [
        {
            "id": 1,
            "title": "Fix bug",
            "state": "closed",
            "updated_at": "2024-01-15T10:00:00Z",
            "created_at": "2024-01-14T10:00:00Z",
            "merged_at": "2024-01-15T12:00:00Z",
        },
        {
            "id": 2,
            "title": "Add feature",
            "state": "closed",
            "updated_at": "2024-01-16T10:00:00Z",
            "created_at": "2024-01-15T10:00:00Z",
            "merged_at": "2024-01-16T14:00:00Z",
        },
    ]
