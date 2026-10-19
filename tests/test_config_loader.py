"""Tests for config_loader.py"""

import os

import pytest

from airflow.plugins.config_loader import (
    _find_config_path,
    _load_all_endpoints,
    get_endpoint_field,
    load_all_endpoints,
    load_endpoint_config,
)


class TestFindConfigPath:
    """Test _find_config_path function."""

    def test_finds_existing_config(self, config_file):
        """Should find the first existing config file."""
        # Save original and patch
        import airflow.plugins.config_loader as loader

        original = loader.DEFAULT_CONFIG_PATHS
        loader.DEFAULT_CONFIG_PATHS = [config_file]

        try:
            result = _find_config_path()
            assert result == config_file
        finally:
            loader.DEFAULT_CONFIG_PATHS = original

    def test_raises_when_no_config_exists(self):
        """Should raise FileNotFoundError when no config is found."""
        import airflow.plugins.config_loader as loader

        original = loader.DEFAULT_CONFIG_PATHS
        loader.DEFAULT_CONFIG_PATHS = ["/nonexistent/path/config.yml"]

        try:
            with pytest.raises(FileNotFoundError):
                _find_config_path()
        finally:
            loader.DEFAULT_CONFIG_PATHS = original


class TestLoadEndpoints:
    """Test endpoint loading functions."""

    def test_load_all_endpoints(self, config_file):
        """Should load all endpoints from config file."""
        endpoints = _load_all_endpoints(config_file)
        assert len(endpoints) == 3
        assert endpoints[0]["table_name"] == "pull_requests"
        assert endpoints[1]["table_name"] == "issues"
        assert endpoints[2]["table_name"] == "releases"

    def test_load_all_endpoints_invalid_file(self):
        """Should raise FileNotFoundError for non-existent file."""
        with pytest.raises(FileNotFoundError):
            _load_all_endpoints("/nonexistent/config.yml")

    def test_load_all_endpoints_empty_config(self):
        """Should return empty list when no endpoints in config."""
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("endpoints: []\n")
            temp_path = f.name

        try:
            endpoints = _load_all_endpoints(temp_path)
            assert endpoints == []
        finally:
            os.unlink(temp_path)

    def test_load_all_endpoints_missing_endpoints_key(self):
        """Should return empty list when endpoints key is missing."""
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=".yml", delete=False) as f:
            f.write("other_key: value\n")
            temp_path = f.name

        try:
            endpoints = _load_all_endpoints(temp_path)
            assert endpoints == []
        finally:
            os.unlink(temp_path)


class TestLoadEndpointConfig:
    """Test load_endpoint_config function."""

    def test_load_specific_endpoint(self, config_file):
        """Should load a specific endpoint by table_name."""
        result = load_endpoint_config("pull_requests", config_file)
        assert result is not None
        assert result["table_name"] == "pull_requests"
        assert result["api_endpoint"] == "/repos/{owner}/{repo}/pulls"
        assert result["incremental"] is True

    def test_load_nonexistent_endpoint(self, config_file):
        """Should return None for non-existent table_name."""
        result = load_endpoint_config("nonexistent", config_file)
        assert result is None

    def test_load_endpoint_auto_detect_path(self, sample_config, tmp_path):
        """Should auto-detect config path when config_path is None."""
        config_path = tmp_path / "test_config.yml"
        with open(config_path, "w") as f:
            import yaml

            yaml.dump(sample_config, f)

        import airflow.plugins.config_loader as loader

        original = loader.DEFAULT_CONFIG_PATHS
        loader.DEFAULT_CONFIG_PATHS = [str(config_path)]

        try:
            result = load_endpoint_config("issues")
            assert result is not None
            assert result["table_name"] == "issues"
        finally:
            loader.DEFAULT_CONFIG_PATHS = original


class TestLoadAllEndpoints:
    """Test load_all_endpoints public function."""

    def test_load_all(self, config_file):
        """Should load all endpoints via public function."""
        endpoints = load_all_endpoints(config_file)
        assert len(endpoints) == 3

    def test_load_all_auto_detect(self, sample_config, tmp_path):
        """Should auto-detect config path."""
        config_path = tmp_path / "test.yml"
        with open(config_path, "w") as f:
            import yaml

            yaml.dump(sample_config, f)

        import airflow.plugins.config_loader as loader

        original = loader.DEFAULT_CONFIG_PATHS
        loader.DEFAULT_CONFIG_PATHS = [str(config_path)]

        try:
            endpoints = load_all_endpoints()
            assert len(endpoints) == 3
        finally:
            loader.DEFAULT_CONFIG_PATHS = original


class TestGetEndpointField:
    """Test get_endpoint_field function."""

    def test_get_existing_field(self, config_file):
        """Should return field value for existing endpoint."""
        result = get_endpoint_field("pull_requests", "api_endpoint", config_file)
        assert result == "/repos/{owner}/{repo}/pulls"

    def test_get_nonexistent_field(self, config_file):
        """Should return None for non-existent field."""
        result = get_endpoint_field("pull_requests", "nonexistent_field", config_file)
        assert result is None

    def test_get_field_nonexistent_table(self, config_file):
        """Should return None for non-existent table."""
        result = get_endpoint_field("nonexistent", "api_endpoint", config_file)
        assert result is None

    def test_get_since_field(self, config_file):
        """Should return correct since_field value."""
        result = get_endpoint_field("releases", "since_field", config_file)
        assert result == "data->>'published_at'"

    def test_get_incremental_flag(self, config_file):
        """Should return correct incremental flag."""
        result = get_endpoint_field("releases", "incremental", config_file)
        assert result is False
