"""Tests for staging_sql_loader (YAML-based config loader)."""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "airflow" / "plugins"))

from staging_sql_parser import (
    build_insert_sql,
    get_batch_size,
    get_raw_table,
    get_staging_columns,
    get_unique_key,
    load_staging_model_config,
)


class TestLoadStagingModelConfig:
    """Test loading staging model configuration from YAML."""

    def test_load_stg_commits_config(self):
        """Verify stg_commits config loads correctly."""
        config = load_staging_model_config("stg_commits")

        assert config is not None
        assert config["raw_table"] == "commits"
        assert config["unique_key"] == "id"
        assert config["batch_size"] == 5000
        assert len(config["columns"]) == 39

    def test_load_nonexistent_model(self):
        """Verify None returned for unknown model."""
        config = load_staging_model_config("stg_nonexistent")
        assert config is None

    def test_column_types_preserved(self):
        """Verify column types are correctly loaded."""
        columns = get_staging_columns("stg_commits")
        col_map = {col["name"]: col for col in columns}

        # md5 type
        assert col_map["id"]["type"] == "md5"
        assert col_map["id"]["json_path"] == "$.sha"

        # json_extract type (repo_name, sha, etc.)
        assert col_map["repo_name"]["type"] == "json_extract"
        assert col_map["repo_name"]["json_path"] == "$.repo_name"

        assert col_map["sha"]["type"] == "json_extract"
        assert col_map["sha"]["json_path"] == "$.sha"

        # json_extract_typed type
        assert col_map["commit_author_date"]["type"] == "json_extract_typed"
        assert col_map["commit_author_date"]["json_path"] == "$.commit.author.date"
        assert col_map["commit_author_date"]["sql_type"] == "TIMESTAMP"

        # static type
        assert col_map["loaded_at"]["type"] == "static"
        assert col_map["loaded_at"]["expression"] == "CURRENT_TIMESTAMP"


class TestBuildInsertSql:
    """Test INSERT SQL generation."""

    def test_build_insert_sql_stg_commits(self):
        """Verify INSERT SQL is generated correctly."""
        sql, columns = build_insert_sql("stg_commits")

        assert sql.startswith("INSERT INTO")
        assert "staging.commits" in sql or "staging.stg_commits" in sql
        assert "VALUES" in sql
        assert "?" in sql

        # Verify columns match config
        assert "id" in columns
        assert "sha" in columns
        assert "commit_message" in columns
        assert "loaded_at" in columns


class TestHelperFunctions:
    """Test helper functions."""

    def test_get_batch_size(self):
        """Verify batch size retrieval."""
        size = get_batch_size("stg_commits")
        assert size == 5000

    def test_get_raw_table(self):
        """Verify raw table name retrieval."""
        raw_table = get_raw_table("stg_commits")
        assert raw_table == "commits"

    def test_get_unique_key(self):
        """Verify unique key retrieval."""
        unique_key = get_unique_key("stg_commits")
        assert unique_key == "id"

    def test_get_unique_key_nonexistent(self):
        """Verify error for unknown model."""
        with pytest.raises(ValueError):
            get_unique_key("stg_nonexistent")
