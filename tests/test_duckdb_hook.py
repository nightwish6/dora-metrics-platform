"""Tests for duckdb_hook.py"""

import os

import pytest

from airflow.plugins.duckdb_hook import DuckDBHook


@pytest.fixture
def test_db_path(tmp_path):
    """Create a temporary DuckDB file path."""
    return str(tmp_path / "test.duckdb")


@pytest.fixture
def hook(test_db_path):
    """Create a DuckDBHook instance with test database."""
    return DuckDBHook(
        db_path=test_db_path,
        schema="raw",
        read_only=False,
    )


class TestGetConn:
    """Test get_conn method."""

    def test_creates_connection(self, hook):
        """Should create a DuckDB connection."""
        conn = hook.get_conn()
        assert conn is not None

    def test_reuses_connection(self, hook):
        """Should return the same connection on subsequent calls."""
        conn1 = hook.get_conn()
        conn2 = hook.get_conn()
        assert conn1 is conn2

    def test_creates_directory(self, test_db_path):
        """Should create the database directory if it doesn't exist."""
        hook = DuckDBHook(
            db_path=test_db_path,
            schema="raw",
            read_only=False,
        )
        hook.get_conn()
        assert os.path.exists(test_db_path)


class TestClose:
    """Test close method."""

    def test_closes_connection(self, hook):
        """Should close the connection."""
        hook.get_conn()
        hook.close()
        assert hook._conn is None

    def test_close_without_connection(self, hook):
        """Should not raise error when closing without connection."""
        hook.close()  # Should not raise


class TestTableExists:
    """Test table_exists method."""

    def test_table_exists_true(self, hook):
        """Should return True if table exists."""
        hook.get_conn().execute("CREATE TABLE raw.test_table (id INTEGER)")
        assert hook.table_exists("test_table") is True

    def test_table_exists_false(self, hook):
        """Should return False if table doesn't exist."""
        assert hook.table_exists("nonexistent_table") is False


class TestGetRowCount:
    """Test get_row_count method."""

    def test_get_row_count_with_data(self, hook):
        """Should return correct row count."""
        hook.get_conn().execute("CREATE TABLE raw.test_table (id INTEGER, name VARCHAR)")
        hook.get_conn().execute("INSERT INTO raw.test_table VALUES (1, 'Alice'), (2, 'Bob')")
        assert hook.get_row_count("test_table") == 2

    def test_get_row_count_empty_table(self, hook):
        """Should return 0 for empty table."""
        hook.get_conn().execute("CREATE TABLE raw.empty_table (id INTEGER)")
        assert hook.get_row_count("empty_table") == 0


class TestGetLastLoadedAt:
    """Test get_last_loaded_at method."""

    def test_get_last_loaded_at_with_data(self, hook):
        """Should return maximum timestamp value."""
        hook.get_conn().execute("CREATE TABLE raw.events (data JSON)")
        hook.get_conn().execute(
            "INSERT INTO raw.events VALUES "
            '(\'{"updated_at": "2024-01-15T10:00:00Z"}\'), '
            '(\'{"updated_at": "2024-01-16T12:00:00Z"}\')'
        )
        result = hook.get_last_loaded_at("events", "data->>'updated_at'")
        assert result == "2024-01-16T12:00:00:00Z" or result == "2024-01-16T12:00:00Z"

    def test_get_last_loaded_at_empty_table(self, hook):
        """Should return None for empty table."""
        hook.get_conn().execute("CREATE TABLE raw.empty_events (data JSON)")
        result = hook.get_last_loaded_at("empty_events", "data->>'updated_at'")
        assert result is None

    def test_get_last_loaded_at_no_timestamp(self, hook):
        """Should return None when field doesn't exist."""
        hook.get_conn().execute("CREATE TABLE raw.no_ts (data JSON)")
        hook.get_conn().execute('INSERT INTO raw.no_ts VALUES (\'{"other_field": "value"}\')')
        result = hook.get_last_loaded_at("no_ts", "data->>'updated_at'")
        assert result is None


class TestCheckIfEmpty:
    """Test check_if_empty method."""

    def test_check_if_empty_true(self, hook):
        """Should return True for empty table."""
        hook.get_conn().execute("CREATE TABLE raw.empty_table (id INTEGER)")
        assert hook.check_if_empty("empty_table") is True

    def test_check_if_empty_false(self, hook):
        """Should return False for non-empty table."""
        hook.get_conn().execute("CREATE TABLE raw.has_data (id INTEGER)")
        hook.get_conn().execute("INSERT INTO raw.has_data VALUES (1)")
        assert hook.check_if_empty("has_data") is False


class TestContextManager:
    """Test context manager protocol."""

    def test_context_manager(self, test_db_path):
        """Should work as context manager."""
        with DuckDBHook(
            db_path=test_db_path,
            schema="raw",
            read_only=False,
        ) as hook:
            conn = hook.get_conn()
            assert conn is not None

        # Connection should be closed after context
        assert hook._conn is None
