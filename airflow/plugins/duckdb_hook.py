"""
DuckDB Hook for connecting to DuckDB and querying raw data.

Used by GitHubApiLoadOperator to:
1. Check if a table is empty (full load vs incremental)
2. Extract the last_loaded_at timestamp for incremental filtering
"""

from __future__ import annotations

import logging
import os

import duckdb

logger = logging.getLogger(__name__)


class DuckDBHook:
    """
    Hook for interacting with DuckDB database.

    Args:
        db_path: Path to the DuckDB database file
        schema: Schema name (default: raw)
        read_only: Open connection in read-only mode (allows parallel reads)
    """

    def __init__(
        self,
        db_path: str = "data/duckdb/data.duckdb",
        schema: str = "raw",
        read_only: bool = True,
    ) -> None:
        self.db_path = db_path
        self.schema = schema
        self.read_only = read_only
        self._conn: duckdb.DuckDBPyConnection | None = None

    def get_conn(self) -> duckdb.DuckDBPyConnection:
        """Get or create DuckDB connection."""
        if self._conn is None:
            # Ensure directory exists
            db_dir = os.path.dirname(self.db_path)
            if db_dir:
                os.makedirs(db_dir, exist_ok=True)

            # Open in read-only mode to allow parallel connections
            if self.read_only:
                self._conn = duckdb.connect(self.db_path, read_only=True)
            else:
                self._conn = duckdb.connect(self.db_path)

            # Ensure raw schema exists (only needed for write mode)
            if not self.read_only:
                self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self.schema}")

        return self._conn

    def close(self) -> None:
        """Close DuckDB connection."""
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def table_exists(self, table_name: str) -> bool:
        """Check if a table exists in the raw schema."""
        conn = self.get_conn()
        result = conn.execute(
            f"SELECT count(*) FROM information_schema.tables "
            f"WHERE table_schema = '{self.schema}' "
            f"AND table_name = '{table_name}'"
        ).fetchone()
        return result[0] > 0 if result else False

    def get_row_count(self, table_name: str) -> int:
        """Get the number of rows in a table."""
        conn = self.get_conn()
        result = conn.execute(f"SELECT COUNT(*) FROM {self.schema}.{table_name}").fetchone()
        return result[0] if result else 0

    def get_last_loaded_at(
        self,
        table_name: str,
        since_field: str,
    ) -> str | None:
        """
        Get the maximum value of the since_field from a table.

        Args:
            table_name: Name of the table in raw schema
            since_field: JSON path expression to extract the timestamp

        Returns:
            The maximum timestamp value as ISO string, or None if table is empty
        """
        conn = self.get_conn()

        try:
            result = conn.execute(f"SELECT MAX({since_field}) FROM {self.schema}.{table_name}").fetchone()

            if result and result[0] is not None:
                return str(result[0])
            return None
        except Exception as e:
            logger.warning(
                "Could not extract last_loaded_at from %s: %s",
                table_name,
                e,
            )
            return None

    def check_if_empty(self, table_name: str) -> bool:
        """
        Check if a table is empty (0 rows).

        Args:
            table_name: Name of the table in raw schema

        Returns:
            True if table has 0 rows, False otherwise
        """
        return self.get_row_count(table_name) == 0

    def __enter__(self) -> DuckDBHook:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.close()
