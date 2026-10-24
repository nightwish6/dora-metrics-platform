"""
Staging Data Loader - batch loader for staging tables.

This module provides a fallback mechanism for loading staging tables
directly from DuckDB raw layer when dbt fails (e.g., OOM errors).

Usage:
    loader = StagingDataLoader(
        model_name="stg_commits",
        duckdb_path="/path/to/data.duckdb",
    )
    result = loader.load()
"""

from __future__ import annotations

from typing import Any

from staging_sql_parser import (
    get_batch_size,
    get_raw_table,
    get_staging_columns,
    get_unique_key,
)


class StagingDataLoader:
    """
    Batch loader for staging tables from raw DuckDB layer.

    Uses ATTACH to READ raw data (avoids lock conflicts with dbt)
    then direct connection for WRITE (ensures changes are saved).

    Workflow:
    1. ATTACH DB in memory → READ raw data with LIMIT/OFFSET into memory
    2. Detach → CLOSE read connection → RELEASE lock
    3. Direct connect → WRITE to staging table with ON CONFLICT UPDATE
    """

    def __init__(
        self,
        model_name: str,
        duckdb_path: str,
        config_path: str | None = None,
        log_callback: callable | None = None,
    ) -> None:
        """
        Initialize the loader.

        Args:
            model_name: Staging model name (e.g. 'stg_commits')
            duckdb_path: Path to DuckDB database file
            config_path: Optional path to staging config YAML
            log_callback: Optional callback for logging (e.g. self.log.info)
        """
        self.model_name = model_name
        self.duckdb_path = duckdb_path
        self.config_path = config_path
        self.log_callback = log_callback

    def load(self) -> dict[str, Any]:
        """
        Perform full reload of staging table from raw layer.

        Returns:
            Dict with loading statistics
        """
        import duckdb

        # Get config
        batch_size = get_batch_size(self.model_name, self.config_path)
        columns = get_staging_columns(self.model_name, self.config_path)
        raw_table_name = self._get_raw_table_name()
        staging_table = self._get_staging_table_name()
        full_staging = f"{staging_table}"  # e.g. staging.commits
        col_names = [col["name"] for col in columns]
        unique_key = get_unique_key(self.model_name, self.config_path)

        # Step 1: READ batches from ATTACH (avoids lock conflicts with dbt)
        con = duckdb.connect(":memory:")
        try:
            con.execute(f"ATTACH '{self.duckdb_path}' AS main_db")

            # Get count from raw table
            count_result = con.execute(f"SELECT COUNT(*) FROM main_db.{raw_table_name}").fetchone()
            total_rows = count_result[0]

            if total_rows == 0:
                return {
                    "status": "success",
                    "fallback_triggered": True,
                    "records_loaded": 0,
                    "batches_processed": 0,
                    "batch_size": batch_size,
                }

            # Build SELECT SQL from raw
            raw_table_unqualified = raw_table_name.split(".")[-1]
            select_sql = self._build_select_sql(columns, raw_table_unqualified, db_alias="main_db")

            # Read ALL batches into memory
            all_batches = []
            offset = 0

            while offset < total_rows:
                query = select_sql.format(limit=batch_size, offset=offset)
                df = con.execute(query).fetchdf()

                if len(df) == 0:
                    break

                all_batches.append(df)
                offset += batch_size

        finally:
            con.close()

        # Step 2: WRITE with direct connection (ensures changes are saved)
        write_con = duckdb.connect(self.duckdb_path)
        try:
            # Truncate staging table
            write_con.execute(f"DELETE FROM {full_staging}")

            # Create unique index for ON CONFLICT
            try:
                write_con.execute(
                    f"CREATE UNIQUE INDEX IF NOT EXISTS {staging_table.split('.')[1]}_unique_idx "
                    f"ON {full_staging} ({unique_key})"
                )
            except Exception:
                pass  # Index might already exist

            # Build ON CONFLICT UPDATE clause
            conflict_cols = [c for c in col_names if c != unique_key]
            update_clause = ", ".join([f"{c} = EXCLUDED.{c}" for c in conflict_cols])
            conflict_sql = f"ON CONFLICT ({unique_key}) " f"DO UPDATE SET {update_clause}"

            # Write batches
            total_loaded = 0
            batch_num = 0

            for df in all_batches:
                batch_num += 1
                batch_count = len(df)

                # Log each cycle
                if self.log_callback:
                    self.log_callback(
                        "[%s] Processing batch %d: %d rows " "(OFFSET %d)",
                        self.model_name,
                        batch_num,
                        batch_count,
                        batch_num * batch_size,
                    )

                # Insert batch into staging table with ON CONFLICT
                temp_table = f"_batch_{batch_num}"
                write_con.register(temp_table, df)

                cols_str = ", ".join(col_names)
                insert_sql = (
                    f"INSERT INTO {full_staging} ({cols_str}) "
                    f"SELECT {cols_str} FROM {temp_table} "
                    f"{conflict_sql}"
                )

                write_con.execute(insert_sql)
                write_con.execute(f"DROP VIEW IF EXISTS {temp_table}")

                total_loaded += batch_count

            # Verify final count
            final_count = write_con.execute(f"SELECT COUNT(*) FROM {full_staging}").fetchone()[0]

            return {
                "status": "success",
                "fallback_triggered": True,
                "records_loaded": total_loaded,
                "batches_processed": batch_num,
                "batch_size": batch_size,
                "verified_count": final_count,
            }

        finally:
            write_con.close()

    def _build_select_sql(self, columns: list[dict], raw_table: str, db_alias: str = "main_db") -> str:
        """
        Build SELECT SQL for batch loading.

        Args:
            columns: Column definitions from config
            raw_table: Raw table name (unqualified)
            db_alias: Database alias for ATTACH (e.g. 'main_db')

        Returns:
            SQL string with {{limit}} and {{offset}} placeholders
        """
        qualified_table = f"{db_alias}.raw.{raw_table}"

        select_parts = []

        for col in columns:
            col_type = col["type"]
            col_name = col["name"]

            if col_type == "md5":
                json_path = col.get("json_path", col.get("raw_column", col_name))
                escaped_path = json_path.replace("'", "\\'")
                select_parts.append(f"md5(CAST(t.data->>'{escaped_path}' AS VARCHAR)) AS {col_name}")
            elif col_type == "json_extract":
                json_path = col["json_path"].replace("'", "\\'")
                select_parts.append(f"t.data->>'{json_path}' AS {col_name}")
            elif col_type == "json_extract_typed":
                json_path = col["json_path"].replace("'", "\\'")
                sql_type = col["sql_type"]
                select_parts.append(f"try_cast(t.data->>'{json_path}' as {sql_type}) AS {col_name}")
            elif col_type == "passthrough":
                select_parts.append(f"t.{col_name} AS {col_name}")
            elif col_type == "static":
                expression = col.get("expression", "CURRENT_TIMESTAMP")
                select_parts.append(f"{expression} AS {col_name}")
            else:
                select_parts.append(f"t.{col_name} AS {col_name}")

        select_clause = ", ".join(select_parts)
        return f"SELECT {select_clause} FROM {qualified_table} t " f"LIMIT {{limit}} OFFSET {{offset}}"

    def _get_staging_table_name(self) -> str:
        """
        Get fully qualified staging table name.

        Returns:
            Schema-qualified table name (e.g. 'staging.commits')
        """
        table_name = self.model_name
        if self.model_name.startswith("stg_"):
            table_name = self.model_name[4:]

        return f"staging.{table_name}"

    def _get_raw_table_name(self) -> str:
        """
        Get fully qualified raw table name.

        Returns:
            Schema-qualified table name (e.g. 'raw.commits')
        """
        raw_table = get_raw_table(self.model_name, self.config_path)
        return f"raw.{raw_table}"
