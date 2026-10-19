#!/usr/bin/env python3
"""Create DuckDB with test data for CI."""

import json
import os
import tempfile

import duckdb

DB_PATH = "data/duckdb/data.duckdb"
SCHEMA = "raw"

# Ensure directory exists
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# Sample data for each table
tables = {
    "pull_requests": [
        {
            "id": 1,
            "number": 100,
            "title": "Fix bug",
            "state": "closed",
            "updated_at": "2024-01-15T10:00:00Z",
            "created_at": "2024-01-14T10:00:00Z",
            "merged_at": "2024-01-15T12:00:00Z",
        },
        {
            "id": 2,
            "number": 101,
            "title": "Add feature",
            "state": "closed",
            "updated_at": "2024-01-16T10:00:00Z",
            "created_at": "2024-01-15T10:00:00Z",
            "merged_at": "2024-01-16T14:00:00Z",
        },
    ],
    "issues": [
        {
            "id": 1,
            "number": 200,
            "title": "Bug report",
            "state": "closed",
            "updated_at": "2024-01-15T10:00:00Z",
            "created_at": "2024-01-14T10:00:00Z",
        },
        {
            "id": 2,
            "number": 201,
            "title": "Enhancement",
            "state": "closed",
            "updated_at": "2024-01-16T10:00:00Z",
            "created_at": "2024-01-15T10:00:00Z",
        },
    ],
    "commits": [
        {
            "sha": "abc123",
            "commit": {
                "committer": {"date": "2024-01-15T10:00:00Z"},
                "message": "Fix bug",
            },
        },
        {
            "sha": "def456",
            "commit": {
                "committer": {"date": "2024-01-16T10:00:00Z"},
                "message": "Add feature",
            },
        },
    ],
    "releases": [
        {
            "id": 1,
            "tag_name": "v1.0.0",
            "published_at": "2024-01-15T10:00:00Z",
            "name": "Release 1.0",
        },
        {
            "id": 2,
            "tag_name": "v1.1.0",
            "published_at": "2024-01-16T10:00:00Z",
            "name": "Release 1.1",
        },
    ],
    "milestones": [
        {
            "id": 1,
            "number": 1,
            "title": "Milestone 1",
            "state": "closed",
            "updated_at": "2024-01-15T10:00:00Z",
            "created_at": "2024-01-14T10:00:00Z",
        },
        {
            "id": 2,
            "number": 2,
            "title": "Milestone 2",
            "state": "open",
            "updated_at": "2024-01-16T10:00:00Z",
            "created_at": "2024-01-15T10:00:00Z",
        },
    ],
    "workflow_runs": [
        {
            "id": 1,
            "name": "CI",
            "status": "completed",
            "updated_at": "2024-01-15T10:00:00Z",
            "created_at": "2024-01-15T09:00:00Z",
        },
        {
            "id": 2,
            "name": "Deploy",
            "status": "completed",
            "updated_at": "2024-01-16T10:00:00Z",
            "created_at": "2024-01-16T09:00:00Z",
        },
    ],
}

# Connect to DuckDB (creates file if not exists)
conn = duckdb.connect(DB_PATH)
conn.execute(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}")

for table_name, data in tables.items():
    # Drop existing table
    conn.execute(f"DROP TABLE IF EXISTS {SCHEMA}.{table_name}")

    # Write JSON to temp file for read_json_auto
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(data, f)
        json_file = f.name

    try:
        # Create table with all required columns directly
        if table_name == "pull_requests":
            id_col = "number"
        elif table_name == "commits":
            id_col = "sha"
        else:
            id_col = "id"

        conn.execute(f"""
            CREATE TABLE {SCHEMA}.{table_name} AS
            SELECT
                md5(CAST({id_col} AS VARCHAR)) AS id,
                TO_JSON(t) AS data,
                '{table_name}' AS source_name,
                'vscode' AS repo_name,
                now() AS loaded_at,
                t.*
            FROM read_json_auto('{json_file}') t
        """)
    finally:
        os.unlink(json_file)

# Verify
for table_name in tables:
    result = conn.execute(f"SELECT COUNT(*) FROM {SCHEMA}.{table_name}").fetchone()
    print(f"{table_name}: {result[0]} rows")

conn.close()
print(f"\nDuckDB created at: {DB_PATH}")
