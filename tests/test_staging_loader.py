"""Tests for StagingDataLoader (fallback batch loader)."""

import json
import sys
from pathlib import Path

import duckdb
import pytest

sys.path.insert(0, str(Path(__file__).parent.parent / "airflow" / "plugins"))

from staging_loader import StagingDataLoader


@pytest.fixture
def duckdb_path(tmp_path):
    """Create a test DuckDB database with raw_commits table."""
    db_file = tmp_path / "test_data.duckdb"

    con = duckdb.connect(str(db_file))

    # Create raw schema and raw_commits table
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute("""
        CREATE TABLE raw.commits (
            id VARCHAR,
            data JSON,
            source_name VARCHAR,
            repo_name VARCHAR,
            loaded_at TIMESTAMP
        )
    """)

    # Create staging schema and table
    con.execute("CREATE SCHEMA IF NOT EXISTS staging")
    con.execute("""
        CREATE TABLE staging.commits (
            id VARCHAR,
            repo_name VARCHAR,
            sha VARCHAR,
            url VARCHAR,
            html_url VARCHAR,
            node_id VARCHAR,
            comments_url VARCHAR,
            commit_message VARCHAR,
            commit_tree_sha VARCHAR,
            commit_tree_url VARCHAR,
            commit_url VARCHAR,
            commit_comment_count INTEGER,
            commit_author_name VARCHAR,
            commit_author_email VARCHAR,
            commit_author_date TIMESTAMP,
            commit_committer_name VARCHAR,
            commit_committer_email VARCHAR,
            commit_committer_date TIMESTAMP,
            commit_verification_verified BOOLEAN,
            commit_verification_reason VARCHAR,
            commit_verification_signature VARCHAR,
            commit_verification_payload VARCHAR,
            commit_verification_verified_at TIMESTAMP,
            author_login VARCHAR,
            author_id BIGINT,
            author_node_id VARCHAR,
            author_avatar_url VARCHAR,
            author_html_url VARCHAR,
            author_type VARCHAR,
            author_site_admin BOOLEAN,
            committer_login VARCHAR,
            committer_id BIGINT,
            committer_node_id VARCHAR,
            committer_avatar_url VARCHAR,
            committer_html_url VARCHAR,
            committer_type VARCHAR,
            committer_site_admin BOOLEAN,
            parents VARCHAR,
            loaded_at TIMESTAMP
        )
    """)
    con.execute("CREATE UNIQUE INDEX IF NOT EXISTS commits_unique_idx ON staging.commits (id)")

    # Insert 25 sample commits with nested JSON
    commits = []
    for i in range(25):
        commit_data = {
            "sha": f"abc{i:03d}",
            "url": f"https://api.github.com/repos/vscode/commits/abc{i:03d}",
            "html_url": f"https://github.com/vscode/commit/abc{i:03d}",
            "node_id": f"MDY6Q29tbWl0MTIzNDU{i:02d}",
            "comments_url": f"https://api.github.com/repos/vscode/commits/abc{i:03d}/comments",
            "commit": {
                "message": f"Commit message {i}",
                "tree": {"sha": f"tree{i:03d}", "url": f"https://api.github.com/repos/vscode/git/trees/tree{i:03d}"},
                "url": f"https://api.github.com/repos/vscode/commits/abc{i:03d}",
                "comment_count": i % 10,
                "author": {"name": f"Author {i}", "email": f"author{i}@example.com", "date": "2024-01-15T10:30:00Z"},
                "committer": {
                    "name": f"Committer {i}",
                    "email": f"committer{i}@example.com",
                    "date": "2024-01-15T11:30:00Z",
                },
                "verification": {
                    "verified": i % 2 == 0,
                    "reason": "valid",
                    "signature": f"sig{i:03d}",
                    "payload": f"payload{i:03d}",
                    "verified_at": "2024-01-15T12:00:00Z",
                },
            },
            "author": {
                "login": f"author{i}",
                "id": 1000 + i,
                "node_id": f"MDQ6VXNlcjEwMDA{i:02d}",
                "avatar_url": f"https://avatars.githubusercontent.com/u/1000{i}",
                "html_url": f"https://github.com/author{i}",
                "type": "User",
                "site_admin": False,
            },
            "committer": {
                "login": f"committer{i}",
                "id": 2000 + i,
                "node_id": f"MDQ6VXNlcjIwMDA{i:02d}",
                "avatar_url": f"https://avatars.githubusercontent.com/u/2000{i}",
                "html_url": f"https://github.com/committer{i}",
                "type": "User",
                "site_admin": False,
            },
            "parents": json.dumps(
                [{"sha": f"parent{i:03d}", "url": f"https://api.github.com/repos/vscode/commits/parent{i:03d}"}]
            ),
        }
        commits.append((f"id_{i}", json.dumps(commit_data), "commits", "vscode", "2024-01-15 10:00:00"))

    con.executemany("INSERT INTO raw.commits VALUES (?, ?, ?, ?, ?)", commits)

    con.close()
    return str(db_file)


class TestStagingDataLoader:
    """Tests for StagingDataLoader fallback loader."""

    def test_load_full_reload(self, duckdb_path):
        """Test full reload of staging table."""
        loader = StagingDataLoader(
            model_name="stg_commits",
            duckdb_path=duckdb_path,
        )

        result = loader.load()

        assert result["status"] == "success"
        assert result["fallback_triggered"] is True
        assert result["records_loaded"] == 25
        assert result["batches_processed"] >= 1
        assert result["verified_count"] == 25

    def test_load_empty_table(self, tmp_path):
        """Test loading from empty raw table."""
        db_file = str(tmp_path / "empty.duckdb")

        con = duckdb.connect(db_file)
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute(
            "CREATE TABLE raw.commits (id VARCHAR, data JSON, source_name VARCHAR, repo_name VARCHAR, loaded_at TIMESTAMP)"
        )
        con.execute("CREATE SCHEMA IF NOT EXISTS staging")
        con.execute("CREATE TABLE staging.commits (id VARCHAR)")
        con.close()

        loader = StagingDataLoader(
            model_name="stg_commits",
            duckdb_path=db_file,
        )

        result = loader.load()

        assert result["status"] == "success"
        assert result["records_loaded"] == 0
        assert result["batches_processed"] == 0

    def test_data_integrity(self, duckdb_path):
        """Verify data integrity after loading."""
        loader = StagingDataLoader(
            model_name="stg_commits",
            duckdb_path=duckdb_path,
        )

        loader.load()

        # Verify data using DuckDB
        con = duckdb.connect(duckdb_path)
        try:
            # Check row count
            count = con.execute("SELECT COUNT(*) FROM staging.commits").fetchone()[0]
            assert count == 25

            # Check specific values
            row = con.execute(
                "SELECT sha, commit_message, commit_author_date FROM staging.commits " "WHERE sha = 'abc005'"
            ).fetchone()

            assert row[0] == "abc005"
            assert row[1] == "Commit message 5"
            assert row[2] is not None  # timestamp should be set

            # Check boolean casting
            row = con.execute(
                "SELECT commit_verification_verified FROM staging.commits " "WHERE sha = 'abc002'"
            ).fetchone()
            assert row[0] is True  # 2 % 2 == 0

            # Check integer casting
            row = con.execute("SELECT commit_comment_count FROM staging.commits " "WHERE sha = 'abc010'").fetchone()
            assert row[0] == 0  # 10 % 10 == 0

        finally:
            con.close()
