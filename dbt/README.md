# DORA Metrics Platform - DBT Documentation

## Project Overview

This dbt project implements a data pipeline for calculating **DORA (DevOps Research and Assessment) metrics** from GitHub data. The project follows a layered architecture with three distinct layers:

| Layer | Folder | Schema | Materialization |
|-------|--------|--------|-----------------|
| Raw | `1_raw/` | `raw` | Incremental |
| Staged | `2_staged/` | `staging` | Incremental |
| Marts | `3_marts/` | `marts` | Table |

## Data Flow

```
JSON Files → 1_raw → 2_staged → 3_marts
   (GitHub)     (Load)   (Flatten)   (Calculate)
```

---

## 1. Raw Layer (`1_raw/`)

Raw models load data from JSON files and store the original JSON payload in a `data` column. Each row is uniquely identified by an MD5 hash of the source identifier.

| Model | Source | Unique Key | Description |
|-------|--------|------------|-------------|
| `raw_commits` | Commits JSON | `id` | Commit metadata including author, committer, verification status, and timestamps. |
| `raw_issues` | Issues JSON | `id` | GitHub issue/PR information including state, labels, assignees, milestones, and timestamps. |
| `raw_milestones` | Milestones JSON | `id` | GitHub milestone information including title, description, due dates, and state tracking. |
| `raw_pull_requests` | PR JSON | `id` | PR metadata including URLs, labels, states, and diff statistics. |
| `raw_releases` | Releases JSON | `id` | GitHub release information including tag names, asset URLs, and author details. |
| `raw_workflow_runs` | Workflow JSON | `id` | CI/CD pipeline execution information including status, conclusion, head SHA, and timestamps. |

**Common Parameters:**
- `json_path`: Path to the JSON file (e.g., `data/raw/vscode_commits.json`)
- `repo_name`: Repository name (defaults to `vscode`)

---

## 2. Staging Layer (`2_staged/`)

Staging models flatten the raw JSON data into structured columns with proper types. Most models support incremental refresh.

| Model | Source Model | Incremental By | Description |
|-------|--------------|----------------|-------------|
| `stg_commits` | `raw_commits` | `commit_committer_date` | Flattened commit data with author/committer info, verification status, and metadata. |
| `stg_issues` | `raw_issues` | `updated_at` | Flattened issue/PR data with state, labels, assignees, milestones, and timestamps. |
| `stg_milestones` | `raw_milestones` | N/A | Flattened milestone data with state, description, due dates, creator info. |
| `stg_pull_requests` | `raw_pull_requests` | `updated_at` | Flattened PR data with state, labels, diffs, review stats, assignees, branch info. |
| `stg_releases` | `raw_releases` | N/A | Flattened release data with tag name, release name, asset info, author details. |
| `stg_workflow_runs` | `raw_workflow_runs` | `updated_at` | Flattened workflow run data with CI/CD status, conclusion, head SHA, event type. |

---

## 3. Marts Layer (`3_marts/`)

### Core DORA Metrics

The four core DORA metrics defined by the State of DevOps Report.

| Model | Metric | Key Columns |
|-------|--------|-------------|
| `core_lead_time` | **Lead Time for Changes** | `avg_lead_time_hours`, `p50_lead_time_hours`, `p90_lead_time_hours` |
| `core_deployment_frequency` | **Deployment Frequency** | `release_count` per month |
| `core_cfr` | **Change Failure Rate** | `cfr_revert_rate` (percentage of reverting commits) |
| `core_time_to_restore` | **Time to Restore Service** | `avg_restore_days`, `bug_fix_p50`, `bug_fix_p90` |

### Aggregated DORA Metrics

| Model | Description |
|-------|-------------|
| `core_dora_metrics_avg` | Combines all four DORA metrics into a single aggregated view per repository with monthly averages. |

### Supporting Metrics

| Model | Description |
|-------|-------------|
| `supporting_flow` | **Flow & Efficiency**: PR merge rate, closed time, assignee coverage, stale PR rate, draft ratio, locked PR/issue ratio, issue-to-PR ratio. |
| `supporting_health` | **Project Health**: commit velocity, top author share, on-time milestone completion, milestone coverage, release-to-PR size. |
| `supporting_quality` | **Code Quality**: revert rate, GPG verification, author/committer mismatch, bug ratio, comment density, quick-win ratio, backlog aging. |

---

## Common Columns

All models include:

| Column | Type | Description |
|--------|------|-------------|
| `repo_name` | VARCHAR | GitHub repository name |
| `period` | DATE | Month-truncated period (where applicable) |
| `loaded_at` | TIMESTAMP | Timestamp when the record was loaded |

---

## Macros

| Macro | Description |
|-------|-------------|
| `generate_schema_name` | Custom schema name generator. Uses default schema from profiles.yml when no custom schema is specified. |

### Test Macros

Located in `macros/tests/`:

| Macro | Description |
|-------|-------------|
| `not_empty_string` | Ensures a string field is not null and not just whitespace. |
| `valid_git_sha` | Validates Git SHA-1 format (40 lowercase hex characters). |
| `valid_github_login` | Validates GitHub login conventions (lowercase, alphanumeric + hyphens, max 39 chars). |
| `valid_email` | Validates email format. |
| `valid_repo_name` | Validates repository name format. |
| `valid_sha` | Validates SHA format. |
| `commit_date_after_unix_epoch` | Ensures commit date is after Unix epoch. |
| `commit_date_not_too_old` | Ensures commit date is not excessively old. |
| `commit_message_min_length` | Ensures commit message meets minimum length. |
| `not_in_future` | Ensures a timestamp is not in the future. |

---

## Running DBT Commands

```bash
# Compile models
dbt compile

# Generate documentation
dbt docs generate

# Serve documentation
dbt docs serve

# Run models
dbt run

# Run tests
dbt test
```

## Project Configuration

- **DBT Version**: 2.x
- **Adapter**: DuckDB (uses `read_json_auto` for JSON parsing)
- **Incremental Strategy**: `delete+insert`
- **Default Materialization**: Table (Raw & Staged are incremental)
