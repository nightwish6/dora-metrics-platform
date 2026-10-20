#!/usr/bin/env bash
set -euo pipefail

# Run dbt models locally without Airflow.
# Usage:
#   ./scripts/run_dbt.sh          # Run + test all raw models
#   ./scripts/run_dbt.sh parse    # Only parse models
#   ./scripts/run_dbt.sh test     # Only run tests
#   ./scripts/run_dbt.sh clean    # Clean dbt target directory

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DBT_DIR="$PROJECT_ROOT/dbt"

cd "$DBT_DIR"

case "${1:-run}" in
  parse)
    echo "=== dbt parse ==="
    dbt parse
    ;;
  test)
    echo "=== dbt test ==="
    dbt test --select raw
    ;;
  run)
    echo "=== dbt run ==="
    dbt run --select raw
    echo "=== dbt test ==="
    dbt test --select raw
    ;;
  clean)
    echo "=== dbt clean ==="
    dbt clean
    ;;
  *)
    echo "Usage: $0 {parse|test|run|clean}"
    exit 1
    ;;
esac
