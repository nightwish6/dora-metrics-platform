#!/usr/bin/env bash
set -euo pipefail

echo "=== Installing pre-commit hooks ==="
pip install pre-commit
pre-commit install

echo ""
echo "=== Hooks installed ==="
echo "Pre-commit hooks will now run before every git commit."
echo ""
echo "To run manually: pre-commit run --all-files"
echo "To uninstall:    pre-commit uninstall"
