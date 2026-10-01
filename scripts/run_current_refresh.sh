#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$PROJECT_ROOT"

if [[ -f "$PROJECT_ROOT/.env" ]]; then
  # shellcheck source=/dev/null
  source "$PROJECT_ROOT/scripts/load_project_env.sh" "$PROJECT_ROOT/.env"
fi

if [[ -z "${PYTHON_BIN:-}" ]]; then
  if [[ -x "$PROJECT_ROOT/.venv/bin/python" ]]; then
    PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
  else
    PYTHON_BIN=python3
  fi
fi

export PYTHONPATH="$PROJECT_ROOT:$PROJECT_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

if [[ -x "$PROJECT_ROOT/.venv/bin/python" ]]; then
  export PYSPARK_PYTHON="${PYSPARK_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
  export PYSPARK_DRIVER_PYTHON="${PYSPARK_DRIVER_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"
fi

export HADOOP_USER_NAME="${HADOOP_USER_NAME:-hadoop}"

exec "$PYTHON_BIN" -m src.current_refresh.scheduler "$@"
