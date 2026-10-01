#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$PROJECT_ROOT/scripts/load_project_env.sh" "$PROJECT_ROOT/.env"
PYTHON_BIN="${PLAYER_COUNT_PYTHON:-$PROJECT_ROOT/.venv/bin/python}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Python interpreter is not executable: $PYTHON_BIN" >&2
  exit 2
fi

cd "$PROJECT_ROOT"
exec "$PYTHON_BIN" -m src.streaming.player_count_producer "$@"
