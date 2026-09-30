#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
source "$PROJECT_ROOT/scripts/load_project_env.sh" "$PROJECT_ROOT/.env"
cd "$PROJECT_ROOT"
exec python -m src.streaming.review_producer "$@"
