#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(
  cd "$(dirname "${BASH_SOURCE[0]}")"
  pwd
)

VENV_PATH="${SCRIPT_DIR}/.venv"
PYTHON_BIN="${VENV_PATH}/bin/python"

if [[ ! -x "${PYTHON_BIN}" ]]; then
  cat <<'EOF' 1>&2
Missing .venv. Create it with:
  uv venv .venv
  uv pip install ultralytics opencv-python opentelemetry-sdk opentelemetry-api
EOF
  exit 1
fi

BASE_URL="${BASE_URL:-http://127.0.0.1:3011}"
WORKER_ID="${WORKER_ID:-$(hostname -s)}"

exec "${PYTHON_BIN}" "${SCRIPT_DIR}/scripts/vision_worker.py" \
  --base-url "${BASE_URL}" \
  --worker-id "${WORKER_ID}" \
  "$@"
