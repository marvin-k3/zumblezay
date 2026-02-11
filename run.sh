#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(
  cd "$(dirname "${BASH_SOURCE[0]}")"
  pwd
)
LOCAL_RUN_SCRIPT="${SCRIPT_DIR}/run.local.sh"

if [[ ! -f "${LOCAL_RUN_SCRIPT}" ]]; then
  cat <<'EOF' 1>&2
Missing run.local.sh
Create one (it is gitignored) based on run.local.sh.example and put your local-only
arguments there. Each change here stays out of commits automatically.
EOF
  exit 1
fi

# shellcheck source=/dev/null
source "${LOCAL_RUN_SCRIPT}"

if [[ -z "${RUN_BIN:-}" ]]; then
  RUN_BIN="zumblezay_server"
fi

if ! declare -p RUN_ARGS >/dev/null 2>&1; then
  echo "RUN_ARGS array must be set in run.local.sh" 1>&2
  exit 1
fi

if [[ "${#RUN_ARGS[@]}" -eq 0 ]]; then
  echo "RUN_ARGS cannot be empty" 1>&2
  exit 1
fi

cargo run --bin="${RUN_BIN}" -- "${RUN_ARGS[@]}" "$@"
