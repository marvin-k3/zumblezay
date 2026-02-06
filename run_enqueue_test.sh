#!/usr/bin/env bash
set -euo pipefail

BASE_URL="${BASE_URL:-http://127.0.0.1:3011}"
DATE="${DATE:-}"
LIMIT="${LIMIT:-3}"

if [[ -n "${DATE}" ]]; then
  payload=$(printf '{"date":"%s","limit":%s}' "${DATE}" "${LIMIT}")
else
  payload=$(printf '{"limit":%s}' "${LIMIT}")
fi

curl -sS "${BASE_URL}/api/vision/enqueue_test" \
  -H "Content-Type: application/json" \
  -d "${payload}"
echo
