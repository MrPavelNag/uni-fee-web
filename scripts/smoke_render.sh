#!/usr/bin/env bash
set -euo pipefail

# Quick production smoke checks for Render deployment.
# Checks:
# - /healthz
# - /api/meta
# - /api/positions/chains
#
# Usage:
#   ./scripts/smoke_render.sh
#   SMOKE_BASE_URL="https://your-service.onrender.com" ./scripts/smoke_render.sh
#   SMOKE_TIMEOUT_SEC=20 ./scripts/smoke_render.sh

SMOKE_BASE_URL="${SMOKE_BASE_URL:-https://uni-fee-web.onrender.com}"
SMOKE_TIMEOUT_SEC="${SMOKE_TIMEOUT_SEC:-15}"

CURL_ARGS=(
  --silent
  --show-error
  --location
  --max-time "${SMOKE_TIMEOUT_SEC}"
)

ok_count=0
fail_count=0

base_trimmed="${SMOKE_BASE_URL%/}"

check_status_200() {
  local path="$1"
  local label="$2"
  local url="${base_trimmed}${path}"
  local body_file
  body_file="$(mktemp)"

  local code
  code="$(curl "${CURL_ARGS[@]}" -o "${body_file}" -w "%{http_code}" "${url}" || true)"

  if [[ "${code}" == "200" ]]; then
    echo "OK   ${label} (${url})"
    ok_count=$((ok_count + 1))
  else
    echo "FAIL ${label} (${url}) -> HTTP ${code}"
    fail_count=$((fail_count + 1))
  fi

  rm -f "${body_file}"
}

check_json_status_200() {
  local path="$1"
  local label="$2"
  local url="${base_trimmed}${path}"
  local body_file
  body_file="$(mktemp)"

  local code
  code="$(curl "${CURL_ARGS[@]}" -o "${body_file}" -w "%{http_code}" "${url}" || true)"

  if [[ "${code}" != "200" ]]; then
    echo "FAIL ${label} (${url}) -> HTTP ${code}"
    fail_count=$((fail_count + 1))
    rm -f "${body_file}"
    return
  fi

  if python3 - "${body_file}" <<'PY'
import json
import sys
from pathlib import Path

body = Path(sys.argv[1]).read_text(encoding="utf-8")
json.loads(body)
PY
  then
    echo "OK   ${label} (${url})"
    ok_count=$((ok_count + 1))
  else
    echo "FAIL ${label} (${url}) -> response is not JSON-like"
    fail_count=$((fail_count + 1))
  fi

  rm -f "${body_file}"
}

echo "==> Smoke check base URL: ${base_trimmed}"
echo "==> Timeout per request: ${SMOKE_TIMEOUT_SEC}s"
echo

check_status_200 "/healthz" "healthz"
check_json_status_200 "/api/meta" "api meta"
check_json_status_200 "/api/positions/chains" "positions chains"

echo
echo "==> Smoke result: ${ok_count} OK, ${fail_count} FAIL"

if [[ "${fail_count}" -gt 0 ]]; then
  exit 1
fi
