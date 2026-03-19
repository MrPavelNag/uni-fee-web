#!/usr/bin/env bash
set -euo pipefail

# One-command helper for deploy flow:
# - commit local code changes (excluding SQLite runtime files)
# - push to GitHub branch used by Render
# - optionally trigger Render deploy hook
# - optionally wait for healthcheck
#
# Usage:
#   ./scripts/deploy_render.sh "your commit message"
#   TARGET_BRANCH=milestone/web-mvp-stable ./scripts/deploy_render.sh
#   RENDER_DEPLOY_HOOK_URL="https://api.render.com/deploy/..." ./scripts/deploy_render.sh
#   RENDER_DEPLOY_HOOK_URL="..." RENDER_HEALTHCHECK_URL="https://uni-fee-web.onrender.com/healthz" ./scripts/deploy_render.sh

TARGET_BRANCH="${TARGET_BRANCH:-milestone/web-mvp-stable}"
DEFAULT_MSG="webapp: update before Render deploy"
COMMIT_MSG="${1:-$DEFAULT_MSG}"
RENDER_DEPLOY_HOOK_URL="${RENDER_DEPLOY_HOOK_URL:-}"
RENDER_HEALTHCHECK_URL="${RENDER_HEALTHCHECK_URL:-https://uni-fee-web.onrender.com/healthz}"
HEALTHCHECK_TIMEOUT_SEC="${HEALTHCHECK_TIMEOUT_SEC:-240}"
HEALTHCHECK_INTERVAL_SEC="${HEALTHCHECK_INTERVAL_SEC:-5}"
AUTO_SMOKE_CHECK="${AUTO_SMOKE_CHECK:-1}"
SMOKE_SCRIPT_PATH="${SMOKE_SCRIPT_PATH:-./scripts/smoke_render.sh}"
SMOKE_BASE_URL="${SMOKE_BASE_URL:-}"

run_smoke_check() {
  if [[ "${AUTO_SMOKE_CHECK}" != "1" ]]; then
    echo "==> AUTO_SMOKE_CHECK=0, skipping smoke checks"
    return
  fi

  if [[ ! -f "${SMOKE_SCRIPT_PATH}" ]]; then
    echo "==> Smoke script not found: ${SMOKE_SCRIPT_PATH}"
    echo "==> Skipping smoke checks"
    return
  fi

  if [[ ! -x "${SMOKE_SCRIPT_PATH}" ]]; then
    chmod +x "${SMOKE_SCRIPT_PATH}"
  fi

  local inferred_base="${SMOKE_BASE_URL}"
  if [[ -z "${inferred_base}" && -n "${RENDER_HEALTHCHECK_URL}" ]]; then
    if [[ "${RENDER_HEALTHCHECK_URL}" == */healthz ]]; then
      inferred_base="${RENDER_HEALTHCHECK_URL%/healthz}"
    fi
  fi

  echo "==> Running smoke checks"
  if [[ -n "${inferred_base}" ]]; then
    SMOKE_BASE_URL="${inferred_base}" "${SMOKE_SCRIPT_PATH}"
  else
    "${SMOKE_SCRIPT_PATH}"
  fi
}

if ! git rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Error: run this script inside a git repository."
  exit 1
fi

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "${CURRENT_BRANCH}" == "HEAD" ]]; then
  echo "Error: detached HEAD. Checkout ${TARGET_BRANCH} first."
  exit 1
fi

if [[ "${CURRENT_BRANCH}" != "${TARGET_BRANCH}" ]]; then
  echo "Error: current branch is '${CURRENT_BRANCH}', expected '${TARGET_BRANCH}'."
  echo "Tip: git checkout ${TARGET_BRANCH}"
  exit 1
fi

echo "==> Preparing deploy from branch: ${CURRENT_BRANCH}"

# Stage everything first, then unstage runtime SQLite files.
git add -A
git restore --staged data/*.sqlite3 data/*.sqlite3-shm data/*.sqlite3-wal 2>/dev/null || true

if ! git diff --cached --quiet; then
  echo "==> Creating commit"
  git commit -m "${COMMIT_MSG}"
else
  echo "==> No code changes to commit"
fi

echo "==> Pushing to origin/${CURRENT_BRANCH}"
git push origin "${CURRENT_BRANCH}"

echo
if [[ -n "${RENDER_DEPLOY_HOOK_URL}" ]]; then
  echo "==> Triggering Render deploy hook"
  curl -fsS -X POST "${RENDER_DEPLOY_HOOK_URL}" >/dev/null
  echo "==> Deploy triggered"
  if [[ -n "${RENDER_HEALTHCHECK_URL}" ]]; then
    echo "==> Waiting for healthcheck: ${RENDER_HEALTHCHECK_URL}"
    started_at="$(date +%s)"
    while true; do
      if curl -fsS "${RENDER_HEALTHCHECK_URL}" >/dev/null; then
        echo "==> Healthcheck OK"
        break
      fi
      now_ts="$(date +%s)"
      elapsed="$((now_ts - started_at))"
      if [[ "${elapsed}" -ge "${HEALTHCHECK_TIMEOUT_SEC}" ]]; then
        echo "Error: healthcheck timeout after ${HEALTHCHECK_TIMEOUT_SEC}s"
        exit 1
      fi
      sleep "${HEALTHCHECK_INTERVAL_SEC}"
    done
  fi
  run_smoke_check
else
  run_smoke_check
  echo "Done."
  echo "No RENDER_DEPLOY_HOOK_URL set."
  echo "Next: trigger deploy in Render UI (or enable Auto Deploy)."
fi
