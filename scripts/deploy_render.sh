#!/usr/bin/env bash
set -euo pipefail

# One-command helper for deploy flow:
# - commit local code changes (excluding SQLite runtime files)
# - push to GitHub branch used by Render
#
# Usage:
#   ./scripts/deploy_render.sh "your commit message"
#   TARGET_BRANCH=milestone/web-mvp-stable ./scripts/deploy_render.sh

TARGET_BRANCH="${TARGET_BRANCH:-milestone/web-mvp-stable}"
DEFAULT_MSG="webapp: update before Render deploy"
COMMIT_MSG="${1:-$DEFAULT_MSG}"

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
echo "Done."
echo "Next in Render: Manual Deploy -> Deploy latest commit (or wait for auto deploy)."
