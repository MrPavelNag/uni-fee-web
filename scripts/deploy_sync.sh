#!/usr/bin/env bash
set -euo pipefail

# Sync selected files from Cursor worktree to the main repo,
# then commit and push to deployment branch.
#
# Usage:
#   ./scripts/deploy_sync.sh "commit message"
#
# Default source worktree:
#   /Users/pavelnag/.cursor/worktrees/uni_fee/lbw

SRC_ROOT="${SRC_ROOT:-/Users/pavelnag/.cursor/worktrees/uni_fee/lbw}"
TARGET_BRANCH="${TARGET_BRANCH:-milestone/web-mvp-stable}"
DEFAULT_MSG="sync latest changes from worktree"
COMMIT_MSG="${1:-$DEFAULT_MSG}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DST_ROOT="${DST_ROOT:-/Users/pavelnag/agents/uni_fee}"

if ! git -C "${DST_ROOT}" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
  echo "Error: destination is not a git repository: ${DST_ROOT}"
  exit 1
fi

CURRENT_BRANCH="$(git -C "${DST_ROOT}" branch --show-current)"
if [[ "${CURRENT_BRANCH}" != "${TARGET_BRANCH}" ]]; then
  echo "Error: current branch is '${CURRENT_BRANCH}', expected '${TARGET_BRANCH}'."
  echo "Tip: cd \"${DST_ROOT}\" && git checkout ${TARGET_BRANCH}"
  exit 1
fi

FILES=(
  "webapp/main.py"
  "README.md"
  "docs/SMOKE_CHECKLIST.md"
  "scripts/deploy_render.sh"
  "scripts/smoke_render.sh"
  "scripts/deploy_sync.sh"
)

echo "==> Source: ${SRC_ROOT}"
echo "==> Destination: ${DST_ROOT}"

for rel in "${FILES[@]}"; do
  src="${SRC_ROOT}/${rel}"
  dst="${DST_ROOT}/${rel}"
  if [[ ! -f "${src}" ]]; then
    echo "Error: source file not found: ${src}"
    exit 1
  fi
  mkdir -p "$(dirname "${dst}")"
  cp "${src}" "${dst}"
done

chmod +x "${DST_ROOT}/scripts/deploy_render.sh" 2>/dev/null || true
chmod +x "${DST_ROOT}/scripts/smoke_render.sh" 2>/dev/null || true
chmod +x "${DST_ROOT}/scripts/deploy_sync.sh" 2>/dev/null || true

echo "==> Staging files"
git -C "${DST_ROOT}" add "${FILES[@]}"

if git -C "${DST_ROOT}" diff --cached --quiet; then
  echo "==> No new changes after sync"
  exit 0
fi

echo "==> Committing"
git -C "${DST_ROOT}" commit -m "${COMMIT_MSG}"

echo "==> Pushing to origin/${TARGET_BRANCH}"
git -C "${DST_ROOT}" push origin "${TARGET_BRANCH}"

echo
echo "Done."
echo "Next: Render -> Manual Deploy -> Deploy latest commit"
