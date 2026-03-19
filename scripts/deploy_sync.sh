#!/usr/bin/env bash
set -euo pipefail

# Sync selected files from Cursor worktree to the main repo,
# then optionally commit and push to deployment branch.
#
# Usage:
#   ./scripts/deploy_sync.sh "commit message"
#   SYNC_ONLY=1 ./scripts/deploy_sync.sh
#   NO_PUSH=1 ./scripts/deploy_sync.sh "commit without push"
#
# Default source worktree:
#   /Users/pavelnag/.cursor/worktrees/uni_fee/lbw
# Optional files are skipped if not present in source.

SRC_ROOT="${SRC_ROOT:-/Users/pavelnag/.cursor/worktrees/uni_fee/lbw}"
TARGET_BRANCH="${TARGET_BRANCH:-milestone/web-mvp-stable}"
DEFAULT_MSG="sync latest changes from worktree"
COMMIT_MSG="${1:-$DEFAULT_MSG}"
DST_ROOT="${DST_ROOT:-/Users/pavelnag/agents/uni_fee}"
SYNC_ONLY="${SYNC_ONLY:-0}"
NO_PUSH="${NO_PUSH:-0}"

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

REQUIRED_FILES=(
  "webapp/main.py"
  "README.md"
  "scripts/deploy_render.sh"
  "scripts/deploy_sync.sh"
)

echo "==> Source: ${SRC_ROOT}"
echo "==> Destination: ${DST_ROOT}"

if [[ ! -d "${SRC_ROOT}" ]]; then
  echo "Error: source directory not found: ${SRC_ROOT}"
  exit 1
fi

OPTIONAL_FILES=(
  "docs/SMOKE_CHECKLIST.md"
  "scripts/smoke_render.sh"
)

STAGE_FILES=()

for rel in "${REQUIRED_FILES[@]}"; do
  src="${SRC_ROOT}/${rel}"
  dst="${DST_ROOT}/${rel}"
  if [[ ! -f "${src}" ]]; then
    echo "Error: required source file not found: ${src}"
    exit 1
  fi
  if [[ "${src}" == "${dst}" ]]; then
    echo "==> Source equals destination, leaving as-is: ${rel}"
    STAGE_FILES+=("${rel}")
    continue
  fi
  mkdir -p "$(dirname "${dst}")"
  cp "${src}" "${dst}"
  STAGE_FILES+=("${rel}")
done

for rel in "${OPTIONAL_FILES[@]}"; do
  src="${SRC_ROOT}/${rel}"
  dst="${DST_ROOT}/${rel}"
  if [[ ! -f "${src}" ]]; then
    echo "Warn: optional source file missing, skipping: ${src}"
    continue
  fi
  if [[ "${src}" == "${dst}" ]]; then
    echo "==> Source equals destination, leaving as-is: ${rel}"
    STAGE_FILES+=("${rel}")
    continue
  fi
  mkdir -p "$(dirname "${dst}")"
  cp "${src}" "${dst}"
  STAGE_FILES+=("${rel}")
done

chmod +x "${DST_ROOT}/scripts/deploy_render.sh" 2>/dev/null || true
chmod +x "${DST_ROOT}/scripts/smoke_render.sh" 2>/dev/null || true
chmod +x "${DST_ROOT}/scripts/deploy_sync.sh" 2>/dev/null || true

echo "==> Staging files"
git -C "${DST_ROOT}" add "${STAGE_FILES[@]}"

if git -C "${DST_ROOT}" diff --cached --quiet; then
  echo "==> No new changes after sync"
  exit 0
fi

if [[ "${SYNC_ONLY}" == "1" ]]; then
  echo "==> SYNC_ONLY=1, skipping commit and push"
  exit 0
fi

echo "==> Committing"
git -C "${DST_ROOT}" commit -m "${COMMIT_MSG}"

if [[ "${NO_PUSH}" == "1" ]]; then
  echo "==> NO_PUSH=1, skipping push"
  echo "Done."
  echo "Next: run git -C \"${DST_ROOT}\" push origin ${TARGET_BRANCH}"
  exit 0
fi

echo "==> Pushing to origin/${TARGET_BRANCH}"
git -C "${DST_ROOT}" push origin "${TARGET_BRANCH}"

echo
echo "Done."
echo "Next: Render -> Manual Deploy -> Deploy latest commit"
