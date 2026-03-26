#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."

BASE=$(git merge-base main HEAD)
echo "Merge base with main: $BASE"
echo "Commits being squashed: $(git rev-list --count "$BASE"..HEAD)"

git reset --soft "$BASE"

git commit -m "Initial implementation of agent-notebook

Co-Authored-By: Claude Opus 4.6 (1M context) <noreply@anthropic.com>"

echo ""
git log --oneline -3
echo ""
git status
