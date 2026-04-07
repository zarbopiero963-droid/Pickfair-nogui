#!/usr/bin/env bash
set -euo pipefail

REPO="${1:-zarbopiero963-droid/Pickfair-nogui}"
BRANCH="${2:-$(git branch --show-current)}"

echo "Repo:   $REPO"
echo "Branch: $BRANCH"

PR_NUMBER="$(gh pr list --repo "$REPO" --head "$BRANCH" --json number --jq '.[0].number')"

if [ -z "$PR_NUMBER" ] || [ "$PR_NUMBER" = "null" ]; then
  echo "No open PR found for branch: $BRANCH"
  exit 1
fi

echo "Using PR #$PR_NUMBER"

gh api "repos/$REPO/pulls/$PR_NUMBER/files?per_page=100" > pr_files_raw.json

python - <<'PY'
import json

ALLOWED = {
    "tests/integration/test_betfair_timeout_and_ghost_orders.py",
    "tests/chaos/test_runtime_network_instability.py",
    "tests/chaos/test_runtime_partial_failure_paths.py",
    "tests/chaos/test_runtime_reconcile_under_stress.py",
}

with open("pr_files_raw.json", "r", encoding="utf-8") as f:
    data = json.load(f)

files = [item["filename"] for item in data if item.get("filename")]

print("\nChanged files:")
for f in files:
    print("-", f)

invalid = [f for f in files if f not in ALLOWED]

if invalid:
    print("\nINVALID FILES:")
    for f in invalid:
        print("-", f)
    raise SystemExit(1)

print("\nScope valid.")
PY