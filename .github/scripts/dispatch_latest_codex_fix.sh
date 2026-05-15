#!/usr/bin/env bash
set -euo pipefail

PR="${1:?missing PR number}"
REPO="${2:-${GITHUB_REPOSITORY:-zarbopiero963-droid/Pickfair-nogui}}"

DISPATCHED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Dispatching Auto PR Codex Fix for PR #${PR} at ${DISPATCHED_AT}"

gh workflow run auto-pr-codex-fix.yml \
  --repo "$REPO" \
  -f pr_number="$PR"

run_id=""
for _ in $(seq 1 60); do
  sleep 5
  run_id="$(
    gh run list \
      --repo "$REPO" \
      --workflow='Auto PR Codex Fix' \
      --event workflow_dispatch \
      --limit 30 \
      --json databaseId,createdAt,status,url \
    | jq -r --arg dispatched "$DISPATCHED_AT" '
        [
          .[]
          | select(.createdAt >= $dispatched)
        ]
        | sort_by(.createdAt)
        | last
        | .databaseId // empty
      '
  )"

  if [ -n "$run_id" ]; then
    break
  fi
done

if [ -z "$run_id" ]; then
  echo "::error::Could not find freshly dispatched Auto PR Codex Fix run."
  exit 2
fi

echo "Watching Auto PR Codex Fix run: ${run_id}"
gh run watch "$run_id" --repo "$REPO" || true

echo "Auto PR Codex Fix result:"
gh run view "$run_id" \
  --repo "$REPO" \
  --json databaseId,status,conclusion,url \
  --jq '.'
