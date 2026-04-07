#!/usr/bin/env bash
set -euo pipefail

# Repo path on the VPS (adjust if your clone lives elsewhere).
APP_PATH="/home/deploy/domain_agent"
DEPLOY_USER="deploy"

echo "🚀 Starting deployment update..."

if ! sudo -u "$DEPLOY_USER" bash -lc "cd \"$APP_PATH\" && git pull"; then
  echo "❌ Git pull failed." >&2
  exit 1
fi

echo "✅ Update complete!"
