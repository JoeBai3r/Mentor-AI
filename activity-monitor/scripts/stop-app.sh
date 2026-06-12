#!/usr/bin/env bash
set -euo pipefail

# Stop the Tauri desktop app, its dev tooling (vite/tauri-cli), and any
# daemon sidecar processes left running.
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

PATTERNS=(
  "$ROOT_DIR/frontend/src-tauri/target/.*/activity-monitor-daemon"
  "$ROOT_DIR/frontend/src-tauri/target/.*/app"
  "tauri dev"
  "$ROOT_DIR/frontend/node_modules/.bin/vite"
  "$ROOT_DIR/frontend/node_modules/.bin/tauri"
)

USER="$(whoami)"
killed_any=0
for pattern in "${PATTERNS[@]}"; do
  pids=$(pgrep -u "$USER" -f "$pattern" || true)
  if [ -n "$pids" ]; then
    echo "Stopping ($pattern): $pids"
    kill $pids 2>/dev/null || true
    killed_any=1
  fi
done

if [ "$killed_any" = "1" ]; then
  sleep 1
  for pattern in "${PATTERNS[@]}"; do
    pids=$(pgrep -u "$USER" -f "$pattern" || true)
    if [ -n "$pids" ]; then
      echo "Force-killing ($pattern): $pids"
      kill -9 $pids 2>/dev/null || true
    fi
  done
  echo "Stopped."
else
  echo "Nothing running."
fi
