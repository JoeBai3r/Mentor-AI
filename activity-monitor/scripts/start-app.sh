#!/usr/bin/env bash
set -euo pipefail

# Build the daemon sidecar and launch the Tauri desktop app, which starts
# the daemon and frontend together.
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"

"$ROOT_DIR/scripts/build-daemon-sidecar.sh"

cd "$ROOT_DIR/frontend"
npm run tauri dev
