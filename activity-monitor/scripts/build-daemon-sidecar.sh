#!/usr/bin/env bash
set -euo pipefail

# Build the daemon and place it where Tauri expects its sidecar binary,
# named with the Rust target triple as required by Tauri's externalBin convention.
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
DAEMON_DIR="$ROOT_DIR/daemon"
BIN_DIR="$ROOT_DIR/frontend/src-tauri/binaries"

source "$HOME/.cargo/env" 2>/dev/null || true

echo "Building daemon (release)..."
( cd "$DAEMON_DIR" && cargo build --release )

TRIPLE="$(rustc -vV | sed -n 's/^host: //p')"
EXT=""
if [[ "$TRIPLE" == *windows* ]]; then EXT=".exe"; fi

mkdir -p "$BIN_DIR"
cp "$DAEMON_DIR/target/release/activity_daemon$EXT" "$BIN_DIR/activity-monitor-daemon-$TRIPLE$EXT"

echo "Daemon sidecar ready: $BIN_DIR/activity-monitor-daemon-$TRIPLE$EXT"
