#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
HOOK_DIR="$ROOT_DIR/scripts/shell-hooks"

install_if_missing(){
  local rcfile="$1"; local source_line="$2"
  if [ -f "$rcfile" ]; then
    if grep -Fq "$source_line" "$rcfile"; then
      echo "Already present in $rcfile"
    else
      echo "Adding source line to $rcfile"
      printf "\n# Activity Monitor hook\n%s\n" "$source_line" >> "$rcfile"
    fi
  else
    echo "Creating $rcfile and adding source line"
    mkdir -p "$(dirname "$rcfile")"
    printf "# Activity Monitor hook\n%s\n" "$source_line" > "$rcfile"
  fi
}

# Bash
BASHRC="$HOME/.bashrc"
BASHLINE="source $HOOK_DIR/bash.sh"
install_if_missing "$BASHRC" "$BASHLINE"

# Zsh
ZSHRC="$HOME/.zshrc"
ZSHLINE="source $HOOK_DIR/zsh.sh"
install_if_missing "$ZSHRC" "$ZSHLINE"

# Fish
FISHCFG="$HOME/.config/fish/config.fish"
FISHLINE="source $HOOK_DIR/fish.fish"
install_if_missing "$FISHCFG" "$FISHLINE"

echo "Install complete. Restart your shells or source the rc file to enable hooks."
