# Zsh shell integration for Activity Monitor
# Source this from ~/.zshrc: source /path/to/activity-monitor/scripts/shell-hooks/zsh.sh

_AM_SOCKET="/tmp/activity_monitor.sock"

_am_send() {
  local cmd="$1"; local exit_code="$2"; local shell="$3"; local cwd="$4"; local phase="$5"; local ts
  ts=$(date +%s)
  NOHUP_CMD="$(cd "$(dirname "${0}")" && pwd)/term_sender.py"
  nohup python3 "$NOHUP_CMD" "$cmd" "$exit_code" "$shell" "$cwd" "$ts" "$phase" >/dev/null 2>&1 & disown || true
}

# preexec hook - run before command
_am_preexec() {
  AM_LAST_CMD="$1"
  # ignore internal
  case "$AM_LAST_CMD" in
    _am_*|source*) return;;
  esac
  _am_send "$AM_LAST_CMD" "" "zsh" "$PWD" "start"
}

# precmd hook - runs before prompt after command
_am_postexec() {
  local ec=$?
  local cmd="$AM_LAST_CMD"
  case "$cmd" in
    _am_*|source*) return;;
  esac
  _am_send "$cmd" "$ec" "zsh" "$PWD" "end"
}

if typeset -f add-zsh-hook >/dev/null 2>&1; then
  autoload -Uz add-zsh-hook
  add-zsh-hook preexec _am_preexec
  add-zsh-hook precmd _am_postexec
else
  preexec() { _am_preexec "$1"; }
  precmd() { _am_postexec; }
fi
