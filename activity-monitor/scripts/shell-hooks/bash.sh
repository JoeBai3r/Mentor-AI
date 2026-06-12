# Bash shell integration for Activity Monitor
# Source this from ~/.bashrc: source /path/to/activity-monitor/scripts/shell-hooks/bash.sh

_AM_SOCKET="/tmp/activity_monitor.sock"

_am_send() {
  local cmd="$1" exit_code="$2" shell="$3" cwd="$4" ts phase="$5"
  ts=$(date +%s)
  NOHUP_CMD="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/term_sender.py"
  nohup python3 "$NOHUP_CMD" "$cmd" "$exit_code" "$shell" "$cwd" "$ts" "$phase" >/dev/null 2>&1 & disown || true
}

# Keep two slots to track current and previous commands
_am_preexec() {
  # shift commands
  AM_PREV_COMMAND="${AM_COMMAND:-}"
  AM_COMMAND="$BASH_COMMAND"

  # ignore internal functions and the PROMPT_COMMAND itself
  case "$AM_COMMAND" in
    _am_*|PROMPT_COMMAND|source*)
      return
      ;;
  esac

  # publish start phase
  _am_send "$AM_COMMAND" "" "bash" "$PWD" "start"
}
trap '_am_preexec' DEBUG

# After command returns, send the completed command (from AM_PREV_COMMAND)
_am_postexec() {
  local ec=$?
  local cmd="${AM_PREV_COMMAND:-$AM_COMMAND}"
  # ignore internal commands
  case "$cmd" in
    _am_*|PROMPT_COMMAND|source*)
      return
      ;;
  esac
  _am_send "$cmd" "$ec" "bash" "$PWD" "end"
}

# Append to PROMPT_COMMAND safely (preserve existing)
if [[ -n "${PROMPT_COMMAND:-}" ]]; then
  PROMPT_COMMAND="_am_postexec; $PROMPT_COMMAND"
else
  PROMPT_COMMAND="_am_postexec"
fi
