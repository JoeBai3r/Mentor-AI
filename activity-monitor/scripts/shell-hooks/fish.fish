# Fish shell integration for Activity Monitor
# Source this from config.fish: source /path/to/activity-monitor/scripts/shell-hooks/fish.fish

set -g _AM_SOCKET "/tmp/activity_monitor.sock"

function __am_send -a cmd exit_code shell cwd phase --no-scope-shadowing
  set ts (date +%s)
  set NOHUP_CMD (cd (dirname (status --current-filename)); echo "$PWD")/term_sender.py
  nohup python3 "$NOHUP_CMD" "$cmd" "$exit_code" "$shell" "$cwd" "$ts" "$phase" >/dev/null 2>&1 & disown || true
end

function __am_preexec --on-event fish_preexec
  set -g AM_LAST_CMD $argv[1]
  # ignore internal
  if test (string match -r "^_am_.*" "$AM_LAST_CMD"); return; end
  __am_send "$AM_LAST_CMD" "" "fish" "$PWD" "start"
end

function __am_postexec --on-event fish_postexec
  set -l ec $status
  if test -z "$AM_LAST_CMD"; return; end
  if test (string match -r "^_am_.*" "$AM_LAST_CMD"); return; end
  __am_send "$AM_LAST_CMD" $ec "fish" "$PWD" "end"
end
