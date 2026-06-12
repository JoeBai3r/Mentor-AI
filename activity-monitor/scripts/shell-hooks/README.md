Shell hooks for Activity Monitor

Files:
- bash.sh — source from ~/.bashrc
- zsh.sh  — source from ~/.zshrc
- fish.fish — source from config.fish

Usage example:
  # Bash
  echo "source /path/to/activity-monitor/scripts/shell-hooks/bash.sh" >> ~/.bashrc

Notes:
- The hooks send newline-delimited JSON messages to /tmp/activity_monitor.sock using python3. Ensure python3 is available.
- The daemon must be running to receive messages.
- For fish, your fish version must support fish_preexec/fish_postexec events (fish 3+).
