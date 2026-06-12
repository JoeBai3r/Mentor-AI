#!/usr/bin/env python3
import socket, json, sys
import os

def main():
    try:
        cmd = sys.argv[1]
        exit_code = sys.argv[2] if len(sys.argv) > 2 else None
        shell = sys.argv[3] if len(sys.argv) > 3 else None
        cwd = sys.argv[4] if len(sys.argv) > 4 else None
        ts = int(sys.argv[5]) if len(sys.argv) > 5 else int(__import__('time').time())
        phase = sys.argv[6] if len(sys.argv) > 6 else None
    except Exception:
        return
    obj = {
        "command": cmd,
        "exit_code": int(exit_code) if exit_code is not None and exit_code != '' else None,
        "shell": shell,
        "cwd": cwd,
        "timestamp": ts,
        "phase": phase,
    }
    try:
        s = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        s.connect('/tmp/activity_monitor.sock')
        s.sendall((json.dumps(obj) + '\n').encode())
        s.close()
    except Exception:
        pass

if __name__ == '__main__':
    main()
