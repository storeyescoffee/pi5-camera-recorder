#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE="$SCRIPT_DIR/.pid"

if [[ ! -f "$PID_FILE" ]]; then
  echo "No PID file: $PID_FILE" >&2
  exit 1
fi

pid="$(tr -d ' \n\r\t' < "$PID_FILE")"
if ! [[ "$pid" =~ ^[0-9]+$ ]]; then
  echo "Invalid PID in $PID_FILE: $pid" >&2
  exit 1
fi

if ! kill -0 "$pid" 2>/dev/null; then
  echo "Process $pid is not running; removing stale $PID_FILE" >&2
  rm -f "$PID_FILE"
  exit 0
fi

kill -KILL "$pid"
rm -f "$PID_FILE"
echo "Sent SIGKILL to $pid"
