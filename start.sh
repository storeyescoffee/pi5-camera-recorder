#!/bin/bash

# Start once after 1 minute using at. Extra arguments are passed to main.py (e.g. ./start.sh --imx500)
args=""
if [ $# -gt 0 ]; then
    args=" $(printf '%q ' "$@")"
fi
echo "cd $HOME/pi5-camera-recorder && python3 main.py${args}" | at now
