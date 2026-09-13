#!/bin/bash

sudo apt install -y \
  python3-av \
  python3-picamera2 \
  python3-boto3 \
  ffmpeg \
  at


# at(1) schedules the next recording-session start; cron runs the @reboot entry below
sudo systemctl enable --now atd >/dev/null 2>&1 || true
sudo systemctl enable --now cron >/dev/null 2>&1 || true

# Ensure exactly one @reboot cron entry. Extra arguments are passed to main.py, so the mode
# survives a reboot (e.g. ./install.sh -v2). Any earlier variant of the entry is replaced.
args=""
if [ $# -gt 0 ]; then
    args=" $(printf '%q ' "$@")"
fi
CRON_LINE="@reboot cd \$HOME/pi5-camera-recorder && python3 main.py${args}"
(crontab -l 2>/dev/null | grep -Fv '@reboot cd $HOME/pi5-camera-recorder && python3 main.py'; echo "$CRON_LINE") | crontab -

# Make start.sh and stop.sh executable
chmod +x start.sh stop.sh