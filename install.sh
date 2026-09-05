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

# Ensure @reboot cron entry exists (no duplicates)
CRON_LINE='@reboot cd $HOME/pi5-camera-recorder && python3 main.py'
(crontab -l 2>/dev/null | grep -Fv "$CRON_LINE"; echo "$CRON_LINE") | crontab -

# Make start.sh and stop.sh executable
chmod +x start.sh stop.sh