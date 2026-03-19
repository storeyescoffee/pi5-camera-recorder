#!/bin/bash

sudo apt install -y \
  python3-av \
  python3-picamera2 \
  python3-boto3 \
  ffmpeg \
  at


# Ensure @reboot cron entry exists (no duplicates)
CRON_LINE='@reboot cd $HOME/pi5-camera-recorder && python3 main.py'
(crontab -l 2>/dev/null | grep -Fv "$CRON_LINE"; echo "$CRON_LINE") | crontab -

# Start once after 1 minute using at
echo "cd $HOME/pi5-camera-recorder && python3 main.py" | at now + 1 minute
