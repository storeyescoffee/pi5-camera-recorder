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

# One @reboot line in /etc/cron.d, named after this folder. Unlike a user crontab it survives
# a re-image of the user account and is visible as a plain file. A cron.d entry carries a user
# field and needs a trailing newline; the filename must hold only [A-Za-z0-9_-] or cron skips it.
# Extra arguments are passed to main.py, so the mode survives a reboot (e.g. ./install.sh --imx500).
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CRON_NAME="$(basename "$PROJECT_DIR")"
CRON_NAME="${CRON_NAME//[^A-Za-z0-9_-]/-}"   # bash, not tr -c: tr would also map the trailing newline
CRON_FILE="/etc/cron.d/$CRON_NAME"
args=""
if [ $# -gt 0 ]; then
    args=" $(printf '%q ' "$@")"
    args="${args% }"
fi
printf '@reboot %s cd %s && python3 main.py%s\n'   "$(id -un)" "$(printf '%q' "$PROJECT_DIR")" "$args" | sudo tee "$CRON_FILE" >/dev/null
sudo chown root:root "$CRON_FILE"
sudo chmod 644 "$CRON_FILE"
echo "Installed $CRON_FILE:"
cat "$CRON_FILE"

# Drop the @reboot line older installs put in the user crontab, or the recorder starts twice.
OLD_ENTRY='@reboot cd $HOME/pi5-camera-recorder && python3 main.py'
if crontab -l 2>/dev/null | grep -Fq "$OLD_ENTRY"; then
    crontab -l 2>/dev/null | grep -Fv "$OLD_ENTRY" | crontab -
    echo "Removed the old @reboot entry from the user crontab"
fi

# Make start.sh and stop.sh executable
chmod +x start.sh stop.sh