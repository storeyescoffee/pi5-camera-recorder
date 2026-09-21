"""Business hours and at(1) scheduling for the recording session.

Ported from pi0-camera-recorder/src/schedule.py. Queue convention: at queue 'a'
holds the next recording-session start; nothing else uses that queue.
"""

import logging
import shutil
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)


def parse_time(s):
    """Parse 'HH:MM' (or bare 'HH') to (hour, minute)."""
    parts = str(s).strip().split(":")
    return int(parts[0]), int(parts[1]) if len(parts) > 1 else 0


def is_within_business_hours(now, start_h, start_m, end_h, end_m):
    """
    Check if now is within business hours.
    Handles: 07:00-23:00 (same day) and 17:00-00:00 / 17:00-02:00 (overnight).
    """
    now_mins = now.hour * 60 + now.minute
    start_mins = start_h * 60 + start_m
    end_mins = end_h * 60 + end_m

    if start_mins < end_mins:
        return start_mins <= now_mins < end_mins
    return now_mins >= start_mins or now_mins < end_mins


def seconds_until_end_time(now, end_h, end_m):
    """
    Seconds from now until end-time. If end crosses midnight (e.g. 02:00),
    end is interpreted as next occurrence.
    """
    today_end = now.replace(hour=end_h, minute=end_m, second=0, microsecond=0)
    if end_h * 60 + end_m > now.hour * 60 + now.minute:
        delta = today_end - now
    else:
        delta = today_end + timedelta(days=1) - now
    return max(0, int(delta.total_seconds()))


def next_start_datetime(now, start_h, start_m):
    """Next occurrence of start-time (today or tomorrow)."""
    today_start = now.replace(hour=start_h, minute=start_m, second=0, microsecond=0)
    if today_start > now:
        return today_start
    return today_start + timedelta(days=1)


def _clear_at_queue(queue="a"):
    """Remove existing at jobs in the given queue, leaving other queues untouched."""
    try:
        result = subprocess.run(
            ["atq"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode != 0:
            return
        for line in result.stdout.strip().splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[0].isdigit() and parts[-2] == queue:
                subprocess.run(
                    ["atrm", parts[0]],
                    capture_output=True,
                    timeout=5,
                )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        pass


def schedule_at(start_h, start_m, script_path=None):
    """Schedule main.py to run at start-time using at(1), queue 'a'.

    Drops existing queue-'a' jobs first. A missing at(1) is logged and ignored —
    the recorder must not die because scheduling is unavailable.
    """
    if shutil.which("at") is None:
        logger.error("'at' command not found. Install with: apt install at")
        return

    _clear_at_queue("a")

    script = Path(script_path) if script_path else Path(__file__).resolve().parent / "main.py"
    cmd = f"cd {script.parent} && python3 {script}"

    now = datetime.now()
    start_dt = next_start_datetime(now, start_h, start_m)

    if start_dt.date() == now.date():
        at_spec = f"{start_h:02d}:{start_m:02d}"
    else:
        at_spec = f"{start_h:02d}:{start_m:02d} tomorrow"

    proc = subprocess.run(
        ["at", "-q", "a", at_spec],
        input=cmd.encode(),
        capture_output=True,
    )
    if proc.returncode != 0:
        logger.error("Failed to schedule at %s: %s", at_spec, proc.stderr.decode())
        return
    logger.info("Scheduled next run at %s", at_spec)


def resolve_business_hours(config, settings=None, ignore=False):
    """Return ((start_h, start_m), (end_h, end_m)), or (None, None) when no window applies.

    Precedence:
      --ignore-hours / [business_hour] ignore_hours -> (None, None)
      API BUSINESS_HOUR.start-time / end-time
      config.conf [business_hour] start_time / end_time
      otherwise (None, None), i.e. record 24/7

    Unparseable values log a warning and fall through to the next source.
    """
    if ignore:
        logger.info("Business hours ignored (--ignore-hours)")
        return None, None

    if config.has_section("business_hour"):
        try:
            if config.getboolean("business_hour", "ignore_hours", fallback=False):
                logger.info("Business hours ignored ([business_hour] ignore_hours)")
                return None, None
        except ValueError:
            logger.warning("[business_hour] ignore_hours is not a boolean; ignoring it")

    sources = []
    if isinstance(settings, dict) and isinstance(settings.get("BUSINESS_HOUR"), dict):
        bh = settings["BUSINESS_HOUR"]
        sources.append(("API BUSINESS_HOUR", bh.get("start-time"), bh.get("end-time")))
    if config.has_section("business_hour"):
        sources.append((
            "config.conf [business_hour]",
            config.get("business_hour", "start_time", fallback=""),
            config.get("business_hour", "end_time", fallback=""),
        ))

    for origin, raw_start, raw_end in sources:
        start_s = str(raw_start or "").strip()
        end_s = str(raw_end or "").strip()
        if not start_s or not end_s:
            continue
        try:
            start = parse_time(start_s)
            end = parse_time(end_s)
        except (ValueError, IndexError):
            logger.warning("%s has unparseable hours (%r - %r); skipping", origin, start_s, end_s)
            continue
        if not (0 <= start[0] <= 23 and 0 <= start[1] <= 59 and 0 <= end[0] <= 23 and 0 <= end[1] <= 59):
            logger.warning("%s has out-of-range hours (%r - %r); skipping", origin, start_s, end_s)
            continue
        logger.info("Business hours from %s: %02d:%02d - %02d:%02d", origin, *start, *end)
        return start, end

    logger.info("No business hours configured; recording continuously")
    return None, None
