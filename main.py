#!/usr/bin/env python3
"""
Raspberry Pi 5 Video Recorder with Automatic AWS S3 Region Detection
and Flexible Filename Placeholders
"""

import json
import os
import time
import configparser
from datetime import datetime
from pathlib import Path

from api_client import create_api_client
from schedule import (
    _clear_at_queue,
    is_within_business_hours,
    resolve_business_hours,
    schedule_at,
    seconds_until_end_time,
)
from video_recorder import CACHE_SETTINGS_PATH, VideoRecorder


def _load_settings(config):
    """Store settings from the API, falling back to cache/settings.json.

    Returns (settings dict or None, api_ok bool).
    """
    api_client = create_api_client(config, None)
    settings = None
    api_ok = False
    if api_client:
        settings = api_client.get_store_settings()
        api_ok = settings is not None
    if not settings and CACHE_SETTINGS_PATH.exists():
        try:
            with open(CACHE_SETTINGS_PATH, "r", encoding="utf-8") as f:
                settings = json.load(f)
        except Exception:
            pass
    return settings, api_ok


def _run_test(config_file="config.conf"):
    """Check API and S3 connectivity, show all settings."""
    config = configparser.ConfigParser()
    config.read(config_file)
    print("\n=== Pi Video Recorder - Connectivity Test ===\n")

    # API test
    settings, api_ok = _load_settings(config)
    print(f"API:  {'OK' if api_ok else 'FAILED (using cache)' if settings else 'FAILED'}")

    # S3 test
    s3_ok = False
    if settings:
        try:
            rec = settings.get("RECORDING", {})
            loc = str(rec.get("s3-location", "")).strip()
            if loc.startswith("s3://"):
                loc = loc[5:]
            bucket = loc.split("/", 1)[0] if loc else None
            if bucket:
                import boto3
                from botocore.config import Config
                cfg_probe = Config(signature_version="s3v4", max_pool_connections=16)
                # Prefer credentials from config [aws] (store settings), else boto3 default chain.
                access_key = config.get("aws", "access_key", fallback="").strip() if config.has_section("aws") else ""
                secret_key = config.get("aws", "secret_key", fallback="").strip() if config.has_section("aws") else ""
                session_token = config.get("aws", "session_token", fallback="").strip() if config.has_section("aws") else ""
                default_region = config.get("aws", "default_region", fallback="").strip() if config.has_section("aws") else ""
                creds = {}
                if access_key and secret_key:
                    creds = {"aws_access_key_id": access_key, "aws_secret_access_key": secret_key}
                    if session_token:
                        creds["aws_session_token"] = session_token
                base_region = default_region or "us-east-1"

                client = boto3.Session(**creds).client("s3", region_name=base_region, config=cfg_probe)
                try:
                    region_resp = client.get_bucket_location(Bucket=bucket)
                finally:
                    try:
                        client.close()
                    except Exception:
                        pass
                region = region_resp.get("LocationConstraint") or "us-east-1"
                regional = boto3.Session(**creds).client(
                    "s3",
                    region_name=region,
                    config=cfg_probe,
                )
                try:
                    regional.head_bucket(Bucket=bucket)
                finally:
                    try:
                        regional.close()
                    except Exception:
                        pass
                s3_ok = True
                print(f"S3:   OK (bucket={bucket}, region={region})")
            else:
                print("S3:   SKIPPED (no s3-location in settings)")
        except Exception as e:
            print(f"S3:   FAILED - {e}")
    else:
        print("S3:   SKIPPED (no settings)")

    # Show settings (mask sensitive)
    if settings:
        print("\n--- Settings ---")
        _mask = {"password", "secret-key", "access-key", "secret_key", "access_key"}
        def _mask_dict(d):
            out = {}
            for k, v in d.items():
                key = k.replace("-", "_").lower()
                if any(m in key for m in _mask) and isinstance(v, str) and len(v) > 4:
                    out[k] = "***"
                elif isinstance(v, dict):
                    out[k] = _mask_dict(v)
                else:
                    out[k] = v
            return out
        print(json.dumps(_mask_dict(settings), indent=2))
    print("\n===========================================\n")
    return 0 if (settings and (api_ok or s3_ok)) else 1


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Pi Video Recorder Uploader")
    parser.add_argument("--config", default="config.conf", help="Configuration file path")
    parser.add_argument("--single", action="store_true", help="Record single video instead of continuous")
    parser.add_argument("--test", action="store_true", help="Test API and S3 connectivity, show settings")
    parser.add_argument("--reconcile", action="store_true",
                        help="Upload all clips still saved on the SD card (pending + dead-letter), then exit. Does not record.")
    parser.add_argument("--imx500", action="store_true", help="Overlay IMX500 bounding boxes on recorded frames (if metadata is present)")
    parser.add_argument("--ignore-hours", action="store_true",
                        help="Record at any hour, ignoring BUSINESS_HOUR and [business_hour] in config.conf")
    args = parser.parse_args()

    if args.test:
        return _run_test(args.config)

    if args.reconcile:
        try:
            recorder = VideoRecorder(args.config, upload_only=True)
            n = recorder.reconcile()
            print(f"Reconcile complete: re-queued {n} clip(s).")
            return 0
        except KeyboardInterrupt:
            print("\nReconcile interrupted by user")
            return 1
        except Exception as e:
            print(f"Reconcile error: {e}")
            import traceback
            traceback.print_exc()
            return 1

    # Business-hours gate. Checked before VideoRecorder is built: that constructor blocks
    # on S3 init and a pending-upload retry pass, which we must not pay just to find out
    # we are outside the window. --single keeps its old unconditional behavior.
    deadline_ts = None
    if not args.single:
        gate_config = configparser.ConfigParser()
        gate_config.read(args.config)
        settings, _ = _load_settings(gate_config)
        start, end = resolve_business_hours(gate_config, settings, ignore=args.ignore_hours)

        if start and end:
            now = datetime.now()
            if not is_within_business_hours(now, *start, *end):
                print(f"Outside business hours, scheduling for start-time ({start[0]:02d}:{start[1]:02d})")
                schedule_at(start[0], start[1], Path(__file__).resolve())
                return 0
            remaining = seconds_until_end_time(now, *end)
            if remaining <= 0:
                print("End-time already reached; nothing to record")
                return 0
            # Drop a stale queue-'a' job left over from before a reboot, so it cannot
            # start a second recorder that fights this one for the camera.
            _clear_at_queue("a")
            deadline_ts = time.time() + remaining
            print(f"Recording until end-time ({remaining}s remaining)")

    pid_file = Path(__file__).resolve().parent / ".pid"
    try:
        pid_file.write_text(str(os.getpid()), encoding="utf-8")
        try:
            recorder = VideoRecorder(args.config, imx500_overlay=args.imx500)

            if args.single:
                recorder.record_single_video()
            else:
                recorder.start_continuous_recording(deadline_ts=deadline_ts)
                if deadline_ts is not None:
                    print("Session ended at end-time; exiting")

        except KeyboardInterrupt:
            print("\nRecording stopped by user")
        except Exception as e:
            print(f"Fatal error: {e}")
            import traceback
            traceback.print_exc()
            return 1
        return 0
    finally:
        try:
            pid_file.unlink(missing_ok=True)
        except OSError:
            pass


if __name__ == "__main__":
    main()
