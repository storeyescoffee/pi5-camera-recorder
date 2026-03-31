#!/usr/bin/env python3
"""
Raspberry Pi 5 Video Recorder with Automatic AWS S3 Region Detection
and Flexible Filename Placeholders
"""

import json
import os
import configparser
from pathlib import Path

from api_client import create_api_client
from video_recorder import CACHE_SETTINGS_PATH, VideoRecorder


def _run_test(config_file="config.conf"):
    """Check API and S3 connectivity, show all settings."""
    config = configparser.ConfigParser()
    config.read(config_file)
    print("\n=== Pi Video Recorder - Connectivity Test ===\n")

    # API test
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
                from botocore.client import Config
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

                client = boto3.client("s3", region_name=base_region, config=Config(signature_version="s3v4"), **creds)
                region_resp = client.get_bucket_location(Bucket=bucket)
                region = region_resp.get("LocationConstraint") or "us-east-1"
                regional = boto3.client("s3", region_name=region, config=Config(signature_version="s3v4"), **creds)
                regional.head_bucket(Bucket=bucket)
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
    args = parser.parse_args()

    if args.test:
        return _run_test(args.config)

    pid_file = Path(__file__).resolve().parent / ".pid"
    try:
        pid_file.write_text(str(os.getpid()), encoding="utf-8")
        try:
            recorder = VideoRecorder(args.config)

            if args.single:
                recorder.record_single_video()
            else:
                recorder.start_continuous_recording()

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
