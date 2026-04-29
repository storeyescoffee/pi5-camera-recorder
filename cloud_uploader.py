#!/usr/bin/env python3
"""
Cloud Upload Module for Raspberry Pi 5 Video Recorder
Handles all S3/GCS upload functionality with retry logic and concurrent uploads.
"""

import csv
import os
import random
import time
import threading
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import boto3
from botocore.config import Config

from api_client import create_api_client


class CloudUploader:
    """Handles cloud storage uploads with retry logic and concurrent upload management."""
    
    def __init__(self, config, logger=None):
        """
        Initialize CloudUploader.
        
        Args:
            config: ConfigParser object with GCS/S3 settings
            logger: Optional logger instance (creates one if not provided)
        """
        self.config = config
        self.logger = logger or self._setup_logging()

        self.active_uploads = 0
        self.upload_lock = threading.Lock()
        self.csv_lock = threading.Lock()
        self._reinit_lock = threading.Lock()  # serialize S3 close/reinit across upload worker threads
        self.s3_client = None
        self.s3_region = None
        self._boto_session = None  # boto3.Session — one session, one pooled S3 client
        self._upload_executor = None  # bounded thread pool for uploads (never one OS thread per clip)
        self._upload_executor_workers = None
        self._upload_slot_n = None
        self._upload_busy = None  # threading.Semaphore(max_concurrent) — backpressure on queue
        self.api_client = create_api_client(config, logger)

        self._load_config()
        self._ensure_upload_limits()
        self._ensure_upload_executor_locked()
        self._init_s3_client()
    
    def _get_aws_credentials_kwargs(self):
        """Return boto3 credential kwargs from config, if provided.

        Supports settings API keys copied into config section [aws]:
        - access_key / access-key
        - secret_key / secret-key
        - session_token / session-token (optional)
        """
        if not self.config.has_section("aws"):
            return {}
        # video_recorder copies API keys with '-' replaced by '_', but accept both for robustness.
        access_key = (self.config.get("aws", "access_key", fallback="") or self.config.get("aws", "access-key", fallback="")).strip()
        secret_key = (self.config.get("aws", "secret_key", fallback="") or self.config.get("aws", "secret-key", fallback="")).strip()
        session_token = (self.config.get("aws", "session_token", fallback="") or self.config.get("aws", "session-token", fallback="")).strip()

        if access_key and secret_key:
            out = {"aws_access_key_id": access_key, "aws_secret_access_key": secret_key}
            if session_token:
                out["aws_session_token"] = session_token
            return out
        return {}

    def _get_aws_default_region(self):
        if not self.config.has_section("aws"):
            return ""
        return (self.config.get("aws", "default_region", fallback="") or self.config.get("aws", "default-region", fallback="")).strip()

    def _setup_logging(self):
        """Setup logging if not provided."""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger
    
    def _load_config(self):
        """Load cloud storage configuration."""
        try:
            self.endpoint_url = self.config.get("gcs", "endpoint_url")
            bl = self.config.get("gcs", "bucket_location").strip()
            self.bucket_name = bl.split("/", 1)[0] if bl else self.config.get("gcs", "bucket_name")
            self.region = self.config.get("gcs", "region", fallback="us-east-1")
            
            # Recording settings needed for upload
            self.max_concurrent_uploads = int(self.config.get("recording", "max_concurrent_uploads", fallback="3"))
            self.delete_after_upload = self.config.getboolean("recording", "delete_after_upload", fallback=False)
            self.recording_duration = int(self.config.get("recording", "duration_minutes"))
            self.bitrate = int(self.config.get("camera", "bitrate"))
            self.pending_uploads_csv = self.config.get("recording", "pending_uploads_csv", fallback="./pending_uploads.csv")
            self.pending_retry_interval_minutes = int(self.config.get("recording", "pending_retry_interval_minutes", fallback="10"))
            # urllib3 pool used by boto3 (avoid CLOSE-WAIT / pool exhaustion when max_concurrent_uploads > default 10)
            _mc = max(1, self.max_concurrent_uploads)
            _default_pool = str(max(_mc * 4, 32))
            self.s3_pool_connections = int(self.config.get("recording", "s3_pool_connections", fallback=_default_pool))
            # App-level upload retries (distinct from botocore's internal retries on single request)
            self.upload_retry_backoff_initial = float(
                self.config.get("recording", "upload_retry_backoff_initial_sec", fallback="2")
            )
            self.upload_retry_backoff_cap = float(self.config.get("recording", "upload_retry_backoff_cap_sec", fallback="120"))
            
            self.video_url_base = self.config.get("api", "video_url_base", fallback="").strip() if self.config.has_section("api") else ""
            
            self.logger.info("Cloud upload configuration loaded successfully.")
            creds = self._get_aws_credentials_kwargs()
            if creds:
                self.logger.info("Using AWS credentials from store settings ([aws] section)")
            else:
                self.logger.info("Using AWS credentials from system default credential chain")
        except Exception as e:
            self.logger.error(f"Error loading cloud upload configuration: {e}", exc_info=True)
            raise

    def _make_botocore_config(self):
        """Shared botocore Config: urllib3 pool size, timeouts, retries (reduces CLOSE-WAIT / stalls)."""
        pool = max(self.s3_pool_connections, self.max_concurrent_uploads * 4, 32)
        retries = {"max_attempts": 10, "mode": "standard"}
        kw = dict(
            signature_version="s3v4",
            max_pool_connections=pool,
            retries=retries,
            connect_timeout=60,
            read_timeout=300,
        )
        try:
            return Config(**kw, tcp_keepalive=True)
        except TypeError:
            return Config(**kw)

    def _close_s3_connection(self):
        """Release urllib3 pools and CLOSE-WAIT sockets before creating a new client."""
        client = getattr(self, "s3_client", None)
        if client is not None:
            try:
                client.close()
            except Exception:
                pass
        self.s3_client = None
        self._boto_session = None

    def _ensure_upload_executor_locked(self):
        """Rebuild ThreadPoolExecutor when max_concurrent_uploads changes (under caller coordination)."""
        n = max(1, int(self.max_concurrent_uploads))
        if self._upload_executor_workers == n and self._upload_executor is not None:
            return
        if self._upload_executor is not None:
            self._upload_executor.shutdown(wait=True, cancel_futures=False)
            self._upload_executor = None
        self._upload_executor_workers = n
        self._upload_executor = ThreadPoolExecutor(
            max_workers=n,
            thread_name_prefix="s3-upload",
        )
        self.logger.info(f"[UPLOAD] Thread pool ready: max_workers={n}")

    def _ensure_upload_limits(self):
        """Recreate semaphore when max_concurrent_uploads changes (blocks producers like the old busy-wait loop)."""
        n = max(1, int(self.max_concurrent_uploads))
        if getattr(self, "_upload_slot_n", None) == n and self._upload_busy is not None:
            return
        self._upload_slot_n = n
        self._upload_busy = threading.Semaphore(n)

    def reload_settings(self):
        """Reload configuration from config (e.g. after sync-settings MQTT message)."""
        self._load_config()
        with self.upload_lock:
            self._ensure_upload_limits()
            self._ensure_upload_executor_locked()
            self._close_s3_connection()
        self._init_s3_client()
        self.logger.info("[STORE] Cloud upload settings reloaded")
    
    def _init_s3_client(self, max_retries=5, retry_delay=10):
        """Create one boto3 Session + one regional S3 client; close discovery client to avoid leaking sockets."""
        self._close_s3_connection()
        cfg = self._make_botocore_config()
        creds = self._get_aws_credentials_kwargs()
        base_region = self._get_aws_default_region() or "us-east-1"

        for attempt in range(1, max_retries + 1):
            try:
                self._boto_session = boto3.Session(**creds)

                base_client = self._boto_session.client("s3", region_name=base_region, config=cfg)
                try:
                    region_resp = base_client.get_bucket_location(Bucket=self.bucket_name)
                finally:
                    try:
                        base_client.close()
                    except Exception:
                        pass

                actual_region = region_resp.get("LocationConstraint") or "us-east-1"
                self.logger.info(f"Detected S3 bucket region: {actual_region}")

                endpoint = f"https://s3.{actual_region}.amazonaws.com"
                self.logger.info(f"Using regional S3 endpoint: {endpoint}")

                self.s3_client = self._boto_session.client(
                    "s3",
                    endpoint_url=endpoint,
                    region_name=actual_region,
                    config=cfg,
                )
                self.s3_region = actual_region

                self.logger.info("✅ S3 client initialized successfully (singleton per process, pooled connections).")
                return True
            except Exception as e:
                self.logger.error(f"Error initializing S3 client (attempt {attempt}/{max_retries}): {e}")
                self._close_s3_connection()
                if attempt < max_retries:
                    self.logger.info(f"Retrying S3 initialization in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error("Failed to initialize S3 client after all retries. Uploads will be disabled.")
                    self.logger.error(
                        "Make sure AWS credentials are configured (environment variables, IAM role, or ~/.aws/credentials)"
                    )
                    self.s3_client = None
                    return False

    def _reinit_s3_client(self):
        """Reinitialize S3 client (serialized — avoids concurrent close/init from multiple upload threads)."""
        acquired = self._reinit_lock.acquire(blocking=False)
        if not acquired:
            self.logger.info("[UPLOAD] S3 reinit already in progress, waiting...")
            with self._reinit_lock:
                pass
            return
        try:
            self._close_s3_connection()
            self.logger.info("[UPLOAD] Reinitializing S3 client...")
            self._init_s3_client(max_retries=3, retry_delay=min(60, float(self.upload_retry_backoff_cap)))
        finally:
            self._reinit_lock.release()
    
    PENDING_CSV_HEADER = ["local_path", "s3_key", "filename", "failed_at", "video_code"]

    def _migrate_pending_csv_if_needed(self):
        """If CSV has old header (no video_code), migrate to new format."""
        if not os.path.exists(self.pending_uploads_csv):
            return
        with open(self.pending_uploads_csv, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
            rows = list(reader)
        if "video_code" in fieldnames:
            return
        for row in rows:
            row["video_code"] = row.get("video_code", "")
        with open(self.pending_uploads_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.PENDING_CSV_HEADER, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        self.logger.info("[PENDING] Migrated CSV to include video_code column")

    def _add_pending_upload(self, local_path, s3_key, filename, video_code=None):
        """Append failed upload info to CSV for later retry. video_code stored for fallback PUT."""
        with self.csv_lock:
            try:
                self._migrate_pending_csv_if_needed()
                file_exists = os.path.exists(self.pending_uploads_csv)
                with open(self.pending_uploads_csv, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(self.PENDING_CSV_HEADER)
                    writer.writerow([local_path, s3_key, filename, datetime.now().isoformat(), video_code or ""])
                self.logger.info(f"[PENDING] Added to retry list: {filename}" + (f" (video_code={video_code})" if video_code else ""))
            except Exception as e:
                self.logger.error(f"[PENDING] Failed to write to CSV: {e}", exc_info=True)
    
    def _remove_pending_upload(self, local_path):
        """Remove entry from CSV after successful upload."""
        with self.csv_lock:
            try:
                if not os.path.exists(self.pending_uploads_csv):
                    return
                rows = []
                with open(self.pending_uploads_csv, "r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    fieldnames = reader.fieldnames or []
                    for row in reader:
                        if row.get("local_path") != local_path:
                            rows.append(row)
                with open(self.pending_uploads_csv, "w", newline="", encoding="utf-8") as f:
                    if rows and fieldnames:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(rows)
                self.logger.debug(f"[PENDING] Removed from retry list: {local_path}")
            except Exception as e:
                self.logger.error(f"[PENDING] Failed to update CSV: {e}", exc_info=True)
    
    def _build_video_url(self, s3_key):
        """Build full video URL from s3_key. Uses video_url_base if set, else S3 public URL."""
        key = s3_key.lstrip("/")
        if self.video_url_base:
            base = self.video_url_base.rstrip("/")
            return f"{base}/{key}" if key else base
        region = self.s3_region or self.region
        return f"https://{self.bucket_name}.s3.{region}.amazonaws.com/{key}"

    def _retry_delays_seconds(self, attempt_index_1_based):
        """Exponential backoff with jitter between app-level retries (attempt 1→2, etc.)."""
        cap = float(self.upload_retry_backoff_cap)
        base = float(self.upload_retry_backoff_initial)
        exp = min(cap, base * (2 ** max(0, attempt_index_1_based - 1)))
        jitter_max = min(30.0, exp * 0.25 + 1.0)
        return exp + random.uniform(0, jitter_max)

    def _upload_job_impl(
        self, local_path, s3_key, filename, max_retries=3, is_fallback=False, existing_video_code=None, start_time=None
    ):
        """Upload file to S3 with retry logic, metadata, and detailed statistics.

        is_fallback: True when retrying from pending CSV.
        existing_video_code: From CSV when POST succeeded but upload failed (scenario 1).
        start_time: Recording start time (datetime); if None, derived from file mtime - duration for retries.
        """
        if self.s3_client is None:
            self.logger.error(f"[UPLOAD] S3 client unavailable, cannot upload {s3_key}")
            self._add_pending_upload(local_path, s3_key, filename, video_code=existing_video_code)
            return

        if not os.path.exists(local_path):
            self.logger.error(f"[UPLOAD] File not found: {local_path}")
            self._remove_pending_upload(local_path)
            return

        t0 = time.time()
        size_mb = 0.0
        try:
            file_size = os.path.getsize(local_path)
            duration_seconds = self.recording_duration * 60
            video_code = existing_video_code
            # Derive start_time from file mtime if not provided (e.g. retry from pending)
            if start_time is None:
                mtime = os.path.getmtime(local_path)
                start_time = datetime.fromtimestamp(mtime) - timedelta(seconds=duration_seconds)

            if is_fallback:
                if video_code:
                    # Scenario 1: POST succeeded, upload failed. Use stored video_code.
                    if self.api_client:
                        self.api_client.put_main_video(video_code, 0, 0.0, 1, "UPLOADING_FALLBACK")
                else:
                    # Scenario 2: Internet was down, POST failed. Resend POST.
                    if self.api_client:
                        video_url = self._build_video_url(s3_key)
                        video_code = self.api_client.post_main_video(video_url, file_size, duration_seconds, start_time, hour=start_time.hour)
                        if not video_code:
                            self.logger.warning("[API] POST main-video failed on retry")
            elif self.api_client:
                # Normal flow: POST first
                video_url = self._build_video_url(s3_key)
                video_code = self.api_client.post_main_video(video_url, file_size, duration_seconds, start_time, hour=start_time.hour)
                if not video_code:
                    self.logger.warning("[API] POST main-video failed, continuing upload without backend notification")

            for attempt in range(1, max_retries + 1):
                try:
                    size_mb = file_size / (1024 * 1024)
                    self.logger.info(f"[UPLOAD] Starting upload: {filename} -> {s3_key}")
                    self.logger.info(f"[UPLOAD] File size: {size_mb:.2f} MB - Attempt {attempt}/{max_retries}")
                    t0 = time.time()
                    
                    # Upload with metadata
                    self.s3_client.upload_file(
                        local_path,
                        self.bucket_name,
                        s3_key,
                        ExtraArgs={
                            "ContentType": "video/mp4",
                            "Metadata": {
                                "recorded_at": datetime.now().isoformat(),
                                "duration_minutes": str(self.recording_duration),
                                "file_size_bytes": str(file_size),
                                "bitrate_bps": str(self.bitrate)
                            }
                        }
                    )
                    
                    dt = time.time() - t0
                    upload_speed_mbps = (size_mb * 8) / dt if dt > 0 else 0
                    self.logger.info(f"[UPLOAD] Upload completed: {s3_key}")
                    self.logger.info(f"[UPLOAD] Upload time: {dt:.1f} seconds ({dt/60:.1f} minutes)")
                    self.logger.info(f"[UPLOAD] Upload speed: {upload_speed_mbps:.2f} Mbps")
                    self.logger.info(f"[UPLOAD] Average upload rate: {size_mb/dt:.2f} MB/s")
                    
                    if self.api_client and video_code:
                        status = "COMPLETED_FALLBACK" if is_fallback else "COMPLETED"
                        self.api_client.put_main_video(video_code, int(dt), upload_speed_mbps, attempt, status)
                    
                    # Remove from pending CSV if it was a retry
                    self._remove_pending_upload(local_path)
                    # Delete local file after successful upload if configured
                    if self.delete_after_upload:
                        try:
                            os.remove(local_path)
                            self.logger.info(f"[UPLOAD] Deleted local file: {local_path}")
                        except Exception as e:
                            self.logger.error(f"[UPLOAD] Failed to delete local file {local_path}: {e}", exc_info=True)
                    
                    break  # Success, exit retry loop
                except Exception as e:
                    self.logger.error(f"[UPLOAD] Failed for {s3_key} (attempt {attempt}/{max_retries}): {e}", exc_info=True)
                    if attempt < max_retries:
                        self._close_s3_connection()
                        self._reinit_s3_client()
                        wait_time = self._retry_delays_seconds(attempt)
                        self.logger.info(f"[UPLOAD] Retrying upload in {wait_time:.1f} seconds...")
                        time.sleep(wait_time)
                        if self.s3_client is None:
                            continue
                    else:
                        self.logger.error(f"[UPLOAD] Failed after {max_retries} attempts. File: {local_path}")
                        self._add_pending_upload(local_path, s3_key, filename, video_code=video_code)
                        if self.api_client and video_code:
                            dt = time.time() - t0
                            upload_speed_mbps = (size_mb * 8) / dt if dt > 0 else 0
                            self.api_client.put_main_video(video_code, int(dt), upload_speed_mbps, max_retries, "FAILED")

        except Exception as e:
            self.logger.error(f"[UPLOAD] Unexpected error during upload pipeline for {filename}: {e}", exc_info=True)
            raise

    def upload_file(self, local_path, s3_key, filename, is_fallback=False, video_code=None, start_time=None):
        """Queue one upload: bounded ThreadPoolExecutor plus semaphore (capacity = max concurrent)."""
        self._ensure_upload_limits()
        self._ensure_upload_executor_locked()
        if self.s3_client is None:
            self.logger.warning(f"[UPLOAD] S3 client unavailable, skipping upload for {filename}")
            self._reinit_s3_client()
        if self.s3_client is None:
            self.logger.error(f"[UPLOAD] Cannot queue upload (no S3 client): {filename}")
            return

        self._upload_busy.acquire()
        with self.upload_lock:
            self.active_uploads += 1

        def run():
            try:
                self._upload_job_impl(
                    local_path,
                    s3_key,
                    filename,
                    is_fallback=is_fallback,
                    existing_video_code=video_code or None,
                    start_time=start_time,
                )
            finally:
                with self.upload_lock:
                    self.active_uploads -= 1
                self._upload_busy.release()
                self.logger.info(f"[UPLOAD] Upload task finished for {filename}")

        try:
            self._upload_executor.submit(run)
        except Exception as e:
            with self.upload_lock:
                self.active_uploads -= 1
            self._upload_busy.release()
            self.logger.error(f"[UPLOAD] Failed to queue upload for {filename}: {e}", exc_info=True)
            raise
        with self.upload_lock:
            n = self.active_uploads
        self.logger.info(f"[UPLOAD] Upload queued for {filename} (tracked active: {n})")
    
    def retry_pending_uploads(self):
        """Retry all uploads listed in pending_uploads.csv. Removes entries for missing files.
        Uses UPLOADING_FALLBACK then COMPLETED_FALLBACK when retrying."""
        if not os.path.exists(self.pending_uploads_csv):
            self.logger.info("[PENDING] No pending uploads file found")
            return 0
        with self.csv_lock:
            self._migrate_pending_csv_if_needed()
            try:
                with open(self.pending_uploads_csv, "r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
            except Exception as e:
                self.logger.error(f"[PENDING] Failed to read CSV: {e}", exc_info=True)
                return 0
        if not rows:
            return 0
        self.logger.info(f"[PENDING] Retrying {len(rows)} pending upload(s)...")
        count = 0
        for row in rows:
            local_path = row.get("local_path", "")
            s3_key = row.get("s3_key", "")
            filename = row.get("filename", os.path.basename(local_path))
            if not local_path or not s3_key:
                continue
            if os.path.exists(local_path):
                vc = (row.get("video_code") or "").strip() or None
                self.upload_file(local_path, s3_key, filename, is_fallback=True, video_code=vc)
                count += 1
            else:
                self.logger.warning(f"[PENDING] Skipping missing file, removing from list: {local_path}")
                self._remove_pending_upload(local_path)
        if count > 0:
            self.wait_for_uploads(timeout=600)
        return count
    
    def get_pending_local_paths(self):
        """Return set of local_paths currently in pending CSV (for cleanup skip list)."""
        if not os.path.exists(self.pending_uploads_csv):
            return set()
        with self.csv_lock:
            try:
                with open(self.pending_uploads_csv, "r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    return {row.get("local_path", "") for row in reader if row.get("local_path")}
            except Exception:
                return set()
    
    def estimate_file_size_and_upload_time(self, upload_speed_mbps=10):
        """Estimate file size and upload time based on configuration."""
        duration_seconds = self.recording_duration * 60
        file_size_bits = self.bitrate * duration_seconds
        file_size_mb = file_size_bits / (8 * 1024 * 1024)
        
        upload_time_seconds = (file_size_mb * 8) / upload_speed_mbps
        
        self.logger.info(f"=== Upload Estimates ===")
        self.logger.info(f"Recording duration: {self.recording_duration} minutes ({duration_seconds} seconds)")
        self.logger.info(f"Bitrate: {self.bitrate/1_000_000:.1f} Mbps")
        self.logger.info(f"Expected file size: {file_size_mb:.2f} MB")
        self.logger.info(f"Estimated upload time at {upload_speed_mbps} Mbps: {upload_time_seconds:.1f} seconds ({upload_time_seconds/60:.1f} minutes)")
        
        return file_size_mb, upload_time_seconds
    
    def wait_for_uploads(self, timeout=None):
        """
        Wait for all uploads to complete.
        
        Args:
            timeout: Maximum time to wait in seconds (None = wait indefinitely)
        """
        start_time = time.time()
        while self.active_uploads > 0:
            if timeout and (time.time() - start_time) > timeout:
                self.logger.warning(f"[UPLOAD] Timeout waiting for uploads after {timeout} seconds")
                break
            time.sleep(0.5)
        self.logger.info("[UPLOAD] All uploads completed")

    def cleanup(self):
        """Shutdown upload pool and close S3 client (urllib3 pools / sockets)."""
        try:
            ex = getattr(self, "_upload_executor", None)
            if ex is not None:
                self.logger.info("[CLEANUP] Shutting down upload executor...")
                ex.shutdown(wait=True, cancel_futures=False)
                self._upload_executor = None
        except Exception as e:
            self.logger.error(f"[CLEANUP] Error shutting down upload executor: {e}", exc_info=True)
        try:
            self._close_s3_connection()
        except Exception as e:
            self.logger.error(f"[CLEANUP] Error closing S3 client: {e}", exc_info=True)

