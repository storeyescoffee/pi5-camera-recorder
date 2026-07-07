#!/usr/bin/env python3
"""
Cloud Upload Module for Raspberry Pi 5 Video Recorder
Handles all S3/GCS upload functionality with retry logic and concurrent uploads.
"""

import csv
import os
import random
import subprocess
import time
import threading
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import (
    ConnectionClosedError,
    ConnectionError as BotoConnectionError,
    ConnectTimeoutError,
    EndpointConnectionError,
    ReadTimeoutError,
)

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
        self._retry_lock = threading.Lock()  # serialize retry_pending_uploads passes (no overlapping retries)
        self._inflight_lock = threading.Lock()
        self._inflight = set()  # local_paths currently queued/uploading — dedup guard against double-queue
        self._reinit_lock = threading.Lock()  # serialize S3 close/reinit across upload worker threads
        self._bounce_lock = threading.Lock()  # serialize network rebounce + retry (one at a time)
        self._network_bounce_needed = False  # set when an upload fails on a dead link
        self._last_bounce_ts = 0.0  # monotonic-ish wall clock of last bounce (cooldown gate)
        self.s3_client = None
        self.s3_region = None
        self._transfer_config = None  # boto3 TransferConfig — controls multipart chunk size + per-file concurrency
        self._boto_session = None  # boto3.Session — one session, one pooled S3 client
        self._upload_executor = None  # bounded thread pool for uploads (never one OS thread per clip)
        self._upload_executor_workers = None
        self._upload_slot_n = None
        self._upload_busy = None  # threading.Semaphore(max_concurrent) — backpressure on queue
        self.api_client = create_api_client(config, logger)

        self._load_config()
        self._build_transfer_config()
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
            # Default lowered to 2: on weak uplinks (Maroc Telecom) many parallel 443 streams
            # get dropped mid-transfer. Fewer concurrent uploads = fewer mid-transfer failures.
            self.max_concurrent_uploads = int(self.config.get("recording", "max_concurrent_uploads", fallback="2"))
            self.delete_after_upload = self.config.getboolean("recording", "delete_after_upload", fallback=False)
            self.recording_duration = int(self.config.get("recording", "duration_minutes"))
            self.bitrate = int(self.config.get("camera", "bitrate"))
            self.pending_uploads_csv = self.config.get("recording", "pending_uploads_csv", fallback="./pending_uploads.csv")
            self.pending_retry_interval_minutes = int(self.config.get("recording", "pending_retry_interval_minutes", fallback="10"))
            # After this many genuine upload failures, stop retrying and move the clip to the
            # dead-letter store (failed_uploads_csv) instead of endlessly re-queueing it.
            # Raised to 5: 3 is too trigger-happy on a genuinely flaky link.
            self.max_pending_retries = max(1, int(self.config.get("recording", "max_pending_retries", fallback="5")))
            self.failed_uploads_csv = self.config.get("recording", "failed_uploads_csv", fallback="./failed_uploads.csv")
            # urllib3 pool used by boto3 (avoid CLOSE-WAIT / pool exhaustion when max_concurrent_uploads > default 10)
            _mc = max(1, self.max_concurrent_uploads)
            _default_pool = str(max(_mc * 4, 32))
            self.s3_pool_connections = int(self.config.get("recording", "s3_pool_connections", fallback=_default_pool))
            # App-level upload retries (distinct from botocore's internal retries on single request)
            self.upload_retry_backoff_initial = float(
                self.config.get("recording", "upload_retry_backoff_initial_sec", fallback="3")
            )
            self.upload_retry_backoff_cap = float(self.config.get("recording", "upload_retry_backoff_cap_sec", fallback="90"))

            # Multipart transfer tuning (boto3 TransferConfig). Smaller per-file concurrency is the
            # single biggest lever on a weak uplink: boto3 defaults to 10 internal threads PER file,
            # which fight each other and get dropped. chunk size controls how much a dead part costs.
            self.s3_multipart_chunk_mb = max(5, int(self.config.get("recording", "s3_multipart_chunk_mb", fallback="8")))
            self.s3_upload_concurrency = max(1, int(self.config.get("recording", "s3_upload_concurrency", fallback="2")))

            # Network rebounce: auto-bounce the NetworkManager link when uploads fail on a dead link.
            self.network_rebounce_enabled = self.config.getboolean("recording", "network_rebounce_enabled", fallback=False)
            self.network_rebounce_connection = self.config.get("recording", "network_rebounce_connection", fallback="main-pi").strip()
            self.network_rebounce_cooldown = float(self.config.get("recording", "network_rebounce_cooldown_sec", fallback="90"))
            self.network_rebounce_wait_after_up = float(self.config.get("recording", "network_rebounce_wait_after_up_sec", fallback="15"))
            self.network_rebounce_down_up_gap = float(self.config.get("recording", "network_rebounce_down_up_gap_sec", fallback="2"))
            self.network_rebounce_cmd_timeout = float(self.config.get("recording", "network_rebounce_cmd_timeout_sec", fallback="30"))
            # S3 timeouts — low connect timeout detects a dead link fast; read stays high for large uploads.
            # read lowered to 120: with 8MB chunks, 300s is too patient — a stalled part should fail fast.
            self.s3_connect_timeout = int(self.config.get("recording", "s3_connect_timeout_sec", fallback="10"))
            self.s3_read_timeout = int(self.config.get("recording", "s3_read_timeout_sec", fallback="120"))
            self.s3_max_attempts = int(self.config.get("recording", "s3_max_attempts", fallback="5"))

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

    def _build_transfer_config(self):
        """Build the boto3 TransferConfig used by every upload_file call.

        multipart_chunksize controls how much a dropped part wastes (smaller = cheaper retry).
        max_concurrency caps the internal threads boto3 spins up PER file — the default of 10
        overwhelms a weak uplink and causes the mid-transfer ConnectionClosedError seen on
        partNumber=N. Keep this low (2) so parts upload sequentially-ish and survive.
        """
        chunk = self.s3_multipart_chunk_mb * 1024 * 1024
        self._transfer_config = TransferConfig(
            multipart_threshold=chunk,
            multipart_chunksize=chunk,
            max_concurrency=self.s3_upload_concurrency,
            use_threads=True,
        )
        self.logger.info(
            f"[UPLOAD] TransferConfig: chunk={self.s3_multipart_chunk_mb}MB, "
            f"per-file concurrency={self.s3_upload_concurrency}"
        )

    def _make_botocore_config(self):
        """Shared botocore Config: urllib3 pool size, timeouts, retries (reduces CLOSE-WAIT / stalls)."""
        pool = max(self.s3_pool_connections, self.max_concurrent_uploads * 4, 32)
        retries = {"max_attempts": max(1, int(self.s3_max_attempts)), "mode": "standard"}
        kw = dict(
            signature_version="s3v4",
            max_pool_connections=pool,
            retries=retries,
            connect_timeout=int(self.s3_connect_timeout),
            read_timeout=int(self.s3_read_timeout),
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
        self._build_transfer_config()
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

    # Botocore errors that mean "the link is dead" (vs auth/4xx/5xx faults).
    _CONNECTION_ERRORS = (
        EndpointConnectionError,
        ConnectTimeoutError,
        ReadTimeoutError,
        ConnectionClosedError,
        BotoConnectionError,
    )

    def _is_connection_error(self, exc):
        """True for network-down style errors a link rebounce can fix."""
        if isinstance(exc, self._CONNECTION_ERRORS):
            return True
        # Fallback: some botocore/urllib3 wrappers don't subclass the above cleanly.
        name = type(exc).__name__.lower()
        return "timeout" in name or "connection" in name or "endpoint" in name

    def _rebounce_network(self):
        """Bounce the NetworkManager link: sudo nmcli connection down/up <conn>. Never raises."""
        conn = self.network_rebounce_connection
        timeout = self.network_rebounce_cmd_timeout

        def _run(action):
            cmd = ["sudo", "nmcli", "connection", action, conn]
            self.logger.info(f"[NET] Running: {' '.join(cmd)}")
            try:
                r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
            except Exception as e:
                self.logger.error(f"[NET] nmcli {action} {conn} failed to run: {e}")
                return False
            out = (r.stdout or "").strip()
            err = (r.stderr or "").strip()
            if out:
                self.logger.info(f"[NET] nmcli {action} stdout: {out}")
            if err:
                self.logger.warning(f"[NET] nmcli {action} stderr: {err}")
            if r.returncode != 0:
                self.logger.error(f"[NET] nmcli {action} {conn} exited {r.returncode}")
                return False
            return True

        self.logger.info(f"[NET] Rebouncing link '{conn}'...")
        down_ok = _run("down")
        time.sleep(max(0.0, self.network_rebounce_down_up_gap))
        up_ok = _run("up")
        time.sleep(max(0.0, self.network_rebounce_wait_after_up))
        if up_ok:
            self.logger.info(f"[NET] Link '{conn}' rebounced (down_ok={down_ok}, up_ok={up_ok})")
        return up_ok

    def _maybe_trigger_bounce_if_idle(self):
        """If a rebounce is wanted and no uploads are in flight, run the coordinator off-thread."""
        if not (self.network_rebounce_enabled and self._network_bounce_needed):
            return
        with self.upload_lock:
            idle = self.active_uploads == 0
        if idle:
            threading.Thread(target=self._maybe_bounce_and_retry, daemon=True).start()

    def _maybe_bounce_and_retry(self):
        """Once all uploads have drained: bounce the link (cooldown-gated), reinit S3, retry pending."""
        if not self._bounce_lock.acquire(blocking=False):
            return  # a bounce is already running
        try:
            now = time.time()
            since = now - self._last_bounce_ts
            if since < self.network_rebounce_cooldown:
                self.logger.info(
                    f"[NET] Skipping rebounce (cooldown: {since:.0f}s < {self.network_rebounce_cooldown:.0f}s); "
                    "pending uploads will be retried later"
                )
                return  # leave _network_bounce_needed set; periodic retry handles leftovers
            self._last_bounce_ts = now
            self._network_bounce_needed = False
            self._rebounce_network()
            self._init_s3_client(max_retries=2, retry_delay=min(30, float(self.upload_retry_backoff_cap)))
            if self.s3_client is None:
                self.logger.warning("[NET] S3 client still unavailable after rebounce; will retry later")
                return
            self.retry_pending_uploads()
        except Exception as e:
            self.logger.error(f"[NET] Error during rebounce/retry: {e}", exc_info=True)
        finally:
            self._bounce_lock.release()

    PENDING_CSV_HEADER = ["local_path", "s3_key", "filename", "failed_at", "video_code", "attempts"]
    FAILED_CSV_HEADER = ["local_path", "s3_key", "filename", "failed_at", "video_code", "attempts", "reason"]

    @staticmethod
    def _parse_attempts(value):
        """Best-effort int parse of the attempts column (handles '', None, floats)."""
        try:
            return int(float(value))
        except (TypeError, ValueError):
            return 0

    def _migrate_pending_csv_if_needed(self):
        """Ensure the pending CSV has the current columns (video_code, attempts); migrate in place."""
        if not os.path.exists(self.pending_uploads_csv):
            return
        with open(self.pending_uploads_csv, "r", newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            fieldnames = list(reader.fieldnames or [])
            rows = list(reader)
        if "video_code" in fieldnames and "attempts" in fieldnames:
            return
        for row in rows:
            row["video_code"] = row.get("video_code") or ""
            if not row.get("attempts"):
                row["attempts"] = "0"
        with open(self.pending_uploads_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=self.PENDING_CSV_HEADER, extrasaction="ignore")
            writer.writeheader()
            writer.writerows(rows)
        self.logger.info("[PENDING] Migrated CSV to current columns (video_code, attempts)")

    def _add_pending_upload(self, local_path, s3_key, filename, video_code=None, attempts=0):
        """Record a failed upload for later retry. Deduplicates: one row per local_path (latest wins)."""
        with self.csv_lock:
            try:
                self._migrate_pending_csv_if_needed()
                rows = []
                if os.path.exists(self.pending_uploads_csv):
                    with open(self.pending_uploads_csv, "r", newline="", encoding="utf-8") as f:
                        for row in csv.DictReader(f):
                            if row.get("local_path") != local_path:
                                rows.append(row)
                rows.append({
                    "local_path": local_path,
                    "s3_key": s3_key,
                    "filename": filename,
                    "failed_at": datetime.now().isoformat(),
                    "video_code": video_code or "",
                    "attempts": int(attempts),
                })
                with open(self.pending_uploads_csv, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=self.PENDING_CSV_HEADER, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(rows)
                self.logger.info(
                    f"[PENDING] Queued for retry: {filename} (attempts={int(attempts)}"
                    + (f", video_code={video_code}" if video_code else "") + ")"
                )
            except Exception as e:
                self.logger.error(f"[PENDING] Failed to write to CSV: {e}", exc_info=True)

    def _add_failed_upload(self, local_path, s3_key, filename, video_code, attempts, reason):
        """Persist a permanently-failed upload to the dead-letter store (kept on the SD card until
        --reconcile). Deduplicates: one row per local_path (latest wins)."""
        with self.csv_lock:
            try:
                rows = []
                if os.path.exists(self.failed_uploads_csv):
                    with open(self.failed_uploads_csv, "r", newline="", encoding="utf-8") as f:
                        for row in csv.DictReader(f):
                            if row.get("local_path") != local_path:
                                rows.append(row)
                rows.append({
                    "local_path": local_path,
                    "s3_key": s3_key,
                    "filename": filename,
                    "failed_at": datetime.now().isoformat(),
                    "video_code": video_code or "",
                    "attempts": int(attempts),
                    "reason": reason,
                })
                with open(self.failed_uploads_csv, "w", newline="", encoding="utf-8") as f:
                    writer = csv.DictWriter(f, fieldnames=self.FAILED_CSV_HEADER, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(rows)
                self.logger.error(
                    f"[FAILED] Gave up after {int(attempts)} failure(s); saved to dead-letter store: {filename}"
                )
            except Exception as e:
                self.logger.error(f"[FAILED] Could not write dead-letter CSV: {e}", exc_info=True)
    
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

    def _remove_failed_upload(self, local_path):
        """Remove an entry from the dead-letter store (after reconcile re-queues or drops it)."""
        with self.csv_lock:
            try:
                if not os.path.exists(self.failed_uploads_csv):
                    return
                rows = []
                with open(self.failed_uploads_csv, "r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    fieldnames = reader.fieldnames or []
                    for row in reader:
                        if row.get("local_path") != local_path:
                            rows.append(row)
                with open(self.failed_uploads_csv, "w", newline="", encoding="utf-8") as f:
                    if rows and fieldnames:
                        writer = csv.DictWriter(f, fieldnames=fieldnames)
                        writer.writeheader()
                        writer.writerows(rows)
                self.logger.debug(f"[FAILED] Removed from dead-letter store: {local_path}")
            except Exception as e:
                self.logger.error(f"[FAILED] Failed to update dead-letter CSV: {e}", exc_info=True)

    def get_failed_local_paths(self):
        """Return set of local_paths currently in the dead-letter store (saved-on-SD clips)."""
        if not os.path.exists(self.failed_uploads_csv):
            return set()
        with self.csv_lock:
            try:
                with open(self.failed_uploads_csv, "r", newline="", encoding="utf-8") as f:
                    reader = csv.DictReader(f)
                    return {row.get("local_path", "") for row in reader if row.get("local_path")}
            except Exception:
                return set()

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
        self, local_path, s3_key, filename, max_retries=3, is_fallback=False,
        existing_video_code=None, start_time=None, pending_attempts=0,
    ):
        """Upload file to S3 with retry logic, metadata, and detailed statistics.

        is_fallback: True when retrying from pending CSV.
        existing_video_code: From CSV when POST succeeded but upload failed (scenario 1).
        start_time: Recording start time (datetime); if None, derived from file mtime - duration for retries.
        pending_attempts: How many times this clip has already failed (drives the 3-strikes dead-letter).
        """
        if self.s3_client is None:
            self.logger.error(f"[UPLOAD] S3 client unavailable, cannot upload {s3_key}")
            # Link/client is down — not a genuine upload failure, so don't burn an attempt.
            self._add_pending_upload(local_path, s3_key, filename, video_code=existing_video_code, attempts=pending_attempts)
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
                    # Scenario 1: POST succeeded, upload failed. Reuse stored video_code (no duplicate record).
                    if self.api_client:
                        self.api_client.put_main_video(video_code, 0, 0.0, pending_attempts, "UPLOADING_FALLBACK")
                else:
                    # Scenario 2: POST previously failed. Resend it (reuse code on the next retry).
                    if self.api_client:
                        video_url = self._build_video_url(s3_key)
                        video_code = self.api_client.post_main_video(video_url, file_size, duration_seconds, start_time, hour=start_time.hour)
                        if video_code:
                            self.api_client.put_main_video(video_code, 0, 0.0, pending_attempts, "UPLOADING_FALLBACK")
                        else:
                            self.logger.warning("[API] POST main-video failed on retry")
            elif self.api_client:
                # Normal flow: POST to register, then mark UPLOADING so the record has an in-progress state.
                video_url = self._build_video_url(s3_key)
                video_code = self.api_client.post_main_video(video_url, file_size, duration_seconds, start_time, hour=start_time.hour)
                if video_code:
                    self.api_client.put_main_video(video_code, 0, 0.0, 0, "UPLOADING")
                else:
                    self.logger.warning("[API] POST main-video failed, continuing upload without backend notification")

            uploaded = False
            dead_link_deferral = False
            for attempt in range(1, max_retries + 1):
                try:
                    # Guard: a prior failed reinit can leave s3_client=None — don't call
                    # .upload_file on None (was raising AttributeError on the last attempt).
                    if self.s3_client is None:
                        raise RuntimeError("S3 client unavailable (None)")
                    size_mb = file_size / (1024 * 1024)
                    self.logger.info(f"[UPLOAD] Starting upload: {filename} -> {s3_key}")
                    self.logger.info(f"[UPLOAD] File size: {size_mb:.2f} MB - Attempt {attempt}/{max_retries}")
                    t0 = time.time()

                    # Upload with metadata. TransferConfig caps per-file internal concurrency
                    # (default 10 -> 2) and sets multipart chunk size — this is the main fix for
                    # the mid-transfer ConnectionClosedError on partNumber=N over a weak uplink.
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
                        },
                        Config=self._transfer_config,
                    )
                    
                    dt = time.time() - t0
                    upload_speed_mbps = (size_mb * 8) / dt if dt > 0 else 0
                    self.logger.info(f"[UPLOAD] Upload completed: {s3_key}")
                    self.logger.info(f"[UPLOAD] Upload time: {dt:.1f} seconds ({dt/60:.1f} minutes)")
                    self.logger.info(f"[UPLOAD] Upload speed: {upload_speed_mbps:.2f} Mbps")
                    self.logger.info(f"[UPLOAD] Average upload rate: {size_mb/dt:.2f} MB/s")
                    
                    # S3 upload succeeded. If the backend was never told about this clip
                    # (POST failed earlier), register it now — the object exists, so losing
                    # the record would be silent data loss.
                    if self.api_client and not video_code:
                        video_url = self._build_video_url(s3_key)
                        video_code = self.api_client.post_main_video(
                            video_url, file_size, duration_seconds, start_time, hour=start_time.hour
                        )
                        if not video_code:
                            self.logger.warning(
                                "[API] Upload OK but backend registration failed; keeping in pending to register later"
                            )

                    if self.api_client and video_code:
                        status = "COMPLETED_FALLBACK" if is_fallback else "COMPLETED"
                        self.api_client.put_main_video(video_code, int(dt), upload_speed_mbps, attempt, status)
                        # Success and recorded: drop from pending and optionally delete the local file.
                        self._remove_pending_upload(local_path)
                        if self.delete_after_upload:
                            try:
                                os.remove(local_path)
                                self.logger.info(f"[UPLOAD] Deleted local file: {local_path}")
                            except Exception as e:
                                self.logger.error(f"[UPLOAD] Failed to delete local file {local_path}: {e}", exc_info=True)
                    elif self.api_client:
                        # Uploaded but could not register — keep the file and a pending row so a
                        # later retry can register it (don't count this as a failure).
                        self._add_pending_upload(local_path, s3_key, filename, video_code=None, attempts=pending_attempts)
                    else:
                        # No API client configured at all — just clean up pending/local as before.
                        self._remove_pending_upload(local_path)
                        if self.delete_after_upload:
                            try:
                                os.remove(local_path)
                                self.logger.info(f"[UPLOAD] Deleted local file: {local_path}")
                            except Exception as e:
                                self.logger.error(f"[UPLOAD] Failed to delete local file {local_path}: {e}", exc_info=True)

                    uploaded = True
                    break  # Success, exit retry loop
                except Exception as e:
                    self.logger.error(f"[UPLOAD] Failed for {s3_key} (attempt {attempt}/{max_retries}): {e}", exc_info=True)
                    if self.network_rebounce_enabled and self._is_connection_error(e):
                        # Dead link: fail fast (skip the long inline reinit) so every upload
                        # thread drains quickly. The coordinator then bounces the link once
                        # (while nothing is in flight) and retries the pending uploads. This is a
                        # network outage, not a genuine upload failure — don't burn a retry.
                        self.logger.warning(f"[NET] Connection error; deferring '{filename}' to link rebounce")
                        self._close_s3_connection()
                        self._network_bounce_needed = True
                        dead_link_deferral = True
                        break
                    if attempt < max_retries:
                        self._close_s3_connection()
                        self._reinit_s3_client()
                        wait_time = self._retry_delays_seconds(attempt)
                        self.logger.info(f"[UPLOAD] Retrying upload in {wait_time:.1f} seconds...")
                        time.sleep(wait_time)
                        continue

            if not uploaded:
                if dead_link_deferral:
                    # Network down — keep in pending at the same attempt count for the rebounce
                    # coordinator to retry. No FAILED status (the link can't carry it anyway).
                    self.logger.warning(f"[UPLOAD] Deferred to link rebounce, kept in pending: {filename}")
                    self._add_pending_upload(local_path, s3_key, filename, video_code=video_code, attempts=pending_attempts)
                else:
                    new_attempts = pending_attempts + 1
                    dt = time.time() - t0
                    upload_speed_mbps = (size_mb * 8) / dt if dt > 0 else 0
                    if new_attempts >= self.max_pending_retries:
                        # Three strikes: stop retrying, move to the dead-letter store, mark FAILED.
                        self.logger.error(
                            f"[UPLOAD] {filename} failed {new_attempts} time(s) (>= {self.max_pending_retries}); "
                            "giving up and moving to dead-letter store"
                        )
                        self._remove_pending_upload(local_path)
                        self._add_failed_upload(local_path, s3_key, filename, video_code, new_attempts, "max_retries_exceeded")
                        if self.api_client and video_code:
                            self.api_client.put_main_video(video_code, int(dt), upload_speed_mbps, new_attempts, "FAILED")
                    else:
                        self.logger.error(
                            f"[UPLOAD] Upload failed (attempt {new_attempts}/{self.max_pending_retries}), queued for retry: {local_path}"
                        )
                        self._add_pending_upload(local_path, s3_key, filename, video_code=video_code, attempts=new_attempts)

        except Exception as e:
            self.logger.error(f"[UPLOAD] Unexpected error during upload pipeline for {filename}: {e}", exc_info=True)
            raise

    def upload_file(self, local_path, s3_key, filename, is_fallback=False, video_code=None, start_time=None, pending_attempts=0):
        """Queue one upload: bounded ThreadPoolExecutor plus semaphore (capacity = max concurrent)."""
        self._ensure_upload_limits()
        self._ensure_upload_executor_locked()

        # Dedup guard: never queue the same file while a prior/concurrent attempt is in flight
        # (startup, periodic, and rebounce retries can otherwise queue the same clip twice).
        with self._inflight_lock:
            if local_path in self._inflight:
                self.logger.info(f"[UPLOAD] Already in flight, skipping duplicate queue: {filename}")
                return
            self._inflight.add(local_path)

        submitted = False
        try:
            if self.s3_client is None and self.network_rebounce_enabled:
                # Link is known-dead: don't block the caller (recording loop) on a long reinit.
                # Park the file in pending and let the coordinator bounce + retry once idle.
                self.logger.warning(f"[NET] No S3 client; parking '{filename}' for retry and scheduling rebounce")
                self._add_pending_upload(local_path, s3_key, filename, video_code=video_code or None, attempts=pending_attempts)
                self._network_bounce_needed = True
                self._maybe_trigger_bounce_if_idle()
                return
            if self.s3_client is None:
                self.logger.warning(f"[UPLOAD] S3 client unavailable, skipping upload for {filename}")
                self._reinit_s3_client()
            if self.s3_client is None:
                # Still no client — park it so the clip isn't silently dropped.
                self.logger.error(f"[UPLOAD] No S3 client; parking '{filename}' for later retry")
                self._add_pending_upload(local_path, s3_key, filename, video_code=video_code or None, attempts=pending_attempts)
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
                        pending_attempts=pending_attempts,
                    )
                finally:
                    with self.upload_lock:
                        self.active_uploads -= 1
                        drained = self.active_uploads == 0
                    self._upload_busy.release()
                    with self._inflight_lock:
                        self._inflight.discard(local_path)
                    self.logger.info(f"[UPLOAD] Upload task finished for {filename}")
                    # When the last upload drains and a dead-link failure was flagged,
                    # bounce the link once (off-thread) and retry the pending uploads.
                    if drained and self.network_rebounce_enabled and self._network_bounce_needed:
                        threading.Thread(target=self._maybe_bounce_and_retry, daemon=True).start()

            try:
                self._upload_executor.submit(run)
                submitted = True
            except Exception as e:
                with self.upload_lock:
                    self.active_uploads -= 1
                self._upload_busy.release()
                self.logger.error(f"[UPLOAD] Failed to queue upload for {filename}: {e}", exc_info=True)
                raise
            with self.upload_lock:
                n = self.active_uploads
            self.logger.info(f"[UPLOAD] Upload queued for {filename} (tracked active: {n})")
        finally:
            # If we never handed the job to a worker, the run() finally never runs — release the
            # in-flight slot here so the file can be queued again later.
            if not submitted:
                with self._inflight_lock:
                    self._inflight.discard(local_path)

    def retry_pending_uploads(self):
        """Retry all uploads listed in pending_uploads.csv. Removes entries for missing files.
        Clips that already failed max_pending_retries times are moved to the dead-letter store.
        Uses UPLOADING_FALLBACK then COMPLETED_FALLBACK when retrying."""
        # Serialize retry passes — startup, the periodic loop, and the rebounce coordinator can
        # all call this; overlapping passes would double-queue the same rows.
        if not self._retry_lock.acquire(blocking=False):
            self.logger.info("[PENDING] Retry already in progress, skipping this pass")
            return 0
        try:
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
                attempts = self._parse_attempts(row.get("attempts"))
                if not os.path.exists(local_path):
                    self.logger.warning(f"[PENDING] Skipping missing file, removing from list: {local_path}")
                    self._remove_pending_upload(local_path)
                    continue
                if attempts >= self.max_pending_retries:
                    # Safety net for rows already at the limit (e.g. legacy CSVs): dead-letter them.
                    self.logger.error(
                        f"[PENDING] {filename} already failed {attempts} time(s); moving to dead-letter, will not retry"
                    )
                    self._add_failed_upload(local_path, s3_key, filename, row.get("video_code") or "", attempts, "max_retries_exceeded")
                    self._remove_pending_upload(local_path)
                    continue
                vc = (row.get("video_code") or "").strip() or None
                self.upload_file(local_path, s3_key, filename, is_fallback=True, video_code=vc, pending_attempts=attempts)
                count += 1
            if count > 0:
                self.wait_for_uploads(timeout=600)
            return count
        finally:
            self._retry_lock.release()

    def reconcile(self):
        """One-shot manual pass: upload everything still saved on the SD card.

        Covers (a) normal pending retries and (b) the dead-letter store — clips that
        exhausted max_pending_retries and were parked on disk, untouched by the automatic
        retries, waiting for an explicit --reconcile. Returns the number of clips re-queued.
        """
        self.logger.info("[RECONCILE] Starting manual reconcile of on-disk clips...")
        total = self.retry_pending_uploads()
        total += self.reconcile_failed_uploads()
        self.logger.info(f"[RECONCILE] Done. Re-queued {total} clip(s).")
        return total

    def reconcile_failed_uploads(self):
        """Re-drive the dead-letter store: re-upload each saved failed clip that still exists.

        Each clip's attempt counter is reset so it gets a fresh retry budget; it is removed
        from the dead-letter store before re-queueing (and re-added only if it fails again).
        """
        if not os.path.exists(self.failed_uploads_csv):
            self.logger.info("[RECONCILE] No dead-letter store found")
            return 0
        with self.csv_lock:
            try:
                with open(self.failed_uploads_csv, "r", newline="", encoding="utf-8") as f:
                    rows = list(csv.DictReader(f))
            except Exception as e:
                self.logger.error(f"[RECONCILE] Failed to read dead-letter CSV: {e}", exc_info=True)
                return 0
        if not rows:
            self.logger.info("[RECONCILE] Dead-letter store is empty")
            return 0
        self.logger.info(f"[RECONCILE] Re-driving {len(rows)} saved (dead-letter) clip(s)...")
        count = 0
        for row in rows:
            local_path = row.get("local_path", "")
            s3_key = row.get("s3_key", "")
            filename = row.get("filename", os.path.basename(local_path))
            if not local_path or not s3_key:
                self._remove_failed_upload(local_path)
                continue
            if os.path.exists(local_path):
                vc = (row.get("video_code") or "").strip() or None
                # Clear from the dead-letter store first; a fresh failure re-adds it only after
                # another full max_pending_retries cycle.
                self._remove_failed_upload(local_path)
                self.upload_file(local_path, s3_key, filename, is_fallback=True, video_code=vc, pending_attempts=0)
                count += 1
            else:
                self.logger.warning(f"[RECONCILE] Saved clip missing, dropping from dead-letter: {local_path}")
                self._remove_failed_upload(local_path)
        if count > 0:
            self.wait_for_uploads(timeout=1800)
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
        while True:
            # Read active_uploads under the same lock that increments/decrements it,
            # so we never spin on (or exit early from) a stale value.
            with self.upload_lock:
                if self.active_uploads == 0:
                    break
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