#!/usr/bin/env python3
"""
Cloud Upload Module for Raspberry Pi 5 Video Recorder
Handles all S3/GCS upload functionality with retry logic and concurrent uploads.
"""

import csv
import os
import time
import threading
import logging
from datetime import datetime
import boto3
from botocore.client import Config


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
        
        self.upload_threads = []
        self.active_uploads = 0
        self.upload_lock = threading.Lock()
        self.csv_lock = threading.Lock()
        self.s3_client = None
        
        self._load_config()
        self._init_s3_client()
    
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
            self.bucket_name = self.config.get("gcs", "bucket_name")
            self.region = self.config.get("gcs", "region", fallback="us-east-1")
            
            # Recording settings needed for upload
            self.max_concurrent_uploads = int(self.config.get("recording", "max_concurrent_uploads", fallback="3"))
            self.delete_after_upload = self.config.getboolean("recording", "delete_after_upload", fallback=False)
            self.recording_duration = int(self.config.get("recording", "duration_minutes"))
            self.bitrate = int(self.config.get("camera", "bitrate"))
            self.pending_uploads_csv = self.config.get("recording", "pending_uploads_csv", fallback="./pending_uploads.csv")
            self.pending_retry_interval_minutes = int(self.config.get("recording", "pending_retry_interval_minutes", fallback="10"))
            
            self.logger.info("Cloud upload configuration loaded successfully.")
            self.logger.info("Using AWS credentials from system default credential chain")
        except Exception as e:
            self.logger.error(f"Error loading cloud upload configuration: {e}", exc_info=True)
            raise
    
    def _init_s3_client(self, max_retries=5, retry_delay=10):
        """Create S3 client with automatic region detection and retry logic.
        
        Uses AWS default credential chain (environment variables, IAM roles, ~/.aws/credentials).
        """
        for attempt in range(1, max_retries + 1):
            try:
                # Use default credential chain - no explicit credentials needed
                base_client = boto3.client(
                    "s3",
                    region_name="us-east-1",
                    config=Config(signature_version="s3v4")
                )
                
                region_resp = base_client.get_bucket_location(Bucket=self.bucket_name)
                actual_region = region_resp.get("LocationConstraint") or "us-east-1"
                self.logger.info(f"Detected S3 bucket region: {actual_region}")
                
                endpoint = f"https://s3.{actual_region}.amazonaws.com"
                self.logger.info(f"Using regional S3 endpoint: {endpoint}")
                
                # Use default credential chain - no explicit credentials needed
                self.s3_client = boto3.client(
                    "s3",
                    endpoint_url=endpoint,
                    region_name=actual_region,
                    config=Config(signature_version="s3v4")
                )
                
                self.logger.info("✅ S3 client initialized successfully.")
                return True
            except Exception as e:
                self.logger.error(f"Error initializing S3 client (attempt {attempt}/{max_retries}): {e}")
                if attempt < max_retries:
                    self.logger.info(f"Retrying S3 initialization in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error("Failed to initialize S3 client after all retries. Uploads will be disabled.")
                    self.logger.error("Make sure AWS credentials are configured (environment variables, IAM role, or ~/.aws/credentials)")
                    self.s3_client = None
                    return False
    
    def _reinit_s3_client(self):
        """Reinitialize S3 client if it failed previously."""
        if self.s3_client is None:
            self.logger.info("Attempting to reinitialize S3 client...")
            self._init_s3_client(max_retries=3, retry_delay=5)
    
    def _add_pending_upload(self, local_path, s3_key, filename):
        """Append failed upload info to CSV for later retry."""
        with self.csv_lock:
            try:
                file_exists = os.path.exists(self.pending_uploads_csv)
                with open(self.pending_uploads_csv, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    if not file_exists:
                        writer.writerow(["local_path", "s3_key", "filename", "failed_at"])
                    writer.writerow([local_path, s3_key, filename, datetime.now().isoformat()])
                self.logger.info(f"[PENDING] Added to retry list: {filename}")
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
    
    def _cleanup_finished_threads(self):
        """Clean up finished upload threads."""
        with self.upload_lock:
            finished_threads = [t for t in self.upload_threads if not t.is_alive()]
            self.upload_threads = [t for t in self.upload_threads if t.is_alive()]
            
            if finished_threads:
                self.logger.debug(f"[CLEANUP] Removed {len(finished_threads)} finished upload threads")
    
    def _upload_worker(self, local_path, s3_key, filename, max_retries=3):
        """Upload file to S3 with retry logic, metadata, and detailed statistics."""
        if self.s3_client is None:
            self.logger.error(f"[UPLOAD] S3 client unavailable, cannot upload {s3_key}")
            self._add_pending_upload(local_path, s3_key, filename)
            with self.upload_lock:
                self.active_uploads -= 1
            return
        
        if not os.path.exists(local_path):
            self.logger.error(f"[UPLOAD] File not found: {local_path}")
            self._remove_pending_upload(local_path)  # Remove stale entry if any
            with self.upload_lock:
                self.active_uploads -= 1
            return
        
        try:
            for attempt in range(1, max_retries + 1):
                try:
                    file_size = os.path.getsize(local_path)
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
                        # Try to reinitialize S3 client before retry
                        if self.s3_client is None:
                            self._reinit_s3_client()
                        wait_time = 5 * attempt  # Exponential backoff
                        self.logger.info(f"[UPLOAD] Retrying in {wait_time} seconds...")
                        time.sleep(wait_time)
                    else:
                        self.logger.error(f"[UPLOAD] Failed after {max_retries} attempts. File: {local_path}")
                        self._add_pending_upload(local_path, s3_key, filename)
        finally:
            with self.upload_lock:
                self.active_uploads -= 1
                self.logger.info(f"[UPLOAD] Upload thread finished for {filename}")
    
    def upload_file(self, local_path, s3_key, filename):
        """
        Queue a file for upload.
        
        Args:
            local_path: Path to local file
            s3_key: S3 key/path for the file
            filename: Filename for logging
        """
        if self.s3_client is None:
            self.logger.warning(f"[UPLOAD] S3 client unavailable, skipping upload for {filename}")
            self._reinit_s3_client()
            return
        
        # Wait if we've reached max concurrent uploads
        while self.active_uploads >= self.max_concurrent_uploads:
            self.logger.info(f"[UPLOAD] Max concurrent uploads ({self.max_concurrent_uploads}) reached, waiting...")
            time.sleep(2)
            self._cleanup_finished_threads()
        
        # Create and start upload thread
        upload_thread = threading.Thread(
            target=self._upload_worker,
            args=(local_path, s3_key, filename),
            daemon=True,
            name=f"Upload-{filename}"
        )
        
        # Add to thread list and increment counter atomically
        with self.upload_lock:
            self.upload_threads.append(upload_thread)
            self.active_uploads += 1
        
        upload_thread.start()
        self.logger.info(f"[UPLOAD] Upload thread started for {filename} (Active uploads: {self.active_uploads})")
    
    def retry_pending_uploads(self):
        """Retry all uploads listed in pending_uploads.csv. Removes entries for missing files."""
        if not os.path.exists(self.pending_uploads_csv):
            self.logger.info("[PENDING] No pending uploads file found")
            return 0
        with self.csv_lock:
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
        remaining = []
        for row in rows:
            local_path = row.get("local_path", "")
            s3_key = row.get("s3_key", "")
            filename = row.get("filename", os.path.basename(local_path))
            if not local_path or not s3_key:
                continue
            if os.path.exists(local_path):
                self.upload_file(local_path, s3_key, filename)
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
            time.sleep(1)
            self._cleanup_finished_threads()
        self.logger.info("[UPLOAD] All uploads completed")
    
    def cleanup(self):
        """Cleanup resources and wait for uploads to finish."""
        try:
            if self.upload_threads:
                self.logger.info(f"[CLEANUP] Waiting for {len(self.upload_threads)} upload threads to complete...")
                for thread in self.upload_threads:
                    if thread.is_alive():
                        thread.join(timeout=30)  # Wait max 30 seconds per thread
        except Exception as e:
            self.logger.error(f"[CLEANUP] Error during upload cleanup: {e}", exc_info=True)

