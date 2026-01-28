#!/usr/bin/env python3
"""
Raspberry Pi 5 Video Recorder with Automatic AWS S3 Region Detection
and Flexible Filename Placeholders
"""

import os, time, threading, configparser, logging
from datetime import datetime
from pathlib import Path
import cv2
import numpy as np
from picamera2 import Picamera2, MappedArray
from picamera2.encoders import H264Encoder
from picamera2.outputs import FfmpegOutput
import boto3
from botocore.client import Config
from logging.handlers import RotatingFileHandler
import libcamera

class VideoRecorder:
    def __init__(self, config_file="config.conf"):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Setup logging with file handler
        self._setup_logging()
        
        self.upload_threads = []
        self.active_uploads = 0
        self.upload_lock = threading.Lock()
        self.camera = None
        self.s3_client = None

        self._load_config()
        self._init_s3_client()
        Path(self.local_storage_path).mkdir(parents=True, exist_ok=True)
        
        # Clean up old recordings on startup
        self._cleanup_old_recordings()
        
    def _setup_logging(self):
        """Setup logging with both console and date-based file logging."""
        # Create logs directory
        logs_dir = Path("logs")
        logs_dir.mkdir(parents=True, exist_ok=True)
        
        # Date-based log filename
        log_filename = logs_dir / f"{datetime.now().strftime('%Y-%m-%d')}.log"
        
        # Configure root logger
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        
        # Remove existing handlers to avoid duplicates
        self.logger.handlers.clear()
        
        # File handler with rotation (10MB max, keep 5 backups)
        file_handler = RotatingFileHandler(
            log_filename,
            maxBytes=10*1024*1024,  # 10MB
            backupCount=5,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        
        # Console handler
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s"
        )
        console_handler.setFormatter(console_formatter)
        
        # Add handlers
        self.logger.addHandler(file_handler)
        self.logger.addHandler(console_handler)
        
        self.logger.info(f"Logging initialized. Log file: {log_filename}")

    def _cleanup_old_recordings(self):
        """Delete all existing video files in the recordings directory on startup."""
        try:
            recordings_path = Path(self.local_storage_path)
            if not recordings_path.exists():
                return
            
            deleted_count = 0
            total_size = 0
            
            # Find all video files (common video extensions)
            video_extensions = {'.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv', '.m4v'}
            
            for file_path in recordings_path.iterdir():
                if file_path.is_file() and file_path.suffix.lower() in video_extensions:
                    try:
                        file_size = file_path.stat().st_size
                        file_path.unlink()
                        deleted_count += 1
                        total_size += file_size
                        self.logger.info(f"[CLEANUP] Deleted old recording: {file_path.name}")
                    except Exception as e:
                        self.logger.warning(f"[CLEANUP] Failed to delete {file_path.name}: {e}")
            
            if deleted_count > 0:
                size_mb = total_size / (1024 * 1024)
                self.logger.info(f"[CLEANUP] Cleaned up {deleted_count} old recording(s), freed {size_mb:.2f} MB")
            else:
                self.logger.info("[CLEANUP] No old recordings found to clean up")
                
        except Exception as e:
            self.logger.error(f"[CLEANUP] Error cleaning up old recordings: {e}", exc_info=True)

    # ------------------------------------------------------------------ #
    # CONFIGURATION
    # ------------------------------------------------------------------ #
    def _load_config(self):
        """Load configuration from config file."""
        try:
            self.analog_gain = float(self.config.get("camera", "analog_gain"))
            self.shutter_speed = int(self.config.get("camera", "shutter_speed"))
            self.resolution_width = int(self.config.get("camera", "resolution_width"))
            self.resolution_height = int(self.config.get("camera", "resolution_height"))
            self.fps = int(self.config.get("camera", "fps"))
            self.bitrate = int(self.config.get("camera", "bitrate"))
            self.force_color_format = self.config.get("camera", "force_color_format", fallback=None)

            self.recording_duration = int(self.config.get("recording", "duration_minutes"))
            self.video_naming_pattern = self.config.get("recording", "video_naming_pattern")
            self.local_storage_path = self.config.get("recording", "local_storage_path")
            self.max_concurrent_uploads = int(self.config.get("recording", "max_concurrent_uploads", fallback="3"))
            # Parse delete_after_upload as boolean
            self.delete_after_upload = self.config.getboolean("recording", "delete_after_upload", fallback=False)

            self.endpoint_url = self.config.get("gcs", "endpoint_url")
            self.access_key = self.config.get("gcs", "access_key")
            self.secret_key = self.config.get("gcs", "secret_key")
            self.bucket_name = self.config.get("gcs", "bucket_name")
            self.region = self.config.get("gcs", "region", fallback="us-east-1")

            self.logger.info("Configuration loaded successfully.")
        except Exception as e:
            self.logger.error(f"Error loading configuration: {e}", exc_info=True)
            raise  # Config errors are fatal, must raise

    # ------------------------------------------------------------------ #
    # S3 INITIALIZATION (Auto Region)
    # ------------------------------------------------------------------ #
    def _init_s3_client(self, max_retries=5, retry_delay=10):
        """Create S3 client with automatic region detection and retry logic."""
        for attempt in range(1, max_retries + 1):
            try:
                base_client = boto3.client(
                    "s3",
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key,
                    region_name="us-east-1",
                    config=Config(signature_version="s3v4")
                )

                region_resp = base_client.get_bucket_location(Bucket=self.bucket_name)
                actual_region = region_resp.get("LocationConstraint") or "us-east-1"
                self.logger.info(f"Detected S3 bucket region: {actual_region}")

                endpoint = f"https://s3.{actual_region}.amazonaws.com"
                self.logger.info(f"Using regional S3 endpoint: {endpoint}")

                self.s3_client = boto3.client(
                    "s3",
                    endpoint_url=endpoint,
                    aws_access_key_id=self.access_key,
                    aws_secret_access_key=self.secret_key,
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
                    self.s3_client = None
                    return False
    
    def _reinit_s3_client(self):
        """Reinitialize S3 client if it failed previously."""
        if self.s3_client is None:
            self.logger.info("Attempting to reinitialize S3 client...")
            self._init_s3_client(max_retries=3, retry_delay=5)

    # ------------------------------------------------------------------ #
    # CAMERA
    # ------------------------------------------------------------------ #
    def _check_camera_formats(self):
        """Check and display supported camera formats for debugging."""
        try:
            # Get camera info to see supported formats
            camera_info = self.camera.camera_config
            self.logger.info("=== Camera Format Information ===")
            self.logger.info(f"Camera: {camera_info.get('model', 'Unknown')}")
            
            # Try to get available configurations
            try:
                # Test BGR888 format
                bgr_config = self.camera.create_preview_configuration(
                    main={"size": (640, 480), "format": "BGR888"}
                )
                self.logger.info("✓ BGR888 format is supported")
            except Exception as e:
                self.logger.info(f"✗ BGR888 format not supported: {e}")
            
            try:
                # Test RGB888 format
                rgb_config = self.camera.create_preview_configuration(
                    main={"size": (640, 480), "format": "RGB888"}
                )
                self.logger.info("✓ RGB888 format is supported")
            except Exception as e:
                self.logger.info(f"✗ RGB888 format not supported: {e}")
                
            self.logger.info("================================")
            
        except Exception as e:
            self.logger.warning(f"Could not check camera formats: {e}")

    def rotate_180_callback(self, request):
        """
        Runs on every captured frame BEFORE encoding.
        """
        with MappedArray(request, "main") as m:
                # 180° rotation = flip both axes
                m.array[:] = cv2.flip(m.array, -1)
    
    def _setup_camera(self, max_retries=5, retry_delay=5):
        """Configure PiCamera2 with retry logic and intelligent format selection."""
        for attempt in range(1, max_retries + 1):
            try:
                # Clean up previous camera instance if exists
                if self.camera is not None:
                    try:
                        self.camera.stop()
                        self.camera.close()
                    except:
                        pass
                    self.camera = None
                
                self.camera = Picamera2()
                frame_duration_us = int(1_000_000 / self.fps)

                # Check if color format is forced in config
                if self.force_color_format:
                    self.logger.info(f"Forcing color format to: {self.force_color_format}")
                    try:
                        video_config = self.camera.create_video_configuration(
                            main={"size": (self.resolution_width, self.resolution_height), "format": self.force_color_format},
                            controls={
                                "ExposureTime": self.shutter_speed,
                                "AnalogueGain": self.analog_gain,
                                "FrameDurationLimits": (frame_duration_us, frame_duration_us),
                            },
                            transform=libcamera.Transform(rotate=180)
                        )
                        self.camera.configure(video_config)
                        self.use_bgr_format = (self.force_color_format == "BGR888")
                        self.logger.info(f"Camera configured with forced format: {self.force_color_format}")
                    except Exception as forced_error:
                        self.logger.error(f"Failed to configure camera with forced format {self.force_color_format}: {forced_error}")
                        raise
                else:
                    # Try BGR888 format first for optimal performance
                    try:
                        video_config = self.camera.create_video_configuration(
                            main={"size": (self.resolution_width, self.resolution_height), "format": "BGR888"},
                            controls={
                                "ExposureTime": self.shutter_speed,
                                "AnalogueGain": self.analog_gain,
                                "FrameDurationLimits": (frame_duration_us, frame_duration_us),
                                "AeEnable": True,
                                "AeFlickerPeriod": 10000
                            },
                            transform=picamera2.Transform(rotate=180)
                        )
                        self.camera.configure(video_config)
                        self.use_bgr_format = True
                        self.logger.info("Using BGR888 format - no color conversion needed")
                    except Exception as bgr_error:
                        # Fallback to RGB888 if BGR888 is not supported
                        self.logger.warning(f"BGR888 format not supported, falling back to RGB888: {bgr_error}")
                        video_config = self.camera.create_video_configuration(
                            main={"size": (self.resolution_width, self.resolution_height), "format": "RGB888"},
                            controls={
                                "ExposureTime": self.shutter_speed,
                                "AnalogueGain": self.analog_gain,
                                "FrameDurationLimits": (frame_duration_us, frame_duration_us),
                            },
                        )
                        self.camera.configure(video_config)
                        self.use_bgr_format = False
                        self.logger.info("Using RGB888 format - color conversion will be applied")
                
                # attach rotation callback
                self.camera.post_callback = self.rotate_180_callback
                self.camera.start()
                
                # Check and display supported formats for debugging
                self._check_camera_formats()
                
                self.logger.info(f"Camera started at {self.resolution_width}x{self.resolution_height}@{self.fps}fps")
                self.logger.info(f"Frame duration: {frame_duration_us}μs (target: {1_000_000/self.fps:.1f}μs)")
                self.logger.info(f"Analog gain: {self.analog_gain}, Shutter speed: {self.shutter_speed}μs")
                self.logger.info(f"Bitrate: {self.bitrate/1_000_000:.1f}Mbps")
                self.logger.info(f"Color format: {'BGR888 (no conversion)' if getattr(self, 'use_bgr_format', False) else 'RGB888 (with conversion)'}")
                time.sleep(2)
                return True
            except Exception as e:
                self.logger.error(f"Camera setup failed (attempt {attempt}/{max_retries}): {e}", exc_info=True)
                if attempt < max_retries:
                    self.logger.info(f"Retrying camera setup in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    self.logger.error("Failed to setup camera after all retries.")
                    self.camera = None
                    return False
    
    def _reinit_camera(self):
        """Reinitialize camera if it failed previously."""
        if self.camera is None:
            self.logger.info("Attempting to reinitialize camera...")
            return self._setup_camera(max_retries=3, retry_delay=5)
        return True

    # ------------------------------------------------------------------ #
    # FILENAME GENERATION
    # ------------------------------------------------------------------ #
    def _generate_filename(self, timestamp: datetime):
        """Generate filename supporting all placeholders with error handling."""
        try:
            return self.video_naming_pattern.format(
                year=timestamp.strftime('%Y'),
                month=timestamp.strftime('%m'),
                day=timestamp.strftime('%d'),
                hour=timestamp.strftime('%H'),
                minute=timestamp.strftime('%M'),
                second=timestamp.strftime('%S'),
                timestamp=timestamp.strftime('%Y%m%d_%H%M%S')
            )
        except KeyError as e:
            self.logger.error(f"Invalid placeholder in video_naming_pattern: {e}")
            # Fallback to default naming
            return f"video_{timestamp.strftime('%Y%m%d_%H%M%S')}.mp4"
        except Exception as e:
            self.logger.error(f"Error generating filename: {e}", exc_info=True)
            # Fallback to default naming
            return f"video_{timestamp.strftime('%Y%m%d_%H%M%S')}.mp4"
    
    def _get_folder_structure(self, timestamp: datetime):
        """Generate hierarchical folder structure: DD-MM-YYYY/HH/"""
        date_folder = timestamp.strftime('%d-%m-%Y')
        hour_folder = timestamp.strftime('%H')
        return f"{date_folder}/{hour_folder}"
    
    def _verify_video_file(self, filepath):
        """Basic verification of video file integrity."""
        try:
            if not os.path.exists(filepath):
                return False, "File does not exist"
            
            file_size = os.path.getsize(filepath)
            if file_size == 0:
                return False, "File is empty"
            
            # Check if file has reasonable size
            expected_min_size = 1024 * 1024  # At least 1MB
            if file_size < expected_min_size:
                return False, f"File size too small: {file_size} bytes (expected at least {expected_min_size})"
            
            # Try to open with OpenCV to verify it's a valid video
            cap = cv2.VideoCapture(filepath)
            if not cap.isOpened():
                return False, "Cannot open video file with OpenCV"
            
            # Check if we can read at least one frame
            ret, frame = cap.read()
            cap.release()
            
            if not ret or frame is None:
                return False, "Cannot read frames from video file"
            
            return True, "Video file appears valid"
            
        except Exception as e:
            return False, f"Error verifying file: {e}"
    
    def _cleanup_finished_threads(self):
        """Clean up finished upload threads."""
        with self.upload_lock:
            # Remove finished threads
            finished_threads = [t for t in self.upload_threads if not t.is_alive()]
            self.upload_threads = [t for t in self.upload_threads if t.is_alive()]
            
            if finished_threads:
                self.logger.debug(f"[CLEANUP] Removed {len(finished_threads)} finished upload threads")

    # ------------------------------------------------------------------ #
    # UPLOAD THREAD
    # ------------------------------------------------------------------ #
    def _upload_worker(self, local_path, s3_key, filename, max_retries=3):
        """Upload file to S3 with retry logic, metadata, and detailed statistics."""
        if self.s3_client is None:
            self.logger.error(f"[UPLOAD] S3 client unavailable, cannot upload {s3_key}")
            with self.upload_lock:
                self.active_uploads -= 1
            return
        
        if not os.path.exists(local_path):
            self.logger.error(f"[UPLOAD] File not found: {local_path}")
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
        finally:
            with self.upload_lock:
                self.active_uploads -= 1
                self.logger.info(f"[UPLOAD] Upload thread finished for {filename}")

    # ------------------------------------------------------------------ #
    # RECORDING
    # ------------------------------------------------------------------ #
    def record_video(self):
        """Record one video and trigger upload with error handling."""
        if self.camera is None:
            self.logger.error("[RECORD] Camera not available, attempting to reinitialize...")
            if not self._reinit_camera():
                self.logger.error("[RECORD] Cannot record: camera unavailable")
                return False
        
        try:
            ts = datetime.now()
            filename = self._generate_filename(ts)
            local_path = os.path.join(self.local_storage_path, filename)
            folder_structure = self._get_folder_structure(ts)
            s3_key = f"{folder_structure}/{filename}"

            # Clean up finished threads before starting new recording
            self._cleanup_finished_threads()

            self.logger.info(f"[RECORD] Starting recording: {filename} (Active uploads: {self.active_uploads})")
            self.logger.info(f"[RECORD] Color format: {'BGR888 (no conversion)' if getattr(self, 'use_bgr_format', False) else 'RGB888 (with conversion)'}")
            self.logger.info(f"[RECORD] Recording {self.recording_duration * 60}s at {self.fps}fps using native PiCamera2 recording...")

            enc = H264Encoder(bitrate=self.bitrate)
            out = FfmpegOutput(local_path)
            
            recording_start_time = time.time()
            duration_seconds = self.recording_duration * 60
            
            # Start recording with error handling
            try:
                self.camera.start_recording(enc, out)
            except Exception as e:
                self.logger.error(f"[RECORD] Failed to start recording: {e}", exc_info=True)
                # Try to reinitialize camera
                if not self._reinit_camera():
                    return False
                # Retry once
                try:
                    self.camera.start_recording(enc, out)
                except Exception as e2:
                    self.logger.error(f"[RECORD] Retry failed: {e2}", exc_info=True)
                    return False
            
            # Record for the specified duration
            try:
                time.sleep(duration_seconds)
            except KeyboardInterrupt:
                self.logger.info("[RECORD] Recording interrupted by user")
                raise
            
            # Stop recording
            try:
                self.camera.stop_recording()
                actual_duration = time.time() - recording_start_time
                self.logger.info(f"[RECORD] Native recording completed: {filename}")
                self.logger.info(f"[RECORD] Target duration: {duration_seconds}s, Actual duration: {actual_duration:.1f}s")
            except Exception as e:
                self.logger.error(f"[RECORD] Error stopping recording: {e}", exc_info=True)
                # Continue anyway, file might still be valid

            # Verify file integrity
            is_valid, message = self._verify_video_file(local_path)
            if not is_valid:
                self.logger.error(f"[RECORD] Video file verification failed: {message}")
                return False
            
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            self.logger.info(f"[RECORD] Recording completed: {filename} ({file_size_mb:.1f} MB) - {message}")

            # Check if file exists before uploading
            if os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                # Upload if S3 client is available
                if self.s3_client is not None:
                    # Wait if we've reached max concurrent uploads
                    while self.active_uploads >= self.max_concurrent_uploads:
                        self.logger.info(f"[RECORD] Max concurrent uploads ({self.max_concurrent_uploads}) reached, waiting...")
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
                    self.logger.info(f"[RECORD] Upload thread started for {filename} (Active uploads: {self.active_uploads})")
                else:
                    self.logger.warning(f"[RECORD] S3 client unavailable, skipping upload for {filename}")
                    # Try to reinitialize S3 client for next time
                    self._reinit_s3_client()
            else:
                self.logger.error(f"[RECORD] Recorded file is missing or empty: {local_path}")
            
            return True
        except KeyboardInterrupt:
            raise  # Re-raise keyboard interrupt
        except Exception as e:
            self.logger.error(f"[RECORD] Unexpected error during recording: {e}", exc_info=True)
            return False

    # ------------------------------------------------------------------ #
    def estimate_file_size_and_upload_time(self, upload_speed_mbps=10):
        """Estimate file size and upload time based on configuration."""
        # Calculate expected file size
        duration_seconds = self.recording_duration * 60
        file_size_bits = self.bitrate * duration_seconds
        file_size_mb = file_size_bits / (8 * 1024 * 1024)
        
        # Estimate upload time
        upload_time_seconds = (file_size_mb * 8) / upload_speed_mbps
        
        self.logger.info(f"=== Upload Estimates ===")
        self.logger.info(f"Recording duration: {self.recording_duration} minutes ({duration_seconds} seconds)")
        self.logger.info(f"Bitrate: {self.bitrate/1_000_000:.1f} Mbps")
        self.logger.info(f"Expected file size: {file_size_mb:.2f} MB")
        self.logger.info(f"Estimated upload time at {upload_speed_mbps} Mbps: {upload_time_seconds:.1f} seconds ({upload_time_seconds/60:.1f} minutes)")
        
        return file_size_mb, upload_time_seconds
    
    def record_single_video(self):
        """Record a single video."""
        try:
            if not self._setup_camera():
                self.logger.error("Failed to setup camera for single recording")
                return None, None
            
            # Show upload estimates
            self.estimate_file_size_and_upload_time()
            
            success = self.record_video()
            if success:
                self.logger.info("[RECORD] Single recording completed")
                
                # Wait for upload to complete before exiting
                self.logger.info("[RECORD] Waiting for upload to complete...")
                while self.active_uploads > 0:
                    time.sleep(1)
                self.logger.info("[RECORD] All uploads completed")
                return True
            else:
                return False
        finally:
            self.cleanup()
    
    # ------------------------------------------------------------------ #
    def start_continuous_recording(self):
        """Continuous recording loop with error recovery."""
        self.logger.info("Starting continuous recording loop...")
        
        # Initial camera setup
        if not self._setup_camera():
            self.logger.error("Failed to initialize camera. Will retry in main loop.")
        else:
            # Show upload estimates
            self.estimate_file_size_and_upload_time()
        
        consecutive_errors = 0
        max_consecutive_errors = 10
        error_backoff = 5  # seconds
        
        try:
            while True:
                try:
                    # Check camera before recording
                    if self.camera is None:
                        self.logger.warning("Camera not initialized, attempting setup...")
                        if not self._reinit_camera():
                            self.logger.error(f"Camera setup failed. Waiting {error_backoff} seconds before retry...")
                            time.sleep(error_backoff)
                            consecutive_errors += 1
                            if consecutive_errors >= max_consecutive_errors:
                                self.logger.error("Too many consecutive errors. Waiting longer before retry...")
                                time.sleep(60)  # Wait 1 minute before trying again
                                consecutive_errors = 0
                            continue
                    
                    # Attempt to record
                    success = self.record_video()
                    
                    if success:
                        consecutive_errors = 0  # Reset error counter on success
                    else:
                        consecutive_errors += 1
                        self.logger.warning(f"Recording failed. Consecutive errors: {consecutive_errors}")
                        
                        if consecutive_errors >= max_consecutive_errors:
                            self.logger.error("Too many consecutive recording errors. Waiting before retry...")
                            time.sleep(30)
                            consecutive_errors = 0
                            # Try to reinitialize camera
                            self._reinit_camera()
                    
                except KeyboardInterrupt:
                    self.logger.info("Stopped by user (KeyboardInterrupt).")
                    raise
                except Exception as e:
                    consecutive_errors += 1
                    self.logger.error(f"Unexpected error in recording loop: {e}", exc_info=True)
                    
                    if consecutive_errors >= max_consecutive_errors:
                        self.logger.error("Too many consecutive errors. Waiting 60 seconds before retry...")
                        time.sleep(60)
                        consecutive_errors = 0
                        # Try to reinitialize everything
                        self._reinit_camera()
                        self._reinit_s3_client()
                    else:
                        time.sleep(error_backoff)
                        
        except KeyboardInterrupt:
            self.logger.info("Stopped by user.")
        except Exception as e:
            self.logger.error(f"Fatal error in main loop: {e}", exc_info=True)
        finally:
            self.cleanup()

    def cleanup(self):
        """Cleanup resources and wait for uploads to finish."""
        try:
            # Wait for all uploads to complete
            if self.upload_threads:
                self.logger.info(f"[CLEANUP] Waiting for {len(self.upload_threads)} upload threads to complete...")
                for thread in self.upload_threads:
                    if thread.is_alive():
                        thread.join(timeout=30)  # Wait max 30 seconds per thread
            
            # Stop camera
            if self.camera is not None:
                try:
                    self.camera.stop()
                except:
                    pass
                try:
                    self.camera.close()
                except:
                    pass
                self.logger.info("[CLEANUP] Camera resources cleaned up")
                
        except Exception as e:
            self.logger.error(f"[CLEANUP] Error during cleanup: {e}", exc_info=True)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Pi Video Recorder Uploader")
    parser.add_argument("--config", default="config.conf", help="Configuration file path")
    parser.add_argument("--single", action="store_true", help="Record single video instead of continuous")
    args = parser.parse_args()
    
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


if __name__ == "__main__":
    main()
