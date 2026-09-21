"""Main orchestrator: store settings, camera recording, cloud upload."""

import json
import time
import configparser
import logging
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler

from camera_recorder import CameraRecorder
from cloud_uploader import CloudUploader
from api_client import create_api_client

CACHE_SETTINGS_PATH = Path("cache/settings.json")


class VideoRecorder:
    """Main orchestrator class that combines camera recording and cloud upload functionality."""

    def __init__(self, config_file="config.conf", imx500_overlay=False, upload_only=False):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        self.imx500_overlay = bool(imx500_overlay)
        # upload_only: skip the camera entirely (used by --reconcile and the held-clip flush).
        self.upload_only = bool(upload_only)

        # Setup logging with file handler
        self._setup_logging()

        # Fetch and apply store settings from API (if configured)
        self._apply_store_settings()

        # Initialize cloud uploader first (before camera) for pending retry
        self.cloud_uploader = CloudUploader(self.config, self.logger)

        if self.upload_only:
            # No camera, no automatic upload — reconcile()/upload_deferred() drive them.
            self.camera_recorder = None
            return

        # Nothing is uploaded while the camera runs: clips are held in pending and flushed
        # once the session ends. Anything left over from an earlier session is flushed then too.
        self.logger.info("[DEFER] Uploads are held until the session ends")

        # Initialize camera recorder (cleanup will skip pending upload files)
        self.camera_recorder = CameraRecorder(self.config, self.logger, imx500_overlay=self.imx500_overlay)

    def reconcile(self):
        """Upload all clips still saved on the SD card (pending + dead-letter), then return the count."""
        try:
            return self.cloud_uploader.reconcile()
        finally:
            self.cloud_uploader.cleanup()

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
            maxBytes=10 * 1024 * 1024,  # 10MB
            backupCount=5,
            encoding="utf-8",
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

    def _apply_store_settings(self):
        """Fetch store settings from API and merge into config. Uses cache/settings.json as fallback on API error."""
        api_client = create_api_client(self.config, self.logger)
        settings = None
        if api_client:
            settings = api_client.get_store_settings()
            if settings:
                try:
                    CACHE_SETTINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
                    with open(CACHE_SETTINGS_PATH, "w", encoding="utf-8") as f:
                        json.dump(settings, f, indent=2)
                except Exception as e:
                    self.logger.warning(f"[STORE] Failed to save settings cache: {e}")
        if not settings and CACHE_SETTINGS_PATH.exists():
            try:
                with open(CACHE_SETTINGS_PATH, "r", encoding="utf-8") as f:
                    settings = json.load(f)
                self.logger.info("[STORE] Using cached settings (API unavailable)")
            except Exception as e:
                self.logger.warning(f"[STORE] Failed to load settings cache: {e}")
        if not settings:
            raise RuntimeError(
                "Store settings required from API or cache. API unreachable and cache/settings.json missing or invalid."
            )
        try:
            if "RECORDING" in settings:
                rec = settings["RECORDING"]
                if "s3-location" in rec:
                    loc = str(rec["s3-location"]).strip()
                    if loc.startswith("s3://"):
                        loc = loc[5:]  # strip s3://
                    self.config.set("gcs", "bucket_location", loc)
                    self.logger.info(f"[STORE] Applied s3-location: {loc}")
                if "chunk-duration" in rec:
                    self.config.set("recording", "duration_minutes", str(rec["chunk-duration"]))
                    self.logger.info(f"[STORE] Applied chunk-duration: {rec['chunk-duration']}")
            if "REGISTER" in settings and "delta-time" in settings["REGISTER"]:
                dt = settings["REGISTER"]["delta-time"]
                if not self.config.has_section("register"):
                    self.config.add_section("register")
                self.config.set("register", "delta_time", str(dt))
                self.logger.info(f"[STORE] Applied register delta-time: {dt}")
            if "BUSINESS_HOUR" in settings:
                bh = settings["BUSINESS_HOUR"]
                if not self.config.has_section("business_hour"):
                    self.config.add_section("business_hour")
                for api_key, cfg_key in (("start-time", "start_time"), ("end-time", "end_time")):
                    val = str(bh.get(api_key, "")).strip()
                    if val:  # never clobber the config.conf fallback with an empty value
                        self.config.set("business_hour", cfg_key, val)
                        self.logger.info(f"[STORE] Applied {api_key}: {val}")
            if "CAMERA" in settings:
                cam = settings["CAMERA"]
                if "shutter-speed" in cam:
                    self.config.set("camera", "shutter_speed", str(cam["shutter-speed"]))
                    self.logger.info(f"[STORE] Applied shutter-speed: {cam['shutter-speed']}")
                if "analog-gain" in cam:
                    self.config.set("camera", "analog_gain", str(cam["analog-gain"]))
                    self.logger.info(f"[STORE] Applied analog-gain: {cam['analog-gain']}")
                if "bitrate" in cam:
                    self.config.set("camera", "bitrate", str(cam["bitrate"]))
                    self.logger.info(f"[STORE] Applied bitrate: {cam['bitrate']}")
                if "flip" in cam:
                    flip_val = str(cam["flip"]).lower() in ("true", "1", "yes")
                    self.config.set("camera", "reverse_camera", str(flip_val))
                    self.logger.info(f"[STORE] Applied flip (reverse_camera): {flip_val}")
            if "AWS" in settings:
                for key, val in settings["AWS"].items():
                    if not self.config.has_section("aws"):
                        self.config.add_section("aws")
                    self.config.set("aws", key.replace("-", "_"), str(val))
            if "MAIL" in settings:
                for key, val in settings["MAIL"].items():
                    if not self.config.has_section("mail"):
                        self.config.add_section("mail")
                    self.config.set("mail", key.replace("-", "_"), str(val))
        except Exception as e:
            self.logger.warning(f"[STORE] Failed to apply settings: {e}", exc_info=True)

    def record_single_video(self):
        """Record a single video."""
        try:
            if not self.camera_recorder._setup_camera():
                self.logger.error("Failed to setup camera for single recording")
                return False

            # Show upload estimates
            self.cloud_uploader.estimate_file_size_and_upload_time()

            # Record video; the clip is held and uploaded by cleanup() below
            success, local_path, s3_key, filename = self.camera_recorder.record_video(
                upload_callback=self.cloud_uploader.defer_upload
            )

            if success:
                self.logger.info("[RECORD] Single recording completed")
                return True
            else:
                return False
        finally:
            self.cleanup(upload_deferred=True)

    def start_continuous_recording(self, deadline_ts=None):
        """Continuous recording loop with error recovery.

        deadline_ts: optional time.time() value marking the end of the business-hours
        session. No new segment is started once it passes; the segment already running
        is allowed to finish, so the session can overrun by up to one chunk.
        """
        self.logger.info("Starting continuous recording loop...")

        # Initial camera setup
        if not self.camera_recorder._setup_camera():
            self.logger.error("Failed to initialize camera. Will retry in main loop.")
        else:
            # Show upload estimates
            self.cloud_uploader.estimate_file_size_and_upload_time()

        consecutive_errors = 0
        max_consecutive_errors = 10
        error_backoff = 5  # seconds

        try:
            main_yield = float(self.config.get("recording", "main_loop_yield_seconds", fallback="0") or "0")
        except ValueError:
            main_yield = 0.0
        main_yield = max(0.0, min(main_yield, 10.0))

        try:
            while True:
                try:
                    if deadline_ts is not None and time.time() >= deadline_ts:
                        self.logger.info("[SESSION] End-time reached; not starting another segment")
                        break

                    # Check camera (or rpicam-vid binary) before recording
                    if not self.camera_recorder.is_ready_to_record():
                        self.logger.warning("Recorder not ready, attempting camera/setup...")
                        if not self.camera_recorder._reinit_camera():
                            self.logger.error(f"Camera setup failed. Waiting {error_backoff} seconds before retry...")
                            time.sleep(error_backoff)
                            consecutive_errors += 1
                            if consecutive_errors >= max_consecutive_errors:
                                self.logger.error("Too many consecutive errors. Waiting longer before retry...")
                                time.sleep(60)  # Wait 1 minute before trying again
                                consecutive_errors = 0
                            continue

                    # Attempt to record
                    success, local_path, s3_key, filename = self.camera_recorder.record_video(
                        upload_callback=self.cloud_uploader.defer_upload
                    )

                    if success:
                        consecutive_errors = 0  # Reset error counter on success
                        if main_yield > 0:
                            time.sleep(main_yield)
                    else:
                        consecutive_errors += 1
                        self.logger.warning(f"Recording failed. Consecutive errors: {consecutive_errors}")

                        if consecutive_errors >= max_consecutive_errors:
                            self.logger.error("Too many consecutive recording errors. Waiting before retry...")
                            time.sleep(30)
                            consecutive_errors = 0
                            # Reopen the camera (_setup_camera closes the old instance first);
                            # _reinit_camera() is a no-op while a stalled instance still exists
                            self.camera_recorder._setup_camera(max_retries=3, retry_delay=5)

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
                        self.camera_recorder._setup_camera(max_retries=3, retry_delay=5)
                        self.cloud_uploader._reinit_s3_client()
                    else:
                        time.sleep(error_backoff)

        except KeyboardInterrupt:
            self.logger.info("Stopped by user.")
        except Exception as e:
            self.logger.error(f"Fatal error in main loop: {e}", exc_info=True)
        finally:
            # Leaving the loop — end-time reached or interrupted — releases the camera, so the
            # held clips can go up now. A SIGKILL (stop.sh) skips this and leaves them in
            # pending; the next session flushes them.
            self.cleanup(upload_deferred=True)

    @staticmethod
    def pending_uploads_exist(config_file="config.conf"):
        """True if pending_uploads.csv has any row — checked before paying for S3 init."""
        config = configparser.ConfigParser()
        config.read(config_file)
        path = Path(config.get("recording", "pending_uploads_csv", fallback="./pending_uploads.csv"))
        try:
            with open(path, "r", encoding="utf-8") as f:
                return sum(1 for line in f if line.strip()) > 1  # header + at least one row
        except OSError:
            return False

    def upload_deferred(self):
        """Upload held clips (e.g. left over from a session that was stopped early)."""
        try:
            return self.cloud_uploader.upload_deferred()
        finally:
            self.cloud_uploader.cleanup()

    def cleanup(self, upload_deferred=False):
        """Cleanup resources and wait for uploads to finish.

        upload_deferred: once the camera is released, upload the clips held during the session.
        """
        try:
            # Cleanup camera
            if self.camera_recorder is not None:
                self.camera_recorder.cleanup()
            if upload_deferred:
                self.cloud_uploader.upload_deferred()
            # Cleanup uploads
            self.cloud_uploader.cleanup()
        except Exception as e:
            self.logger.error(f"[CLEANUP] Error during cleanup: {e}", exc_info=True)
