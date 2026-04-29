"""Main orchestrator: store settings, camera recording, cloud upload, MQTT sync."""

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
from mqtt_client import MqttSyncClient

CACHE_SETTINGS_PATH = Path("cache/settings.json")


def _get_pi_serial_id():
    """Read Raspberry Pi serial ID from /proc/cpuinfo."""
    try:
        with open("/proc/cpuinfo", "r", encoding="utf-8") as f:
            for line in f:
                if line.startswith("Serial"):
                    return line.split(":", 1)[1].strip()
    except (OSError, IndexError):
        pass
    return ""


class VideoRecorder:
    """Main orchestrator class that combines camera recording and cloud upload functionality."""

    def __init__(self, config_file="config.conf", imx500_overlay=False):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        self.imx500_overlay = bool(imx500_overlay)

        # Setup logging with file handler
        self._setup_logging()

        # Fetch and apply store settings from API (if configured)
        self._apply_store_settings()

        # Initialize cloud uploader first (before camera) for pending retry
        self.cloud_uploader = CloudUploader(self.config, self.logger)
        self.cloud_uploader.retry_pending_uploads()

        # Initialize camera recorder (cleanup will skip pending upload files)
        self.camera_recorder = CameraRecorder(self.config, self.logger, imx500_overlay=self.imx500_overlay)

        # Start MQTT sync-settings listener if BROKER and device_id available
        self.mqtt_client = None
        self._start_mqtt_sync()

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

    def _on_sync_settings(self, payload=None):
        """Called when MQTT sync-settings message received.
        If payload is JSON with name and value: apply single setting (optimized).
        Else: full sync from API."""
        if payload:
            try:
                data = json.loads(payload) if isinstance(payload, str) else payload
                name = data.get("name")
                value = data.get("value")
                if name is not None and value is not None:
                    self._apply_single_setting(str(name), value)
                    return
            except (json.JSONDecodeError, TypeError, AttributeError):
                pass
        # Fallback: full sync
        self.logger.info("[STORE] Sync-settings triggered, full sync from API...")
        self._apply_store_settings()
        self.camera_recorder.reload_settings(force_camera_reinit=True)
        self.cloud_uploader.reload_settings()

    # Setting name -> (config section, config key, value transform, camera_reinit, cloud_reload)
    _SETTING_ACTIONS = {
        # Camera hardware - need reinit
        "shutter-speed": ("camera", "shutter_speed", lambda v: str(v), True, False),
        "analog-gain": ("camera", "analog_gain", lambda v: str(v), True, False),
        "flip": ("camera", "reverse_camera", lambda v: str(str(v).lower() in ("true", "1", "yes")), True, False),
        # Camera/encoder - no reinit, applies on next recording
        "bitrate": ("camera", "bitrate", lambda v: str(v), False, False),
        # Recording - applies on next recording
        "chunk-duration": ("recording", "duration_minutes", lambda v: str(v), False, False),
        "s3-location": ("gcs", "bucket_location", lambda v: str(v).strip()[5:] if str(v).strip().startswith("s3://") else str(v), False, True),
        # Register
        "delta-time": ("register", "delta_time", lambda v: str(v), False, False),
    }

    def _apply_single_setting(self, name, value):
        """Apply a single setting by name and value. Uses targeted action per setting."""
        action = self._SETTING_ACTIONS.get(name)
        if not action:
            self.logger.warning(f"[STORE] Unknown setting name: {name}")
            return
        section, key, transform, camera_reinit, cloud_reload = action
        try:
            config_value = transform(value)
            if not self.config.has_section(section):
                self.config.add_section(section)
            self.config.set(section, key, config_value)
            self.logger.info(f"[STORE] Applied {name}={config_value}")
        except Exception as e:
            self.logger.warning(f"[STORE] Failed to apply {name}: {e}")
            return
        # Update cache so it reflects the new value (for restart fallback)
        self._update_cache_single(name, value)
        self.camera_recorder.reload_settings(force_camera_reinit=camera_reinit)
        if cloud_reload:
            self.cloud_uploader.reload_settings()

    def _update_cache_single(self, name, value):
        """Merge single setting into cache/settings.json."""
        name_to_api = {"shutter-speed": ("CAMERA", "shutter-speed"), "analog-gain": ("CAMERA", "analog-gain"),
                      "flip": ("CAMERA", "flip"), "bitrate": ("CAMERA", "bitrate"),
                      "chunk-duration": ("RECORDING", "chunk-duration"), "s3-location": ("RECORDING", "s3-location"),
                      "delta-time": ("REGISTER", "delta-time")}
        path = name_to_api.get(name)
        if not path or not CACHE_SETTINGS_PATH.exists():
            return
        try:
            with open(CACHE_SETTINGS_PATH, "r", encoding="utf-8") as f:
                cache = json.load(f)
            api_section, api_key = path
            if api_section not in cache:
                cache[api_section] = {}
            cache[api_section][api_key] = str(value)
            with open(CACHE_SETTINGS_PATH, "w", encoding="utf-8") as f:
                json.dump(cache, f, indent=2)
        except Exception as e:
            self.logger.debug(f"[STORE] Could not update cache: {e}")

    def _start_mqtt_sync(self):
        """Start MQTT client to subscribe to sync-settings if BROKER and device_id available."""
        device_id = self.config.get("api", "device_id", fallback="").strip() if self.config.has_section("api") else ""
        if not device_id:
            device_id = _get_pi_serial_id()
        if not device_id:
            return
        broker = {}
        if CACHE_SETTINGS_PATH.exists():
            try:
                with open(CACHE_SETTINGS_PATH, "r", encoding="utf-8") as f:
                    settings = json.load(f)
                broker = settings.get("BROKER", {})
            except Exception as e:
                self.logger.warning(f"[MQTT] Could not read broker from cache: {e}")
        host = broker.get("host", "").strip()
        port = broker.get("port", "1883")
        username = broker.get("username", "").strip()
        password = broker.get("password", "").strip()
        if not host or not username:
            self.logger.info("[MQTT] BROKER not in store settings, sync-settings disabled")
            return
        self.mqtt_client = MqttSyncClient(
            host=host,
            port=port,
            username=username,
            password=password,
            device_id=device_id,
            on_sync_callback=self._on_sync_settings,
            logger=self.logger,
        )
        if self.mqtt_client.start():
            self.logger.info("[MQTT] Sync-settings listener started")
        else:
            self.mqtt_client = None

    def record_single_video(self):
        """Record a single video."""
        try:
            if not self.camera_recorder._setup_camera():
                self.logger.error("Failed to setup camera for single recording")
                return False

            # Show upload estimates
            self.cloud_uploader.estimate_file_size_and_upload_time()

            # Record video with upload callback
            success, local_path, s3_key, filename = self.camera_recorder.record_video(
                upload_callback=self.cloud_uploader.upload_file
            )

            if success:
                self.logger.info("[RECORD] Single recording completed")
                # Wait for upload to complete before exiting
                self.cloud_uploader.wait_for_uploads()
                return True
            else:
                return False
        finally:
            self.cleanup()

    def start_continuous_recording(self):
        """Continuous recording loop with error recovery."""
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
        last_pending_retry = time.time()
        retry_interval_sec = self.cloud_uploader.pending_retry_interval_minutes * 60

        try:
            main_yield = float(self.config.get("recording", "main_loop_yield_seconds", fallback="0") or "0")
        except ValueError:
            main_yield = 0.0
        main_yield = max(0.0, min(main_yield, 10.0))

        try:
            while True:
                try:
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

                    # Periodic retry of pending uploads
                    if retry_interval_sec > 0 and (time.time() - last_pending_retry) >= retry_interval_sec:
                        self.cloud_uploader.retry_pending_uploads()
                        last_pending_retry = time.time()

                    # Attempt to record
                    success, local_path, s3_key, filename = self.camera_recorder.record_video(
                        upload_callback=self.cloud_uploader.upload_file
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
                            # Try to reinitialize camera
                            self.camera_recorder._reinit_camera()

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
                        self.camera_recorder._reinit_camera()
                        self.cloud_uploader._reinit_s3_client()
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
            if self.mqtt_client:
                self.mqtt_client.stop()
                self.mqtt_client = None
            # Cleanup camera
            self.camera_recorder.cleanup()
            # Cleanup uploads
            self.cloud_uploader.cleanup()
        except Exception as e:
            self.logger.error(f"[CLEANUP] Error during cleanup: {e}", exc_info=True)
