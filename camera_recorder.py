#!/usr/bin/env python3
"""
Camera Recording Module for Raspberry Pi 5
Handles all camera setup, video recording, and file management.
"""

import os
import time
import logging
from datetime import datetime
from pathlib import Path
import numpy as np
import av
from picamera2 import Picamera2, MappedArray
from picamera2.encoders import H264Encoder
from picamera2.outputs import FfmpegOutput
import libcamera


class CameraRecorder:
    """Handles camera setup and video recording functionality."""
    
    def __init__(self, config, logger=None):
        """
        Initialize CameraRecorder.
        
        Args:
            config: ConfigParser object with camera and recording settings
            logger: Optional logger instance (creates one if not provided)
        """
        self.config = config
        self.logger = logger or self._setup_logging()
        self.camera = None
        
        self._load_config()
        Path(self.local_storage_path).mkdir(parents=True, exist_ok=True)
        
        # Clean up old recordings on startup
        self._cleanup_old_recordings()
    
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
        """Load camera and recording configuration."""
        try:
            # Camera settings
            self.analog_gain = float(self.config.get("camera", "analog_gain"))
            self.shutter_speed = int(self.config.get("camera", "shutter_speed"))
            self.resolution_width = int(self.config.get("camera", "resolution_width"))
            self.resolution_height = int(self.config.get("camera", "resolution_height"))
            self.fps = int(self.config.get("camera", "fps"))
            self.bitrate = int(self.config.get("camera", "bitrate"))
            self.force_color_format = self.config.get("camera", "force_color_format", fallback=None)
            
            # Recording settings
            self.recording_duration = int(self.config.get("recording", "duration_minutes"))
            self.video_naming_pattern = self.config.get("recording", "video_naming_pattern")
            self.local_storage_path = self.config.get("recording", "local_storage_path")
            
            # Store settings
            self.store_code = self.config.get("storeyes", "store_code")
            
            self.logger.info("Camera configuration loaded successfully.")
        except Exception as e:
            self.logger.error(f"Error loading camera configuration: {e}", exc_info=True)
            raise
    
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
    
    def _check_camera_formats(self):
        """Check and display supported camera formats for debugging."""
        try:
            camera_info = self.camera.camera_config
            self.logger.info("=== Camera Format Information ===")
            self.logger.info(f"Camera: {camera_info.get('model', 'Unknown')}")
            
            try:
                bgr_config = self.camera.create_preview_configuration(
                    main={"size": (640, 480), "format": "BGR888"}
                )
                self.logger.info("✓ BGR888 format is supported")
            except Exception as e:
                self.logger.info(f"✗ BGR888 format not supported: {e}")
            
            try:
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
        """Runs on every captured frame BEFORE encoding."""
        with MappedArray(request, "main") as m:
            # 180° rotation = flip both axes (flip vertically and horizontally)
            m.array[:] = np.flip(np.flip(m.array, axis=0), axis=1)
    
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
                            transform=libcamera.Transform(rotate=180)
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
            return f"video_{timestamp.strftime('%Y%m%d_%H%M%S')}.mp4"
        except Exception as e:
            self.logger.error(f"Error generating filename: {e}", exc_info=True)
            return f"video_{timestamp.strftime('%Y%m%d_%H%M%S')}.mp4"
    
    def _get_folder_structure(self, timestamp: datetime):
        """Generate hierarchical folder structure: <date>/<store_code>/hour/"""
        date_folder = timestamp.strftime('%Y-%m-%d')
        hour_folder = timestamp.strftime('%H')
        return f"{date_folder}/{self.store_code}/{hour_folder}"
    
    def _verify_video_file(self, filepath):
        """Basic verification of video file integrity using PyAV."""
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
            
            # Try to open with PyAV to verify it's a valid video
            try:
                container = av.open(filepath)
            except Exception as e:
                return False, f"Cannot open video file with PyAV: {e}"
            
            # Check if container has video streams
            if len(container.streams.video) == 0:
                container.close()
                return False, "No video streams found in file"
            
            # Try to read at least one frame
            try:
                video_stream = container.streams.video[0]
                frame_count = 0
                for frame in container.decode(video_stream):
                    frame_count += 1
                    if frame_count >= 1:  # Successfully read at least one frame
                        break
                
                container.close()
                
                if frame_count == 0:
                    return False, "Cannot read frames from video file"
                
                return True, "Video file appears valid"
            except Exception as e:
                container.close()
                return False, f"Cannot read frames from video file: {e}"
            
        except Exception as e:
            return False, f"Error verifying file: {e}"
    
    def record_video(self, upload_callback=None):
        """
        Record one video and optionally trigger upload callback.
        
        Args:
            upload_callback: Optional callback function(local_path, s3_key, filename) to handle upload
        
        Returns:
            tuple: (success: bool, local_path: str, s3_key: str, filename: str) or (False, None, None, None)
        """
        if self.camera is None:
            self.logger.error("[RECORD] Camera not available, attempting to reinitialize...")
            if not self._reinit_camera():
                self.logger.error("[RECORD] Cannot record: camera unavailable")
                return False, None, None, None
        
        try:
            ts = datetime.now()
            filename = self._generate_filename(ts)
            local_path = os.path.join(self.local_storage_path, filename)
            folder_structure = self._get_folder_structure(ts)
            s3_key = f"{folder_structure}/{filename}"
            
            self.logger.info(f"[RECORD] Starting recording: {filename}")
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
                if not self._reinit_camera():
                    return False, None, None, None
                try:
                    self.camera.start_recording(enc, out)
                except Exception as e2:
                    self.logger.error(f"[RECORD] Retry failed: {e2}", exc_info=True)
                    return False, None, None, None
            
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
            
            # Verify file integrity
            is_valid, message = self._verify_video_file(local_path)
            if not is_valid:
                self.logger.error(f"[RECORD] Video file verification failed: {message}")
                return False, None, None, None
            
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            self.logger.info(f"[RECORD] Recording completed: {filename} ({file_size_mb:.1f} MB) - {message}")
            
            # Call upload callback if provided
            if upload_callback and os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                upload_callback(local_path, s3_key, filename)
            
            return True, local_path, s3_key, filename
            
        except KeyboardInterrupt:
            raise
        except Exception as e:
            self.logger.error(f"[RECORD] Unexpected error during recording: {e}", exc_info=True)
            return False, None, None, None
    
    def cleanup(self):
        """Cleanup camera resources."""
        try:
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
            self.logger.error(f"[CLEANUP] Error during camera cleanup: {e}", exc_info=True)

