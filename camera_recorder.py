#!/usr/bin/env python3
"""
Camera Recording Module for Raspberry Pi 5
Handles all camera setup, video recording, and file management.
"""

import csv
import gc
import os
import shutil
import shlex
import signal
import subprocess
import time
import logging
from datetime import datetime
from pathlib import Path
import numpy as np
import av
import prctl
from picamera2 import Picamera2, MappedArray
from picamera2.encoders import H264Encoder
from picamera2.outputs import FfmpegOutput
from picamera2.platform import Platform, get_platform
import libcamera


class FfmpegOutputLargeQueue(FfmpegOutput):
    """
    FfmpegOutput with raised thread_queue_size to avoid
    "Thread message queue blocking; consider raising the thread_queue_size" warnings.
    Default 64 is too low when CPU is busy (e.g. during parallel S3 uploads).
    """

    def __init__(self, output_filename, video_thread_queue_size=512, **kwargs):
        super().__init__(output_filename, **kwargs)
        self._video_thread_queue_size = video_thread_queue_size

    def start(self):
        general_options = ['-loglevel', 'warning', '-y']
        video_input = [
            '-use_wallclock_as_timestamps', '1',
            '-thread_queue_size', str(self._video_thread_queue_size),
            '-i', '-'
        ]
        video_codec = ['-c:v', 'copy']
        audio_input = []
        audio_codec = []
        if self.audio:
            audio_input = [
                '-itsoffset', str(self.audio_sync),
                '-f', 'pulse',
                '-sample_rate', str(self.audio_samplerate),
                '-thread_queue_size', '1024',
                '-i', self.audio_device
            ]
            audio_codec = ['-b:a', str(self.audio_bitrate), '-c:a', self.audio_codec]
            if self.audio_filter:
                audio_codec.extend(['-af', self.audio_filter])

        command = (
            ['ffmpeg'] + general_options + audio_input + video_input +
            audio_codec + video_codec + self.output_filename.split()
        )
        self.ffmpeg = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            preexec_fn=lambda: prctl.set_pdeathsig(signal.SIGKILL)
        )
        super(FfmpegOutput, self).start()


class CameraRecorder:
    """Handles camera setup and video recording functionality."""
    
    def __init__(self, config, logger=None, imx500_overlay=False):
        """
        Initialize CameraRecorder.
        
        Args:
            config: ConfigParser object with camera and recording settings
            logger: Optional logger instance (creates one if not provided)
        """
        self.config = config
        self.logger = logger or self._setup_logging()
        self.camera = None
        self.imx500_overlay = bool(imx500_overlay)
        self._imx500 = None
        self._imx500_intrinsics = None
        self._imx500_last_results = []
        self._recording_count = 0  # For periodic camera reinitialization
        self._pending_camera_reinit = False  # Set by reload_settings(); reinit before next recording

        self._load_config()
        if self.use_rpicam_vid and self.imx500_overlay:
            self.logger.warning("[CONFIG] use_rpicam_vid disabled: not compatible with IMX500 overlay")
            self.use_rpicam_vid = False
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
        """Load camera and recording configuration. CAMERA/RECORDING from API/cache only."""
        try:
            # Camera settings (from store settings API/cache only)
            self.analog_gain = float(self.config.get("camera", "analog_gain"))
            self.shutter_speed = int(self.config.get("camera", "shutter_speed"))
            self.resolution_width = int(self.config.get("camera", "resolution_width"))
            self.resolution_height = int(self.config.get("camera", "resolution_height"))
            self.fps = int(self.config.get("camera", "fps"))
            self.bitrate = int(self.config.get("camera", "bitrate"))
            self.force_color_format = self.config.get("camera", "force_color_format", fallback=None)
            self.reverse_camera = self.config.getboolean("camera", "reverse_camera")
            self.imx500_model = self.config.get(
                "camera",
                "imx500_model",
                fallback="/usr/share/imx500-models/imx500_network_ssd_mobilenetv2_fpnlite_320x320_pp.rpk",
            ).strip()
            self.imx500_threshold = float(self.config.get("camera", "imx500_threshold", fallback="0.55"))
            
            # Recording settings (from store settings API/cache only)
            self.recording_duration = int(self.config.get("recording", "duration_minutes"))
            self.video_naming_pattern = self.config.get("recording", "video_naming_pattern")
            self.local_storage_path = self.config.get("recording", "local_storage_path")
            self.pending_uploads_csv = self.config.get("recording", "pending_uploads_csv", fallback="./pending_uploads.csv")
            self.post_stop_delay_seconds = float(self.config.get("recording", "post_stop_delay_seconds", fallback="1.5"))
            self.periodic_camera_reinit_recordings = int(self.config.get("recording", "periodic_camera_reinit_recordings", fallback="0"))
            self.ffmpeg_video_thread_queue_size = int(self.config.get("recording", "ffmpeg_video_thread_queue_size", fallback="512"))
            # Optional: use official rpicam-vid instead of PiCamera2 (frees Python from holding the camera; good on Pi 5)
            self.use_rpicam_vid = self.config.getboolean("recording", "use_rpicam_vid", fallback=False)
            self.rpicam_vid_path = (self.config.get("recording", "rpicam_vid_path", fallback="") or "rpicam-vid").strip()
            self.rpicam_codec = (self.config.get("recording", "rpicam_codec", fallback="h264") or "h264").strip()
            self.rpicam_extra_args = self.config.get("recording", "rpicam_extra_args", fallback="").strip()
            
            # bucket_location from RECORDING.s3-location (API/cache only)
            self.bucket_location = self.config.get("gcs", "bucket_location").strip()
            
            self.logger.info("Camera configuration loaded successfully.")
        except Exception as e:
            self.logger.error(f"Error loading camera configuration: {e}", exc_info=True)
            raise

    def is_ready_to_record(self):
        """True when recording can start: PiCamera2 open, or rpicam-vid mode (binary ok)."""
        if getattr(self, "use_rpicam_vid", False):
            return self._resolve_rpicam_executable() is not None
        return self.camera is not None

    def _resolve_rpicam_executable(self):
        """Return full path to rpicam-vid, or None if not found."""
        p = self.rpicam_vid_path
        if os.path.isabs(p) and os.path.isfile(p) and os.access(p, os.X_OK):
            return p
        w = shutil.which(p)
        if w and os.path.isfile(w):
            return w
        if os.path.isabs(p):
            return p if os.path.isfile(p) and os.access(p, os.X_OK) else None
        return None

    def _build_rpicam_cmd(self, local_path, duration_seconds):
        """Assemble rpicam-vid argv. Bitrate in bits per second (see Raspberry Pi docs)."""
        exe = self._resolve_rpicam_executable()
        if not exe:
            raise RuntimeError("rpicam-vid not on PATH; set rpicam_vid_path in [recording]")

        cmd = [
            exe, "-n",
            "-o", local_path,
            "-t", f"{int(duration_seconds)}s",
            "--width", str(self.resolution_width),
            "--height", str(self.resolution_height),
            "--framerate", str(self.fps),
            "-b", str(self.bitrate),
            "-g", str(self.fps),
            "--shutter", str(self.shutter_speed),
            "--gain", str(self.analog_gain),
            "--inline",
        ]
        if self.rpicam_codec and self.rpicam_codec != "h264":
            cmd.extend(["--codec", self.rpicam_codec])
        if self.reverse_camera:
            cmd.extend(["--hflip", "--vflip"])
        if self.rpicam_extra_args:
            cmd.extend(shlex.split(self.rpicam_extra_args))
        return cmd

    def reload_settings(self, force_camera_reinit=True):
        """Reload configuration from config (e.g. after sync-settings MQTT message).
        If force_camera_reinit, marks camera for reinit so hardware settings (shutter, gain, flip)
        apply on next recording. Otherwise only in-memory config is updated."""
        self._load_config()
        if force_camera_reinit:
            self._pending_camera_reinit = True
            self.logger.info("[STORE] Camera settings reloaded (camera will reinit before next recording)")
        else:
            self.logger.info("[STORE] Camera settings reloaded (applies on next recording)")
    
    def _get_pending_local_paths(self):
        """Return set of normalized local_paths in pending uploads CSV (skip these during cleanup)."""
        if not os.path.exists(self.pending_uploads_csv):
            return set()
        try:
            paths = set()
            with open(self.pending_uploads_csv, "r", newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    p = row.get("local_path", "").strip()
                    if p:
                        try:
                            paths.add(str(Path(p).resolve()))
                        except (OSError, ValueError):
                            paths.add(p)
            return paths
        except Exception as e:
            self.logger.warning(f"[CLEANUP] Could not read pending uploads CSV: {e}")
            return set()
    
    def _cleanup_old_recordings(self):
        """Delete all existing video files in the recordings directory on startup. Skips files in pending uploads."""
        try:
            recordings_path = Path(self.local_storage_path)
            if not recordings_path.exists():
                return
            
            pending_paths = self._get_pending_local_paths()
            deleted_count = 0
            total_size = 0
            
            # Find all video files (common video extensions)
            video_extensions = {'.mp4', '.avi', '.mov', '.mkv', '.flv', '.wmv', '.m4v'}
            
            for file_path in recordings_path.iterdir():
                if file_path.is_file() and file_path.suffix.lower() in video_extensions:
                    # Skip files pending re-upload
                    try:
                        resolved = str(file_path.resolve())
                    except (OSError, ValueError):
                        resolved = str(file_path)
                    if resolved in pending_paths:
                        self.logger.info(f"[CLEANUP] Skipping pending upload: {file_path.name}")
                        continue
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

    def _extract_imx500_boxes(self, metadata):
        """Best-effort extraction of bounding boxes from IMX500 metadata.

        Returns a list of boxes in either:
        - (x1, y1, x2, y2) absolute pixels
        - (x, y, w, h) absolute pixels
        Values may also be normalized floats; caller should normalize if needed.
        """
        if not isinstance(metadata, dict) or not metadata:
            return []

        # Common candidate locations (different IMX500 pipelines expose different keys)
        candidates = []
        for k in ("imx500", "IMX500", "Imx500", "Objects", "objects", "detections", "Detections"):
            v = metadata.get(k)
            if v is not None:
                candidates.append(v)

        # Also scan for any top-level key containing 'imx500'
        for k, v in metadata.items():
            if isinstance(k, str) and "imx500" in k.lower():
                candidates.append(v)

        def _as_list(x):
            if x is None:
                return []
            if isinstance(x, list):
                return x
            if isinstance(x, dict):
                # Some formats embed list under known keys
                for key in ("objects", "detections", "boxes", "bboxes", "results"):
                    if key in x and isinstance(x[key], list):
                        return x[key]
            return []

        items = []
        for c in candidates:
            items.extend(_as_list(c))

        boxes = []
        for it in items:
            if isinstance(it, dict):
                # (xmin, ymin, xmax, ymax)
                if all(k in it for k in ("xmin", "ymin", "xmax", "ymax")):
                    boxes.append((it["xmin"], it["ymin"], it["xmax"], it["ymax"]))
                    continue
                # (x, y, w, h)
                if all(k in it for k in ("x", "y", "w", "h")):
                    boxes.append((it["x"], it["y"], it["w"], it["h"]))
                    continue
                # Alternate naming
                if all(k in it for k in ("x", "y", "width", "height")):
                    boxes.append((it["x"], it["y"], it["width"], it["height"]))
                    continue
                if "bbox" in it and isinstance(it["bbox"], (list, tuple)) and len(it["bbox"]) == 4:
                    boxes.append(tuple(it["bbox"]))
                    continue
            elif isinstance(it, (list, tuple)) and len(it) == 4:
                boxes.append(tuple(it))
        return boxes

    def _draw_rect(self, img, x1, y1, x2, y2, color, thickness=2):
        """Draw rectangle border directly into HxWxC numpy array."""
        h, w = img.shape[0], img.shape[1]
        x1 = max(0, min(w - 1, int(x1)))
        x2 = max(0, min(w - 1, int(x2)))
        y1 = max(0, min(h - 1, int(y1)))
        y2 = max(0, min(h - 1, int(y2)))
        if x2 <= x1 or y2 <= y1:
            return

        t = max(1, int(thickness))
        # top
        img[y1:y1 + t, x1:x2 + 1] = color
        # bottom
        img[y2 - t + 1:y2 + 1, x1:x2 + 1] = color
        # left
        img[y1:y2 + 1, x1:x1 + t] = color
        # right
        img[y1:y2 + 1, x2 - t + 1:x2 + 1] = color

    def imx500_overlay_callback(self, request):
        """Runs on every captured frame BEFORE encoding. Draws IMX500 bounding boxes if present."""
        try:
            md = request.get_metadata() if hasattr(request, "get_metadata") else {}
        except Exception:
            md = {}

        # IMX500 metadata does not usually contain ready-to-draw boxes; you must decode tensors
        # via the IMX500 helper (see picamera2 IMX500 demos).
        detections = []
        if self._imx500 is not None and self._imx500_intrinsics is not None:
            try:
                np_outputs = self._imx500.get_outputs(md, add_batch=True)
                if np_outputs is None:
                    detections = self._imx500_last_results
                else:
                    intr = self._imx500_intrinsics
                    threshold = float(getattr(self, "imx500_threshold", 0.55))
                    input_w, input_h = self._imx500.get_input_size()

                    # Standard object-detection models output (boxes, scores, classes)
                    boxes, scores, classes = np_outputs[0][0], np_outputs[1][0], np_outputs[2][0]

                    if getattr(intr, "bbox_normalization", False):
                        # Demos normalize by input_h (model input height)
                        boxes = boxes / input_h

                    if getattr(intr, "bbox_order", "yx") == "xy":
                        boxes = boxes[:, [1, 0, 3, 2]]

                    detections = []
                    for box, score, category in zip(boxes, scores, classes):
                        try:
                            if float(score) < threshold:
                                continue
                        except Exception:
                            continue
                        try:
                            x, y, w, h = self._imx500.convert_inference_coords(box, md, self.camera)
                            detections.append((x, y, w, h, float(score), int(category)))
                        except Exception:
                            continue
                    self._imx500_last_results = detections
            except Exception:
                # Fall back to best-effort parsing below.
                detections = []

        if not detections:
            boxes = self._extract_imx500_boxes(md)
            for b in boxes:
                if isinstance(b, (list, tuple)) and len(b) == 4:
                    try:
                        a, c, d, e = map(float, b)
                        detections.append((a, c, d, e, None, None))
                    except Exception:
                        pass

        if not detections:
            return

        with MappedArray(request, "main") as m:
            img = m.array
            # Green is (0,255,0) in both RGB and BGR.
            color = np.array([0, 255, 0], dtype=img.dtype)

            for det in detections:
                x, y, w, h, *_ = det
                # IMX500 helper returns x,y,w,h already in ISP output pixels.
                self._draw_rect(img, x, y, x + w, y + h, color=color, thickness=2)
    
    def _setup_camera(self, max_retries=5, retry_delay=5):
        """Configure PiCamera2 with retry logic; or rpicam-vid-only mode (no Python camera)."""
        if self.use_rpicam_vid:
            self._full_camera_reset()
            gc.collect()
            ex = self._resolve_rpicam_executable()
            if not ex:
                self.logger.error(
                    "[RECORD] rpicam-vid not found (apt install rpicam-apps) or set recording.rpicam_vid_path"
                )
                return False
            self._color_format_log = f"rpicam-vid -> {ex}"
            self.logger.info(
                f"[RECORD] rpicam-vid mode: PiCamera2 not used; segments recorded by: {ex}"
            )
            self.logger.info(
                f"Target capture {self.resolution_width}x{self.resolution_height}@{self.fps}fps, "
                f"bitrate {self.bitrate/1_000_000:.1f} Mbps"
            )
            return True

        for attempt in range(1, max_retries + 1):
            try:
                # Clean up previous camera instance; use full reset for thorough release
                self._full_camera_reset()
                gc.collect()
                
                # If IMX500 overlay is enabled, instantiate the IMX500 device helper so we can
                # decode inference tensors from metadata. Otherwise, boxes generally won't exist.
                self._imx500 = None
                self._imx500_intrinsics = None
                self._imx500_last_results = []
                if self.imx500_overlay:
                    try:
                        from picamera2.devices import IMX500
                        from picamera2.devices.imx500 import NetworkIntrinsics
                        self._imx500 = IMX500(self.imx500_model)
                        self._imx500_intrinsics = self._imx500.network_intrinsics or NetworkIntrinsics()
                        if getattr(self._imx500_intrinsics, "task", "") != "object detection":
                            self.logger.warning("[IMX500] Network task is not 'object detection'; overlay may not work")
                        # Ensure defaults (labels, bbox normalization/order, etc.)
                        try:
                            self._imx500_intrinsics.update_with_defaults()
                        except Exception:
                            pass
                        self.camera = Picamera2(self._imx500.camera_num)
                    except Exception as e:
                        self.logger.warning(f"[IMX500] Failed to initialize IMX500 helper, overlay disabled: {e}")
                        self._imx500 = None
                        self._imx500_intrinsics = None
                        self.camera = Picamera2()
                else:
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
                            transform=libcamera.Transform(hflip=True, vflip=True) if self.reverse_camera else libcamera.Transform()
                        )
                        self.camera.configure(video_config)
                        self.use_bgr_format = (self.force_color_format == "BGR888")
                        self._color_format_log = f"{self.force_color_format} (forced in config)"
                        self.logger.info(f"Camera configured with forced format: {self.force_color_format}")
                    except Exception as forced_error:
                        self.logger.error(f"Failed to configure camera with forced format {self.force_color_format}: {forced_error}")
                        raise
                else:
                    # Pi 4 (VC4): H.264 is V4L2 hardware; BGR is a good default.
                    # Pi 5 (PISP): H264Encoder is Libav/libx264 (no HW encode). YUV420 main avoids
                    # a heavy RGB→YUV path in the encoder. IMX500 overlay needs RGB-style buffers; keep BGR.
                    yuv_tried = False
                    if get_platform() != Platform.VC4 and not self.imx500_overlay:
                        try:
                            video_config = self.camera.create_video_configuration(
                                main={"size": (self.resolution_width, self.resolution_height), "format": "YUV420"},
                                controls={
                                    "ExposureTime": self.shutter_speed,
                                    "AnalogueGain": self.analog_gain,
                                    "FrameDurationLimits": (frame_duration_us, frame_duration_us),
                                    "AeEnable": True,
                                    "AeFlickerPeriod": 10000
                                },
                                transform=libcamera.Transform(hflip=True, vflip=True) if self.reverse_camera else libcamera.Transform()
                            )
                            self.camera.configure(video_config)
                            self.use_bgr_format = False
                            self._color_format_log = "YUV420 (Libav H.264; best CPU on Pi 5)"
                            yuv_tried = True
                            self.logger.info("Using YUV420 for software H.264 (reduces load vs BGR on Pi 5+)")
                        except Exception as yuv_e:
                            self.logger.warning(f"YUV420 main not available, trying BGR888: {yuv_e}")

                    if not yuv_tried:
                        # BGR888 first on Pi 4 (hw encode), or fallback on Pi 5
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
                                transform=libcamera.Transform(hflip=True, vflip=True) if self.reverse_camera else libcamera.Transform()
                            )
                            self.camera.configure(video_config)
                            self.use_bgr_format = True
                            bgr_msg = "BGR888 — V4L2 H.264 has no BGR->YUV in userspace" if get_platform() == Platform.VC4 else "BGR888 — Libav H.264 converts to YUV (higher CPU on Pi 5)"
                            self._color_format_log = bgr_msg
                            self.logger.info("Using BGR888 format" + (" - preferred for hardware encoder (Pi 4)" if get_platform() == Platform.VC4 else ""))
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
                                transform=libcamera.Transform(hflip=True, vflip=True) if self.reverse_camera else libcamera.Transform()
                            )
                            self.camera.configure(video_config)
                            self.use_bgr_format = False
                            self._color_format_log = "RGB888 (with conversion in encoder)"
                            self.logger.info("Using RGB888 format - color conversion will be applied")
                
                # Optional per-frame overlay (runs before encoding).
                self.camera.post_callback = self.imx500_overlay_callback if self.imx500_overlay else None
                self.camera.start()
                
                # Check and display supported formats for debugging
                self._check_camera_formats()
                
                self.logger.info(f"Camera started at {self.resolution_width}x{self.resolution_height}@{self.fps}fps")
                self.logger.info(f"Frame duration: {frame_duration_us}μs (target: {1_000_000/self.fps:.1f}μs)")
                self.logger.info(f"Analog gain: {self.analog_gain}, Shutter speed: {self.shutter_speed}μs")
                self.logger.info(f"Bitrate: {self.bitrate/1_000_000:.1f}Mbps")
                self.logger.info(f"Color format: {getattr(self, '_color_format_log', 'see above')}")
                if self.imx500_overlay:
                    self.logger.info(f"[IMX500] Overlay enabled (model={getattr(self, 'imx500_model', '')})")
                time.sleep(2)
                return True
            except Exception as e:
                self.logger.error(f"Camera setup failed (attempt {attempt}/{max_retries}): {e}", exc_info=True)
                self.camera = None
                gc.collect()
                if attempt < max_retries:
                    err_str = str(e).lower()
                    delay = 15 if ("resource busy" in err_str or "did not complete" in err_str) else retry_delay
                    self.logger.info(f"Retrying camera setup in {delay} seconds...")
                    time.sleep(delay)
                else:
                    self.logger.error("Failed to setup camera after all retries.")
                    return False
    
    def _reinit_camera(self):
        """Reinitialize camera if it failed previously."""
        if self.camera is None:
            self.logger.info("Attempting to reinitialize camera...")
            return self._setup_camera(max_retries=3, retry_delay=5)
        return True

    def _full_camera_reset(self):
        """
        Perform a complete camera reset: stop, close, and destroy the instance.
        Ensures pipeline and buffers are fully released. Sets self.camera = None.
        """
        self.logger.info("[RECOVERY] Performing full camera reset...")
        if self.camera is None:
            return
        try:
            try:
                self.camera.stop_recording()
            except Exception:
                pass
        except Exception:
            pass
        try:
            self.camera.stop()
        except Exception:
            pass
        try:
            self.camera.close()
        except Exception:
            pass
        self.camera = None
        gc.collect()
        time.sleep(2.5)
        self.logger.info("[RECOVERY] Full camera reset complete")

    def _safe_stop_recording(self):
        """
        Stop recording with defensive error handling, then wait for buffers to release.
        Call this instead of camera.stop_recording() directly.
        """
        if self.camera is None:
            return
        try:
            self.camera.stop_recording()
        except Exception as e:
            self.logger.warning(f"[RECORD] Error during stop_recording: {e}")
        finally:
            delay = getattr(self, "post_stop_delay_seconds", 1.5)
            time.sleep(delay)

    def _safe_start_recording(self, enc, out, local_path):
        """
        Safe wrapper for start_recording. Handles Broken pipe and encoder state issues.
        On failure: performs full camera reset, reinitializes camera, returns False.
        Caller MUST create a NEW encoder before retry - never reuse a failed encoder.

        Args:
            enc: H264Encoder instance
            out: FfmpegOutput instance
            local_path: Output file path (for logging)

        Returns:
            bool: True if recording started successfully, False otherwise
        """
        try:
            self.camera.start_recording(enc, out)
            return True
        except RuntimeError as e:
            err_str = str(e).lower()
            if "broken pipe" in err_str or "encoder already running" in err_str or "failed to start camera" in err_str:
                self.logger.error(f"[RECORD] Camera/encoder error during start_recording: {e}")
                self._full_camera_reset()
                if not self._setup_camera(max_retries=3, retry_delay=5):
                    return False
                return False
            raise
        except Exception as e:
            self.logger.error(f"[RECORD] Unexpected error during start_recording: {e}", exc_info=True)
            try:
                self.camera.stop_recording()
            except Exception:
                pass
            self._full_camera_reset()
            if not self._setup_camera(max_retries=3, retry_delay=5):
                return False
            return False
    
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
    
    def _get_s3_key_prefix(self, timestamp: datetime):
        """
        Generate S3 key: s3://<bucket_location>/{date}/{hour}/{filename}
        bucket_location = bucket + optional prefix (e.g. storeyes-videos/recordings).
        """
        bl = getattr(self, "bucket_location", "videos")
        parts = bl.split("/", 1)
        key_prefix = parts[1] if len(parts) > 1 else ""
        date_folder = timestamp.strftime('%d-%m-%Y')
        hour_folder = timestamp.strftime('%H')
        return f"{key_prefix}/{date_folder}/{hour_folder}" if key_prefix else f"{date_folder}/{hour_folder}"
    
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
        Uses safe start/stop helpers for production-stable 24/7 continuous recording.

        Args:
            upload_callback: Optional callback function(local_path, s3_key, filename) to handle upload

        Returns:
            tuple: (success: bool, local_path: str, s3_key: str, filename: str) or (False, None, None, None)
        """
        if not self.use_rpicam_vid:
            if self.camera is None:
                self.logger.error("[RECORD] Camera not available, attempting to reinitialize...")
                if not self._reinit_camera():
                    self.logger.error("[RECORD] Cannot record: camera unavailable")
                    return False, None, None, None

        # Reinit camera if settings were synced (shutter, gain, flip need hardware reconfig)
        if self._pending_camera_reinit:
            if self.use_rpicam_vid:
                self._pending_camera_reinit = False
                self.logger.info("[RECORD] Settings synced; rpicam-vid will use updated config on this segment")
            else:
                self.logger.info("[RECORD] Settings synced, reinitializing camera for new hardware config...")
                self._full_camera_reset()
                if not self._setup_camera(max_retries=3, retry_delay=5):
                    return False, None, None, None
                self._pending_camera_reinit = False

        reinit_interval = getattr(self, "periodic_camera_reinit_recordings", 0)
        if (not self.use_rpicam_vid) and reinit_interval > 0 and self._recording_count > 0 and self._recording_count % reinit_interval == 0:
            self.logger.info(f"[RECORD] Periodic camera reinit (every {reinit_interval} recordings)")
            self._full_camera_reset()
            if not self._setup_camera(max_retries=3, retry_delay=5):
                return False, None, None, None

        try:
            ts = datetime.now()
            filename = self._generate_filename(ts)
            local_path = os.path.join(self.local_storage_path, filename)
            key_prefix = self._get_s3_key_prefix(ts)
            s3_key = f"{key_prefix}/{filename}"

            self.logger.info(f"[RECORD] Starting recording: {filename}")
            self.logger.info(f"[RECORD] Color format: {getattr(self, '_color_format_log', 'see startup log')}")
            duration_seconds = self.recording_duration * 60
            if self.use_rpicam_vid:
                self.logger.info(
                    f"[RECORD] Recording {duration_seconds}s at {self.fps}fps with rpicam-vid -> {filename}"
                )
                try:
                    cmd = self._build_rpicam_cmd(local_path, duration_seconds)
                except Exception as e:
                    self.logger.error(f"[RECORD] {e}", exc_info=True)
                    return False, None, None, None
                self.logger.info(f"[RECORD] {' '.join(cmd)}")
                t0 = time.time()
                try:
                    p = subprocess.run(cmd, capture_output=True, text=True, timeout=None)
                except FileNotFoundError as e:
                    self.logger.error(f"[RECORD] rpicam-vid: {e}")
                    return False, None, None, None
                if p.returncode != 0:
                    self.logger.error(
                        f"[RECORD] rpicam-vid exit {p.returncode}\n"
                        f"stderr: {p.stderr or '(empty)'}\nstdout: {p.stdout or '(empty)'}"
                    )
                    return False, None, None, None
                time.sleep(self.post_stop_delay_seconds)
                self.logger.info(
                    f"[RECORD] rpicam-vid done: target {duration_seconds}s, wall {time.time() - t0:.1f}s"
                )
                is_valid, message = self._verify_video_file(local_path)
                if not is_valid:
                    self.logger.error(f"[RECORD] Video file verification failed: {message}")
                    return False, None, None, None
                file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
                self.logger.info(
                    f"[RECORD] Recording completed: {filename} ({file_size_mb:.1f} MB) - {message}"
                )
                self._recording_count += 1
                if upload_callback and os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                    upload_callback(local_path, s3_key, filename, start_time=ts)
                return True, local_path, s3_key, filename

            self.logger.info(
                f"[RECORD] Recording {duration_seconds}s at {self.fps}fps using PiCamera2..."
            )

            # Libav H.264 on Pi 5 defaults framerate=30; must match self.fps for correct timing/bitrate.
            enc = H264Encoder(
                bitrate=self.bitrate,
                framerate=self.fps,
                iperiod=self.fps,
                repeat=True,
            )
            queue_size = getattr(self, "ffmpeg_video_thread_queue_size", 512)
            out = FfmpegOutputLargeQueue(local_path, video_thread_queue_size=queue_size)

            recording_start_time = time.time()

            # Safe start: never reuse a failed encoder
            started = self._safe_start_recording(enc, out, local_path)
            if not started:
                self.logger.info("[RECORD] Retrying with fresh encoder after full camera reset...")
                enc = H264Encoder(
                    bitrate=self.bitrate,
                    framerate=self.fps,
                    iperiod=self.fps,
                    repeat=True,
                )
                out = FfmpegOutputLargeQueue(local_path, video_thread_queue_size=queue_size)
                started = self._safe_start_recording(enc, out, local_path)
            if not started:
                self.logger.error("[RECORD] Failed to start recording after retry")
                return False, None, None, None

            # Record for the specified duration
            try:
                time.sleep(duration_seconds)
            except KeyboardInterrupt:
                self.logger.info("[RECORD] Recording interrupted by user")
                raise

            # Safe stop with buffer release delay
            self._safe_stop_recording()
            actual_duration = time.time() - recording_start_time
            self.logger.info(f"[RECORD] Native recording completed: {filename}")
            self.logger.info(f"[RECORD] Target duration: {duration_seconds}s, Actual duration: {actual_duration:.1f}s")
            
            # Verify file integrity
            is_valid, message = self._verify_video_file(local_path)
            if not is_valid:
                self.logger.error(f"[RECORD] Video file verification failed: {message}")
                return False, None, None, None
            
            file_size_mb = os.path.getsize(local_path) / (1024 * 1024)
            self.logger.info(f"[RECORD] Recording completed: {filename} ({file_size_mb:.1f} MB) - {message}")

            self._recording_count += 1

            # Call upload callback if provided (ts = recording start time)
            if upload_callback and os.path.exists(local_path) and os.path.getsize(local_path) > 0:
                upload_callback(local_path, s3_key, filename, start_time=ts)

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

