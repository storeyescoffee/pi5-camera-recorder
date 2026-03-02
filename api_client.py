#!/usr/bin/env python3
"""
Device Gateway API Client for Raspberry Pi 5 Video Recorder
Communicates with the backend to report video upload state (POST) and upload status (PUT).
"""

import json
import logging
import uuid
from urllib import request, error


UPLOAD_STATUS = frozenset((
    "UPLOADING",
    "COMPLETED",
    "FAILED",
    "UPLOADING_FALLBACK",
    "COMPLETED_FALLBACK",
))


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


class ApiClient:
    """
    Client for the device-gw main-videos API.
    - POST: Initial video state when upload starts
    - PUT: Upload progress/result (uploadTime, uploadSpeed, attempts, uploadStatus)
    """

    def __init__(self, base_url, device_id=None, logger=None):
        """
        Initialize the API client.

        Args:
            base_url: Base URL of the device-gw API (e.g. https://api.example.com)
            device_id: Optional device ID for X-DEVICE-ID header (Pi serial if empty)
            logger: Optional logger instance (creates one if not provided)
        """
        self.base_url = base_url.rstrip("/")
        self.device_id = (device_id or "").strip() or _get_pi_serial_id()
        self.logger = logger or self._setup_logging()

    def _setup_logging(self):
        """Setup logging if not provided."""
        logger = logging.getLogger(__name__)
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
            logger.addHandler(handler)
            logger.setLevel(logging.INFO)
        return logger

    def _request(self, method, path, data=None):
        """
        Make HTTP request to the API.

        Args:
            method: HTTP method (GET, POST, PUT)
            path: API path (e.g. /device-gw/main-videos)
            data: Optional dict to send as JSON body (for POST/PUT)

        Returns:
            tuple: (success: bool, response_body: dict or None, status_code: int)
        """
        url = f"{self.base_url}{path}"
        try:
            req_data = json.dumps(data).encode("utf-8") if data and method != "GET" else None
            headers = {}
            if method != "GET":
                headers["Content-Type"] = "application/json"
            if self.device_id:
                headers["X-DEVICE-ID"] = self.device_id
            req = request.Request(
                url,
                data=req_data,
                method=method,
                headers=headers,
            )
            with request.urlopen(req, timeout=30) as resp:
                body = resp.read().decode("utf-8").strip()
                try:
                    parsed = json.loads(body) if body else {}
                except json.JSONDecodeError:
                    parsed = body  # Plain text response
                return True, parsed, resp.status
        except error.HTTPError as e:
            try:
                body = e.read().decode("utf-8").strip()
                try:
                    parsed = json.loads(body) if body else {}
                except json.JSONDecodeError:
                    parsed = body
            except Exception:
                parsed = {}
            self.logger.error(f"[API] {method} {path} failed: {e.code} {e.reason} - {parsed}")
            return False, parsed, getattr(e, "code", 0)
        except Exception as e:
            self.logger.error(f"[API] {method} {path} error: {e}", exc_info=True)
            return False, None, 0

    def post_main_video(self, video_url, filesize, duration, video_code=None):
        """
        POST initial video state when upload starts.

        Args:
            video_url: Full video URL (required, max 1024 chars)
            filesize: File size in bytes (required)
            duration: Duration in seconds (required)
            video_code: Optional UUID; if omitted, server generates one

        Returns:
            str or None: videoCode to use for subsequent PUT, or None on failure
        """
        if len(video_url) > 1024:
            self.logger.warning(f"[API] videoUrl truncated to 1024 chars")
            video_url = video_url[:1024]

        code = str(video_code) if video_code else str(uuid.uuid4())
        payload = {
            "videoCode": code,
            "videoUrl": video_url,
            "filesize": int(filesize),
            "duration": int(duration),
        }

        ok, resp, _ = self._request("POST", "/device-gw/main-videos", payload)
        if not ok:
            return None

        # Response may be JSON {"videoCode": "..."} or plain text (the videoCode itself)
        if isinstance(resp, dict):
            returned = resp.get("videoCode")
        else:
            returned = str(resp).strip() if resp else None
        result = returned if returned else code
        if result:
            self.logger.info(f"[API] POST main-video OK, videoCode={result}")
        return result

    def put_main_video(self, video_code, upload_time, upload_speed, attempts, upload_status):
        """
        PUT upload status for a main video.

        Args:
            video_code: Video code from POST (required)
            upload_time: Upload time in seconds (required)
            upload_speed: Upload speed in Mbps (required)
            attempts: Number of upload attempts (required)
            upload_status: One of UPLOADING, COMPLETED, FAILED, UPLOADING_FALLBACK, COMPLETED_FALLBACK (required)

        Returns:
            bool: True if the request succeeded
        """
        if upload_status not in UPLOAD_STATUS:
            self.logger.error(f"[API] Invalid uploadStatus: {upload_status}")
            return False

        payload = {
            "uploadTime": int(upload_time),
            "uploadSpeed": float(upload_speed),
            "attempts": int(attempts),
            "uploadStatus": upload_status,
        }

        path = f"/device-gw/main-videos/{video_code}"
        ok, _, _ = self._request("PUT", path, payload)
        if ok:
            self.logger.info(f"[API] PUT main-video OK for videoCode={video_code}")
        return ok

    def get_store_settings(self):
        """
        GET store settings from /api/device-gw/settings.

        Returns:
            dict or None: Parsed payload on success, None on failure.
            Expected structure:
            {
                "RECORDING": {"s3-location": "...", "chunk-duration": "..."},
                "REGISTER": {"delta-time": "..."},
                "CAMERA": {"shutter-speed": "...", "analog-gain": "...", "bitrate": "...", "flip": "..."}
            }
        """
        ok, resp, _ = self._request("GET", "/api/device-gw/settings")
        if not ok or not isinstance(resp, dict):
            return None
        self.logger.info(f"[API] GET store settings OK")
        return resp


def create_api_client(config, logger=None):
    """
    Create ApiClient from config if base_url is configured.

    Args:
        config: ConfigParser with optional [api] section
        logger: Optional logger

    Returns:
        ApiClient or None if api base_url is not configured
    """
    if not config.has_section("api"):
        return None
    base_url = config.get("api", "base_url", fallback="").strip()
    if not base_url:
        return None
    device_id = config.get("api", "device_id", fallback="").strip()
    if not device_id:
        device_id = _get_pi_serial_id()
    return ApiClient(base_url, device_id=device_id, logger=logger)
