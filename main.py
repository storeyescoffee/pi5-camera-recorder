#!/usr/bin/env python3
"""
Raspberry Pi 5 Video Recorder with Automatic AWS S3 Region Detection
and Flexible Filename Placeholders
"""

import time
import configparser
import logging
from datetime import datetime
from pathlib import Path
from logging.handlers import RotatingFileHandler

from camera_recorder import CameraRecorder
from cloud_uploader import CloudUploader


class VideoRecorder:
    """Main orchestrator class that combines camera recording and cloud upload functionality."""
    
    def __init__(self, config_file="config.conf"):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

        # Setup logging with file handler
        self._setup_logging()
        
        # Initialize camera recorder and cloud uploader
        self.camera_recorder = CameraRecorder(self.config, self.logger)
        self.cloud_uploader = CloudUploader(self.config, self.logger)
        
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
        
        try:
            while True:
                try:
                    # Check camera before recording
                    if self.camera_recorder.camera is None:
                        self.logger.warning("Camera not initialized, attempting setup...")
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
                        upload_callback=self.cloud_uploader.upload_file
                    )
                    
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
            # Cleanup camera
            self.camera_recorder.cleanup()
            # Cleanup uploads
            self.cloud_uploader.cleanup()
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
