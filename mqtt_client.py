#!/usr/bin/env python3
"""
MQTT client for subscribing to storeyes/<device-id>/sync-settings.
When a message arrives, triggers a callback to refetch and apply settings.
"""

import logging
import threading

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None


SYNC_SETTINGS_TOPIC_TEMPLATE = "storeyes/{device_id}/sync-settings"


class MqttSyncClient:
    """Subscribe to sync-settings topic and invoke callback on message."""

    def __init__(self, host, port, username, password, device_id, on_sync_callback, logger=None):
        """
        Args:
            host: MQTT broker host (e.g. mqtt.storeyes.io)
            port: Broker port (e.g. 1883)
            username: MQTT username
            password: MQTT password
            device_id: Device identifier for topic storeyes/<device_id>/sync-settings
            on_sync_callback: Callable with no args, invoked when sync message received
            logger: Optional logger
        """
        self.host = host
        self.port = int(port)
        self.username = username
        self.password = password
        self.device_id = device_id
        self.on_sync_callback = on_sync_callback
        self.logger = logger or logging.getLogger(__name__)
        self._client = None
        self._thread = None
        self._stop_event = threading.Event()

    def start(self):
        """Start MQTT client in a background thread."""
        if mqtt is None:
            self.logger.warning("[MQTT] paho-mqtt not installed, sync-settings disabled")
            return False

        self._client = mqtt.Client(client_id=f"storeyes-recorder-{self.device_id}")
        self._client.username_pw_set(self.username, self.password)
        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message
        self._client.on_disconnect = self._on_disconnect

        try:
            self._client.connect(self.host, self.port, keepalive=60)
            self._stop_event.clear()
            self._thread = threading.Thread(target=self._loop, daemon=True)
            self._thread.start()
            self.logger.info(f"[MQTT] Connected to {self.host}:{self.port}, subscribing to sync-settings")
            return True
        except Exception as e:
            self.logger.error(f"[MQTT] Failed to connect: {e}")
            return False

    def _on_connect(self, client, userdata, flags, reason_code, *args):
        if reason_code != 0:
            self.logger.error(f"[MQTT] Connection failed: {reason_code}")
            return
        topic = SYNC_SETTINGS_TOPIC_TEMPLATE.format(device_id=self.device_id)
        client.subscribe(topic)
        self.logger.info(f"[MQTT] Subscribed to {topic}")

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        self.logger.info(f"[MQTT] sync-settings message received on {topic}")
        try:
            self.on_sync_callback()
        except Exception as e:
            self.logger.error(f"[MQTT] sync callback error: {e}", exc_info=True)

    def _on_disconnect(self, client, userdata, reason_code, *args):
        if reason_code != 0:
            self.logger.warning(f"[MQTT] Disconnected: {reason_code}")

    def _loop(self):
        """Run MQTT loop with periodic stop check."""
        while not self._stop_event.is_set():
            try:
                self._client.loop(timeout=1.0)
            except Exception as e:
                if not self._stop_event.is_set():
                    self.logger.warning(f"[MQTT] Loop error: {e}")
                break

    def stop(self):
        """Stop MQTT client."""
        self._stop_event.set()
        if self._client:
            try:
                self._client.disconnect()
                self._client.loop_stop()
            except Exception:
                pass
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=3)
        self.logger.info("[MQTT] Stopped")
