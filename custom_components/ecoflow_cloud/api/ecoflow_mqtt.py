from __future__ import annotations

import logging
import ssl
import threading
import time
from collections.abc import Callable
from typing import Any

from homeassistant.core import callback
from paho.mqtt.client import Client, ConnectFlags, DisconnectFlags, MQTTMessage, PayloadType
from paho.mqtt.enums import CallbackAPIVersion
from paho.mqtt.properties import Properties
from paho.mqtt.reasoncodes import ReasonCode

from ..devices import BaseDevice
from . import EcoflowMqttInfo

_LOGGER = logging.getLogger(__name__)

# paho maps the v3.1.1 CONNACK codes 4 and 5 onto these names; "Banned" is MQTT 5 only
AUTH_FAILURE_REASONS = frozenset({"Bad user name or password", "Not authorized", "Banned"})

# paho keeps every QoS 1 message the broker has not acknowledged and sends it
# again after the next successful connect, however late that is. A command
# older than this is dropped instead: by then the user may have changed the
# setting elsewhere, and a late replay looks like a write nobody made
# (tolwi#819, #922).
STALE_COMMAND_SEC = 60


class EcoflowMQTTClient:
    def __init__(self, mqtt_info: EcoflowMqttInfo, devices: dict[str, BaseDevice]):
        self.connected = False
        self.auth_failed = False
        self.on_auth_failure: Callable[[], None] | None = None
        self.on_set_reply: Callable[[bytes], None] | None = None
        self.on_message_processed: Callable[[], None] | None = None
        self.__mqtt_info = mqtt_info
        self.__devices: dict[str, BaseDevice] = devices
        # topics outside the devices' own, e.g. the set_reply of a STREAM system's main unit
        self.__extra_topics: set[str] = set()
        # mid -> (monotonic publish time, topic) of messages waiting for PUBACK
        self.__unacked: dict[int, tuple[float, str]] = {}
        self.__unacked_lock = threading.Lock()

        # A plain paho client, not HA's AsyncMQTTClient: that one swaps paho's
        # locks for no-ops because HA drives it from the event loop only. This
        # client runs paho's own network thread (loop_start) and publishes from
        # the event loop and executor threads, so it needs the locks.
        self.__client = Client(
            client_id=self.__mqtt_info.client_id,
            reconnect_on_failure=True,
            clean_session=True,
            callback_api_version=CallbackAPIVersion.VERSION2,
        )

        # self.__client._connect_timeout = 15.0
        self.__client.username_pw_set(self.__mqtt_info.username, self.__mqtt_info.password)
        self.__client.tls_set(certfile=None, keyfile=None, cert_reqs=ssl.CERT_REQUIRED)
        self.__client.tls_insecure_set(False)
        self.__client.on_connect = self._on_connect
        self.__client.on_disconnect = self._on_disconnect
        self.__client.on_message = self._on_message
        self.__client.on_publish = self._on_publish
        self.__client.on_socket_close = self._on_socket_close

        _LOGGER.info(
            f"Connecting to MQTT Broker {self.__mqtt_info.url}:{self.__mqtt_info.port} with client id {self.__mqtt_info.client_id} and username {self.__mqtt_info.username}"
        )
        try:
            self.__client.connect(self.__mqtt_info.url, self.__mqtt_info.port, keepalive=15)
        except OSError as error:
            # TimeoutError, but also a DNS failure (socket.gaierror) or a refused
            # connection while the network is not up yet (tolwi#877, #206).
            # paho's network thread retries the first connect itself.
            _LOGGER.warning(
                "Initial EcoFlow MQTT connection to %s:%s failed (%s), retrying in the background",
                self.__mqtt_info.url,
                self.__mqtt_info.port,
                error,
            )
        self.__client.loop_start()

    def add_topic(self, topic: str) -> None:
        if topic in self.__extra_topics or topic in self.__target_topics():
            return
        self.__extra_topics.add(topic)
        if self.__client.is_connected():
            self.__client.subscribe([(topic, 1)])
            _LOGGER.info(f"Subscribed to MQTT topic {topic}")

    def is_connected(self):
        return self.__client.is_connected()

    def reconnect(self) -> bool:
        try:
            _LOGGER.info(f"Re-connecting to MQTT Broker {self.__mqtt_info.url}:{self.__mqtt_info.port}")
            self.__client.loop_stop()
            self.__client.reconnect()
            self.__client.loop_start()
            return True
        except Exception as e:
            _LOGGER.error(e)
            return False

    @callback
    def _on_socket_close(self, client: Client, userdata: Any, sock: Any) -> None:
        _LOGGER.info(f"MQTT Socket disconnection : {str(sock)}")

    @callback
    def _on_connect(
        self, client: Client, userdata: Any, flags: ConnectFlags, rc: ReasonCode, properties: Properties | None = None
    ):
        if rc == 0:
            self.connected = True
            self.auth_failed = False
            # paho resends its unacknowledged messages right after this callback
            self._drop_stale_messages()
            target_topics = [(topic, 1) for topic in self.__target_topics()]
            if target_topics:
                self.__client.subscribe(target_topics)
                _LOGGER.info(f"Subscribed to MQTT topics {target_topics}")
        elif rc.getName() in AUTH_FAILURE_REASONS:
            self.__handle_auth_failure(client, userdata, rc)
        else:
            self.__log_with_reason("connect", client, userdata, rc)

    @callback
    def _on_disconnect(
        self,
        client: Client,
        userdata: Any,
        disconnect_flags: DisconnectFlags,
        reason_code: ReasonCode,
        properties: Properties | None,
    ) -> None:
        if not self.connected:
            # from homeassistant/components/mqtt/client.py
            # This function is re-entrant and may be called multiple times
            # when there is a broken pipe error.
            return
        self.connected = False
        if reason_code.is_failure:
            self.__log_with_reason("disconnect", client, userdata, reason_code)

    @callback
    def _on_publish(self, client: Client, userdata: Any, mid: int, reason_code: ReasonCode, properties: Properties):
        with self.__unacked_lock:
            self.__unacked.pop(mid, None)

    def _drop_stale_messages(self) -> None:
        out_messages = getattr(self.__client, "_out_messages", None)
        out_mutex = getattr(self.__client, "_out_message_mutex", None)
        if out_messages is None or out_mutex is None:
            return
        now = time.monotonic()
        with out_mutex, self.__unacked_lock:
            for mid, (sent_at, topic) in list(self.__unacked.items()):
                if mid not in out_messages:
                    del self.__unacked[mid]
                elif now - sent_at >= STALE_COMMAND_SEC:
                    del out_messages[mid]
                    del self.__unacked[mid]
                    parts = topic.split("/")
                    _LOGGER.warning(
                        "Not resending a message for %s from %d s ago that the broker never acknowledged",
                        parts[3] if len(parts) > 3 else "?",
                        int(now - sent_at),
                    )

    @callback
    def _on_message(self, client, userdata, message: MQTTMessage):
        try:
            # Restrict peer routing to a known live telemetry topic in this entry.
            # Retained messages must not make another device look freshly received.
            source = None if getattr(message, "retain", False) else next(
                (d for d in self.__devices.values() if message.topic == d.device_info.data_topic), None
            )
            for sn, device in self.__devices.items():
                if device.update_data(message.payload, message.topic):
                    _LOGGER.debug(f"Message for {sn} and Topic {message.topic} : {message.payload.hex()}")
                elif source is not None and device is not source:
                    device.update_peer_data(message.payload, source)
        except UnicodeDecodeError as error:
            _LOGGER.error(f"UnicodeDecodeError: {error}. Ignoring message and waiting for the next one.")
        except Exception:
            _LOGGER.error("Unexpected error processing MQTT message on topic %s", message.topic, exc_info=True)

        try:
            if self.on_set_reply is not None and message.topic.endswith("set_reply"):
                self.on_set_reply(message.payload)
            if self.on_message_processed is not None:
                self.on_message_processed()
        except Exception:
            _LOGGER.error("Unexpected error evaluating a set reply on topic %s", message.topic, exc_info=True)

    def stop(self):
        self.__client.unsubscribe(self.__target_topics())
        self.__client.loop_stop()
        self.__client.disconnect()

    def __handle_auth_failure(self, client, userdata, reason_code: ReasonCode) -> None:
        if self.auth_failed:
            return
        self.auth_failed = True
        # paho would otherwise retry the rejected credentials forever
        self.__client.loop_stop()
        self.__log_with_reason("connect", client, userdata, reason_code)
        if self.on_auth_failure is not None:
            self.on_auth_failure()

    def __log_with_reason(self, action: str, client, userdata, reason_code: ReasonCode):
        _LOGGER.error(f"MQTT {action}: {reason_code.getName()} ({self.__mqtt_info.client_id}) - {userdata}")

    def publish(self, topic: str, message: PayloadType) -> None:
        try:
            info = self.__client.publish(topic, message, 1)
            # Recorded after publish (paho's out-message lock is always taken
            # before ours). A PUBACK that beats this line leaves an entry whose
            # mid paho no longer holds; _drop_stale_messages() discards those.
            with self.__unacked_lock:
                self.__unacked[info.mid] = (time.monotonic(), topic)
            _LOGGER.debug("Published mid %s, rc %s", info.mid, info.rc)
        except RuntimeError as error:
            _LOGGER.error("Error on topic %s and message %s: %s", topic, message, error)
        except Exception as error:
            _LOGGER.debug("Error on topic %s and message %s: %s", topic, message, error)

    def __target_topics(self) -> list[str]:
        topics = []
        for device in self.__devices.values():
            for topic in device.device_info.topics():
                topics.append(topic)
        topics.extend(self.__extra_topics)
        # Remove duplicates that can occur when multiple devices have the same topic (for example sub devices)
        return list(set(topics))
