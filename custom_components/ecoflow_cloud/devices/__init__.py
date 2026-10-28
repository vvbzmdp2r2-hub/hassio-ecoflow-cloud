import dataclasses
import json
import logging
from abc import ABC, abstractmethod
from collections.abc import Sequence
from typing import Any, cast, override

from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.components.button import ButtonEntity
from homeassistant.components.number import NumberEntity
from homeassistant.components.select import SelectEntity
from homeassistant.components.sensor import SensorEntity
from homeassistant.components.switch import SwitchEntity
from homeassistant.core import HomeAssistant
from homeassistant.util import dt

from custom_components.ecoflow_cloud.api import EcoflowApiClient
from custom_components.ecoflow_cloud.api.commands import MainSnCache
from custom_components.ecoflow_cloud.api.message import JSONDict, JSONMessage, Message
from custom_components.ecoflow_cloud.device_data import DeviceData
from custom_components.ecoflow_cloud.devices.data_coordinator import DeviceDataCoordinator
from custom_components.ecoflow_cloud.devices.data_holder import EcoflowDataHolder, PreparedData
from custom_components.ecoflow_cloud.devices.status_tracker import StatusTracker

_LOGGER = logging.getLogger(__name__)


@dataclasses.dataclass
class EcoflowDeviceInfo:
    public_api: bool
    sn: str
    name: str
    device_type: str
    status: int
    data_topic: str
    set_topic: str
    set_reply_topic: str
    get_topic: str | None
    get_reply_topic: str | None
    status_topic: str | None = None

    quota_all: str = "/iot-open/sign/device/quota/all"

    def topics(self) -> list[str]:
        topics: list[str | None] = [
            self.data_topic,
            self.get_topic,
            self.get_reply_topic,
            self.set_topic,
            self.set_reply_topic,
            self.status_topic,
        ]
        return [t for t in topics if t is not None]


class NoQuotaMessageError(Exception):
    pass


class BaseDevice(ABC):
    def __init__(self, device_info: EcoflowDeviceInfo, device_data: DeviceData):
        super().__init__()
        self.coordinator: DeviceDataCoordinator
        self.device_info: EcoflowDeviceInfo = device_info
        self.power_step: int = device_data.options.power_step
        self.device_data: DeviceData = device_data

        self.status_tracker = StatusTracker(
            self.device_data.options.assume_offline_sec,
            device_info.status,
        )

        module_sn = self.device_data.sn if self.device_data.parent is not None else None
        self.data = EcoflowDataHolder(
            module_sn,
            self.device_data.options.diagnostic_mode,
            status_callback=self.status_tracker,
        )

    def configure(self, hass: HomeAssistant):
        self.coordinator = DeviceDataCoordinator(hass, self.data, self.device_data.options.refresh_period)

    @staticmethod
    def default_charging_power_step() -> int:
        return 100

    def charging_power_step(self) -> int:
        if self.power_step == -1:
            return self.default_charging_power_step()
        else:
            return self.power_step

    def flat_json(self) -> bool:
        return True

    @property
    def main_sn_cache(self) -> MainSnCache | None:
        """Main SN of the system this device belongs to, for devices whose
        system commands go to that SN. None: every command uses the own SN."""
        return None

    def set_command_target_sn(self, command: Message) -> str:
        return self.device_info.sn

    def evaluates_set_replies(self) -> bool:
        """Opt-in: match set_reply to the command, roll the optimistic state back on a
        rejection or no answer. Only for devices whose reply format is documented."""
        return False

    async def async_restore_state(self):
        """Restore persisted device state on startup. Override in subclasses."""

    def extract_quota_data(self, message: JSONDict) -> PreparedData | None:
        return None

    @abstractmethod
    def sensors(self, client: EcoflowApiClient) -> Sequence[SensorEntity]:
        pass

    @abstractmethod
    def numbers(self, client: EcoflowApiClient) -> Sequence[NumberEntity]:
        pass

    @abstractmethod
    def switches(self, client: EcoflowApiClient) -> Sequence[SwitchEntity]:
        pass

    @abstractmethod
    def selects(self, client: EcoflowApiClient) -> Sequence[SelectEntity]:
        pass

    def binary_sensors(self, client: EcoflowApiClient) -> Sequence[BinarySensorEntity]:
        return []

    def buttons(self, client: EcoflowApiClient) -> Sequence[ButtonEntity]:
        return []

    def update_data(self, raw_data: bytes, data_type: str) -> bool:
        if data_type == self.device_info.data_topic:
            data = self._prepare_data_data_topic(raw_data)
            self.data.add_data(data)
        elif data_type == self.device_info.set_topic:
            data = self._prepare_data_set_topic(raw_data)
            self.data.add_set_message(data)
        elif data_type == self.device_info.set_reply_topic:
            data = self._prepare_data_set_reply_topic(raw_data)
            self.data.add_set_reply_message(data)
        elif data_type == self.device_info.get_topic:
            data = self._prepare_data_get_topic(raw_data)
            self.data.add_get_message(data)
        elif data_type == self.device_info.get_reply_topic:
            data = self._prepare_data_get_reply_topic(raw_data)
            self.data.add_get_reply_message(data)
        elif data_type == self.device_info.status_topic:
            data = self._prepare_data_status_topic(raw_data)
            self.data.add_status(data)
        else:
            return False
        return True

    def update_peer_data(self, raw_data: bytes, source: BaseDevice) -> bool:
        """Opt-in for serial-addressed telemetry from another configured device.

        Called only for that device's live data topic in the same MQTT client.
        Commands, replies and unknown topics never enter this path.
        """
        return False

    def _prepare_data_data_topic(self, raw_data: bytes) -> PreparedData:
        data = self._prepare_data(raw_data)
        return PreparedData(None, data, data)

    def _prepare_data_set_topic(self, raw_data: bytes) -> PreparedData:
        return PreparedData(None, None, self._prepare_data(raw_data))

    def _prepare_data_set_reply_topic(self, raw_data: bytes) -> PreparedData:
        return PreparedData(None, None, self._prepare_data(raw_data))

    def _prepare_data_get_topic(self, raw_data: bytes) -> PreparedData:
        return PreparedData(None, None, self._prepare_data(raw_data))

    def _prepare_data_get_reply_topic(self, raw_data: bytes) -> PreparedData:
        return PreparedData(None, None, self._prepare_data(raw_data))

    def _prepare_data_status_topic(self, raw_data: bytes) -> PreparedData:
        data = self._prepare_data(raw_data)
        if "params" in data and "status" in data["params"]:
            return PreparedData(int(data["params"]["status"]) == 1, None, data)
        return PreparedData(None, None, data)

    def _prepare_data(self, raw_data: bytes) -> dict[str, Any]:
        payload = raw_data.decode("utf-8", errors="ignore")
        try:
            return json.loads(payload)
        except ValueError as error:
            if payload.lstrip().startswith("{"):
                _LOGGER.warning("Ignoring a malformed JSON message for %s: %s", self.device_info.sn, error)
            else:
                # binary (protobuf) frames reach JSON device classes too, e.g. on
                # the set/get topics; one line each at WARNING and ERROR flooded
                # the log (tolwi#210, #872)
                _LOGGER.debug("Ignoring a non-JSON message of %d bytes for %s", len(raw_data), self.device_info.sn)
            return {}


class BaseInternalDevice(BaseDevice):
    def __init__(self, device_info: EcoflowDeviceInfo, device_data: DeviceData):
        super().__init__(device_info, device_data)

    @override
    def _prepare_data_get_reply_topic(self, raw_data: bytes) -> PreparedData:
        data = self._prepare_data(raw_data)
        if "operateType" in data and data["operateType"] == "latestQuotas":
            message_data = data["data"]
            assert isinstance(message_data, dict)

            online = int(cast(bool | int | str, message_data["online"]))
            if online == 1:
                return PreparedData(online == 1, {"params": message_data["quotaMap"], "time": dt.utcnow()}, data)
            else:
                return PreparedData(False, None, data)
        return PreparedData(None, None, data)

    def get_quota_message(self) -> Message:
        return JSONMessage(
            {
                "version": "1.1",
                "moduleType": 0,
                "operateType": "latestQuotas",
                "params": {},
            }
        )


class DiagnosticDevice(BaseDevice):
    def sensors(self, client: EcoflowApiClient) -> Sequence[SensorEntity]:
        return []

    def numbers(self, client: EcoflowApiClient) -> Sequence[NumberEntity]:
        return []

    def switches(self, client: EcoflowApiClient) -> Sequence[SwitchEntity]:
        return []

    def buttons(self, client: EcoflowApiClient) -> Sequence[ButtonEntity]:
        return []

    def selects(self, client: EcoflowApiClient) -> Sequence[SelectEntity]:
        return []
