"""EcoFlow STREAM AC 5000 (SN prefix ``ES22``) and STREAM 5000 (``ES21``) -- internal/App API.

The STREAM 5000 is the same family with MPPT inputs: same telemetry, plus PV
total and per-string power in block 50.1. PV entities are only offered for it.

Transport: MQTT topic ``/app/device/property/<SN>``, envelope protobuf
``StreamACSendHeaderMsg`` from ``proto/stream_ac_pb2.py`` -- the same envelope
every Stream device uses. Unlike the STREAM Microinverter, ``msg.pdata`` is
**not** XOR-obfuscated; it parses as plain protobuf.

Not a ``STREAM_AC`` alias: the AC 5000's telemetry (``cmd_id`` 39/40) sends
small nested blocks keyed by serial number, where ``StreamACChamp_cmd21_3`` is
flat with field numbers in the 500-1200 range. Only the ``cmd_id`` 50 pack frame
resembles the existing schema, so configuring one as ``STREAM_AC`` yields a
handful of SoC values and little else. This decoder dispatches on ``cmd_id``
rather than blind-parsing every schema, so a frame is either understood or
skipped, never mislabelled.

Field meanings were decoded from MQTT captures anchored to app readings; the
commit history carries the evidence for each. Blocks 60/61/62 (``cmd_id`` 40)
are deliberately left undecoded.

Blocks 50.1 and 54.1 carry one entry per linked unit, stamped with its serial.
On a linked pair the master's frames carry both units' entries, so each entry
is read on its own and only the one with this device's serial is used.
"""

import logging
from typing import Any, Iterable, Iterator, override

from google.protobuf.json_format import MessageToDict
from google.protobuf.message import Message as ProtoMessageRaw
from homeassistant.components.binary_sensor import BinarySensorEntity
from homeassistant.components.number import NumberEntity
from homeassistant.components.select import SelectEntity
from homeassistant.components.sensor import SensorEntity
from homeassistant.components.switch import SwitchEntity
from homeassistant.util import utcnow

from custom_components.ecoflow_cloud.api import EcoflowApiClient
from custom_components.ecoflow_cloud.api.message import Message, PrivateAPIMessageProtocol
from custom_components.ecoflow_cloud.binary_sensor import MiscBinarySensorEntity
from custom_components.ecoflow_cloud.devices import BaseDevice, BaseInternalDevice, const
from custom_components.ecoflow_cloud.devices.data_holder import PreparedData
from custom_components.ecoflow_cloud.devices.internal.proto import stream_ac_5000_pb2, stream_ac_pb2
from custom_components.ecoflow_cloud.number import ChargingPowerEntity, MaxBatteryLevelEntity, MinBatteryLevelEntity
from custom_components.ecoflow_cloud.select import PowerDictSelectEntity
from custom_components.ecoflow_cloud.sensor import (
    CumulativeCapacitySensorEntity,
    EnergySensorEntity,
    FrequencySensorEntity,
    LevelSensorEntity,
    MilliVoltSensorEntity,
    RemainSensorEntity,
    StateOfHealthSensorEntity,
    StatusSensorEntity,
    StoredEnergyFromSocSensorEntity,
    VoltSensorEntity,
    WattsSensorEntity,
)
from custom_components.ecoflow_cloud.switch import EnabledEntity

_LOGGER = logging.getLogger(__name__)

# cmd_id -> payload message. cmd_id 40 (blocks 60/61/62) is intentionally absent.
_CMD_ID_STATUS = 2
_CMD_ID_RUNTIME = 39
_CMD_ID_PACK = 50

# Wire tag for field 1, wire type 2 -- every EcoPacket starts with it.
_ECOPACKET_TAG = 0x0A

# Property ids, as sent in pdata field 1 of a set command.
_PROP_POWER_LIMITS = 10
_PROP_AC_OUT = 19
_PROP_DEVICE_CFG = 23
_PROP_WORK_MODE = 25
_PROP_SOC_LIMITS = 29

# Envelope constants, copied verbatim from the app's own writes.
_SET_CMD_FUNC = 254
_SET_CMD_ID = 38
_OUT_LIMIT_FIELD4 = 4
_OUT_LIMIT_MAX = 800

# Entity keys for controls whose protobuf field name differs.
_KEY_AC_OUT = "acOutEnabled"
_KEY_XBOOST = "xboostEnabled"
_KEY_WORK_MODE = "workMode"

# Raw pack SoC. Reads above the app-facing system scale near empty, so it is kept
# on its own key rather than fighting it.
_KEY_PACK_SOC = "packSoc"

# On a P1 outage the device still sends meter block 16, but empty -- which would
# otherwise leave every meter sensor holding a stale reading.
_KEY_METER_LINK = "meterLinkUp"

# Device types / serial prefix of the family member with PV inputs (STREAM 5000).
_PV_DEVICE_TYPES = {"STREAM_5000"}
_PV_SN_PREFIX = "ES21"

# Block 50.1 PV fields; protobuf field name doubles as the parameter key.
_KEY_PV_TOTAL = "pvPower"
_PV_STRING_KEYS = ("pv1Power", "pv2Power", "pv3Power", "pv4Power")
_PV_KEYS = (_KEY_PV_TOTAL, *_PV_STRING_KEYS)
# How far the strings present may sum away from the total before an absent
# string stops counting as idle. The unit's own entry matches exactly; a copy
# relayed by a linked unit is rounded by 1-2 W and has been seen dropping a
# string that was producing ~22 W.
_PV_TOTAL_SLACK_W = 2.5

# Block 50.1 power fields that read 0 W when an entry arrives without them.
_KEY_BATTERY_NET_POWER = "batteryNetPower"
_KEY_AC_GRID_POWER = "acGridPower"


class StreamAC5000CommandMessage(PrivateAPIMessageProtocol):
    """One property write, in the envelope the EcoFlow app uses."""

    def __init__(self, device_sn: str, payload: ProtoMessageRaw):
        self._payload = payload
        self._packet = stream_ac_5000_pb2.StreamAC5000SendCommandMsg()
        message = self._packet.msg

        pdata = payload.SerializeToString()
        message.pdata = pdata
        message.data_len = len(pdata)

        message.src = 32
        message.dest = 2
        message.d_src = 1
        message.d_dest = 1
        message.check_type = 3
        message.need_ack = 1
        message.version = 4
        message.seq = Message.gen_seq()
        message.cmd_func = _SET_CMD_FUNC
        message.cmd_id = _SET_CMD_ID
        message.from_ = "HomeAssistant"
        # The app repeats the serial in all three of these.
        message.device_sn = device_sn
        message.device_sn_2 = device_sn
        message.device_sn_3 = device_sn

    @override
    def to_mqtt_payload(self) -> Any:
        return self._packet.SerializeToString()

    @override
    def to_dict(self) -> dict:
        result = MessageToDict(self._packet, preserving_proto_field_name=True)
        result["msg"]["pdata"] = {
            type(self._payload).__name__: MessageToDict(self._payload, preserving_proto_field_name=True)
        }
        result["msg"].pop("seq", None)
        return {type(self._packet).__name__: result}


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    shift = 0
    result = 0
    while True:
        if pos >= len(buf):
            raise ValueError("truncated varint")
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        shift += 7
        if not byte & 0x80:
            return result, pos
        if shift > 63:
            raise ValueError("varint too long")


def _own_entry(entries: Iterable[Any], sn: str) -> Any | None:
    """Return the per-unit entry stamped with ``sn``, or None.

    Never falls back to another unit's entry: on a linked pair that would show
    one unit's readings on the other.
    """
    wanted = sn.strip().upper()
    for entry in entries:
        if entry.sn.strip().upper() == wanted:
            return entry
    return None


def _pv_values(entry: Any) -> dict[str, float]:
    """PV total and string power of one block 50.1 entry.

    An idle string is omitted from the wire, so an absent string reads 0 W only
    while the strings present add up to the total. Otherwise a producing string
    was left out of this frame and its key stays out, keeping the last reading.
    An entry with neither total nor strings is the night shape: all 0 W.
    """
    values = {key: float(getattr(entry, key)) for key in _PV_KEYS if entry.HasField(key)}
    if not values:
        return dict.fromkeys(_PV_KEYS, 0.0)
    total = values.get(_KEY_PV_TOTAL)
    if total is None:
        return values
    strings = sum(value for key, value in values.items() if key != _KEY_PV_TOTAL)
    if abs(total - strings) <= _PV_TOTAL_SLACK_W:
        for key in _PV_STRING_KEYS:
            values.setdefault(key, 0.0)
    return values


def _iter_packets(payload: bytes) -> Iterator[Any]:
    """Split a payload into EcoPackets and yield each parsed header.

    One MQTT message can carry several concatenated packets, and handing the
    whole buffer to ``ParseFromString`` would merge them into one instead.
    """
    offset = 0
    while offset < len(payload):
        try:
            tag, pos = _read_varint(payload, offset)
            if tag != _ECOPACKET_TAG:
                return
            length, pos = _read_varint(payload, pos)
            end = pos + length
            if end > len(payload):
                return
            packet = stream_ac_pb2.StreamACSendHeaderMsg()
            packet.ParseFromString(payload[offset:end])
        except Exception as error:
            _LOGGER.debug("Stopped splitting payload at offset %d: %s", offset, error)
            return
        yield packet.msg
        offset = end


class StreamAC5000(BaseInternalDevice):
    @override
    def update_peer_data(self, raw_data: bytes, source: BaseDevice) -> bool:
        """Accept only this unit's tagged runtime entries from a STREAM peer.

        A master's untagged meter, configuration, BMS and pack data cannot be
        attributed to another unit. Do not run the full decoder on its behalf.
        Receipt time is HA reception, not proof of a new physical measurement.
        """
        if not isinstance(source, StreamAC5000) or source is self:
            return False
        params: dict[str, Any] = {}
        wanted = self.device_info.sn.strip().upper()
        for header in _iter_packets(raw_data):
            if header.cmd_func != 254 or header.cmd_id != _CMD_ID_RUNTIME:
                continue
            try:
                runtime = stream_ac_5000_pb2.StreamAC5000Runtime()
                runtime.ParseFromString(header.pdata)
            except Exception:
                _LOGGER.debug("Ignoring malformed STREAM peer runtime frame")
                continue
            stats = [entry for entry in runtime.statPack.stat if entry.sn.strip().upper() == wanted]
            powers = [entry for entry in runtime.powerPack.power if entry.sn.strip().upper() == wanted]
            # Duplicate identities inside a frame are ambiguous, not a fallback.
            if len(stats) == 1:
                self._copy(stats[0], params, "soc")
            if len(powers) == 1:
                self._decode_unit_power(powers[0], params)
        if not params:
            return False
        data = {"params": params, "timestamp": utcnow()}
        self.data.add_data(PreparedData(None, data, data))
        return True

    def _has_pv_inputs(self) -> bool:
        sn = self.device_info.sn.strip().upper()
        return self.device_data.device_type in _PV_DEVICE_TYPES or sn.startswith(_PV_SN_PREFIX)

    def _pv_sensors(self, client: EcoflowApiClient) -> list[SensorEntity]:
        if not self._has_pv_inputs():
            # The STREAM AC 5000 has no MPPT, so these would never read anything.
            return []
        return [
            WattsSensorEntity(client, self, _KEY_PV_TOTAL, const.STREAM_POWER_PV_SUM),
            WattsSensorEntity(client, self, "pv1Power", const.STREAM_POWER_PV_1),
            WattsSensorEntity(client, self, "pv2Power", const.STREAM_POWER_PV_2),
            WattsSensorEntity(client, self, "pv3Power", const.STREAM_POWER_PV_3),
            WattsSensorEntity(client, self, "pv4Power", const.STREAM_POWER_PV_4),
        ]

    def sensors(self, client: EcoflowApiClient) -> list[SensorEntity]:
        return [
            # --- battery ------------------------------------------------
            LevelSensorEntity(client, self, "soc", const.STREAM_BATTERY_LEVEL),
            LevelSensorEntity(client, self, "f32ShowSoc", const.STREAM_BATTERY_LEVEL_PRECISE, False),
            LevelSensorEntity(client, self, "bmsBattSoc", const.STREAM_AC5000_BMS_BATTERY_LEVEL, False),
            LevelSensorEntity(client, self, _KEY_PACK_SOC, const.STREAM_AC5000_PACK_BATTERY_LEVEL, False),
            StoredEnergyFromSocSensorEntity(
                client, self, "cmsBattFullEnergy", "f32ShowSoc", const.STREAM_AC5000_STORED_ENERGY
            ),
            # cmsMaxChgSoc / cmsMinDsgSoc are writable, so they live in numbers().
            # RemainSensorEntity clamps the device's 5939 "unknown" sentinel to 0.
            RemainSensorEntity(client, self, "remainTime", const.REMAINING_TIME),
            RemainSensorEntity(client, self, "bmsChgRemTime", const.CHARGE_REMAINING_TIME, False),
            RemainSensorEntity(client, self, "bmsDsgRemTime", const.DISCHARGE_REMAINING_TIME, False),
            # --- power --------------------------------------------------
            WattsSensorEntity(client, self, _KEY_BATTERY_NET_POWER, const.STREAM_AC5000_BATTERY_NET_POWER),
            WattsSensorEntity(client, self, _KEY_AC_GRID_POWER, const.STREAM_AC5000_AC_GRID_POWER),
            # Unsigned magnitude from the CMS/BMS; Battery Net Power carries the sign.
            WattsSensorEntity(client, self, "bpPower", const.STREAM_AC5000_BATTERY_POWER),
            *self._pv_sensors(client),
            WattsSensorEntity(client, self, "maxChgPow", const.STREAM_AC5000_CHARGE_POWER_LIMIT, False),
            WattsSensorEntity(client, self, "maxDsgPow", const.STREAM_AC5000_MAX_DISCHARGE_POWER, False),
            # feedGridModePowLimit is writable and lives in numbers().
            WattsSensorEntity(client, self, "feedGridModePowMax", const.STREAM_FEED_GRID_MODE_POW_MAX, False),
            # --- paired P1 meter ----------------------------------------
            # The meter has no MQTT topic of its own; it reports inside this
            # device's stream and must not be added as a separate device.
            WattsSensorEntity(client, self, "meterTotalPower", const.SMART_METER_POWER_GLOBAL),
            WattsSensorEntity(client, self, "meterPhaseAPower", const.SMART_METER_POWER_L1),
            WattsSensorEntity(client, self, "meterPhaseBPower", const.SMART_METER_POWER_L2),
            WattsSensorEntity(client, self, "meterPhaseCPower", const.SMART_METER_POWER_L3),
            VoltSensorEntity(client, self, "meterPhaseAVol", const.SMART_METER_VOLT_L1, False),
            VoltSensorEntity(client, self, "meterPhaseBVol", const.SMART_METER_VOLT_L2, False),
            VoltSensorEntity(client, self, "meterPhaseCVol", const.SMART_METER_VOLT_L3, False),
            FrequencySensorEntity(client, self, "meterFreq", const.POWER_GRID_FREQUENCY, False),
            # --- pack health --------------------------------------------
            MilliVoltSensorEntity(client, self, "minCellVol", const.MIN_CELL_VOLT, False),
            MilliVoltSensorEntity(client, self, "maxCellVol", const.MAX_CELL_VOLT, False),
            StateOfHealthSensorEntity(client, self, "realSoh", const.REAL_SOH, False),
            StateOfHealthSensorEntity(client, self, "cycleSoh", const.SOH, False),
            # --- lifetime counters (Energy dashboard) -------------------
            CumulativeCapacitySensorEntity(client, self, "accuChgCap", const.ACCU_CHARGE_CAP, False),
            CumulativeCapacitySensorEntity(client, self, "accuDsgCap", const.ACCU_DISCHARGE_CAP, False),
            EnergySensorEntity(client, self, "accuChgEnergy", const.ACCU_CHARGE_ENERGY),
            EnergySensorEntity(client, self, "accuDsgEnergy", const.ACCU_DISCHARGE_ENERGY),
            StatusSensorEntity(client, self),
        ]

    def _command(self, **fields: Any) -> StreamAC5000CommandMessage:
        return StreamAC5000CommandMessage(
            device_sn=self.device_info.sn,
            payload=stream_ac_5000_pb2.StreamAC5000SetCommand(**fields),
        )

    def _soc_limits_command(self, max_chg: int, min_dsg: int) -> StreamAC5000CommandMessage:
        """Write both SoC limits at once, as the app does."""
        return self._command(
            propertyId=_PROP_SOC_LIMITS,
            socLimits=stream_ac_5000_pb2.StreamAC5000SetSocLimits(
                cmsMaxChgSoc=max_chg,
                cmsMinDsgSoc=min_dsg,
            ),
        )

    def binary_sensors(self, client: EcoflowApiClient) -> list[BinarySensorEntity]:
        return [
            MiscBinarySensorEntity(client, self, _KEY_METER_LINK, const.STREAM_AC5000_METER_LINK),
        ]

    def numbers(self, client: EcoflowApiClient) -> list[NumberEntity]:
        return [
            MaxBatteryLevelEntity(
                client,
                self,
                "cmsMaxChgSoc",
                const.MAX_CHARGE_LEVEL,
                50,
                100,
                lambda value, params: self._soc_limits_command(value, int(params.get("cmsMinDsgSoc", 5))),
            ),
            MinBatteryLevelEntity(
                client,
                self,
                "cmsMinDsgSoc",
                const.MIN_DISCHARGE_LEVEL,
                0,
                30,
                lambda value, params: self._soc_limits_command(int(params.get("cmsMaxChgSoc", 100)), value),
            ),
            # Property 10 accepts partial writes, so each slider sends only its
            # own field. 2500 W is the most an owner can unlock without an
            # installer; nothing in the telemetry reports the approved ceiling,
            # so the bound is static and compliance stays the owner's business.
            ChargingPowerEntity(
                client,
                self,
                "feedGridModePowLimit",
                const.STREAM_AC5000_NET_POWER_OUT,
                0,
                2500,
                lambda value: self._command(
                    propertyId=_PROP_POWER_LIMITS,
                    powerLimits=stream_ac_5000_pb2.StreamAC5000SetPowerLimits(
                        feedGridModePowLimit=value,
                        outLimitField4=_OUT_LIMIT_FIELD4,
                        outLimitMax=_OUT_LIMIT_MAX,
                    ),
                ),
            ),
            # The app allows 7200 W but the hardware does 2500 W, or 3000 W with a
            # second battery, so the slider stops where the device can deliver.
            ChargingPowerEntity(
                client,
                self,
                "chgPowLimit",
                const.STREAM_AC5000_NET_POWER_IN,
                0,
                3000,
                lambda value: self._command(
                    propertyId=_PROP_POWER_LIMITS,
                    powerLimits=stream_ac_5000_pb2.StreamAC5000SetPowerLimits(chgPowLimit=value),
                ),
            ),
        ]

    def switches(self, client: EcoflowApiClient) -> list[SwitchEntity]:
        return [
            EnabledEntity(
                client,
                self,
                _KEY_AC_OUT,
                const.STREAM_AC5000_AC_OUTPUT,
                lambda value: self._command(
                    propertyId=_PROP_AC_OUT,
                    acOut=stream_ac_5000_pb2.StreamAC5000SetAcOut(enabled=value),
                ),
            ),
            EnabledEntity(
                client,
                self,
                _KEY_XBOOST,
                const.STREAM_AC5000_XBOOST,
                lambda value: self._command(
                    propertyId=_PROP_DEVICE_CFG,
                    deviceCfg=stream_ac_5000_pb2.StreamAC5000SetDeviceCfg(xboostEnabled=value),
                ),
            ),
            EnabledEntity(
                client,
                self,
                "upsEnabled",
                const.STREAM_AC5000_UPS,
                lambda value: self._command(
                    propertyId=_PROP_DEVICE_CFG,
                    deviceCfg=stream_ac_5000_pb2.StreamAC5000SetDeviceCfg(upsEnabled=value),
                ),
            ),
        ]

    def selects(self, client: EcoflowApiClient) -> list[SelectEntity]:
        return [
            PowerDictSelectEntity(
                client,
                self,
                _KEY_WORK_MODE,
                const.STREAM_AC5000_WORK_MODE,
                const.STREAM_AC5000_WORK_MODE_OPTIONS,
                lambda value: self._command(propertyId=_PROP_WORK_MODE, workMode=value),
            ),
        ]

    @override
    def _prepare_data(self, raw_data: bytes) -> dict[str, Any]:
        params: dict[str, Any] = {}

        for header in _iter_packets(raw_data):
            pdata = getattr(header, "pdata", b"")
            if not pdata:
                continue
            try:
                self._decode_pdata(header.cmd_id, pdata, params)
            except Exception as error:
                _LOGGER.debug(
                    "Failed to decode cmd_id %s pdata %s: %s",
                    header.cmd_id,
                    pdata.hex(),
                    error,
                )

        raw: dict[str, Any] = {"params": params}
        if params:
            raw["timestamp"] = utcnow()
        return raw

    def _decode_pdata(self, cmd_id: int, pdata: bytes, params: dict[str, Any]) -> None:
        if cmd_id == _CMD_ID_STATUS:
            status = stream_ac_5000_pb2.StreamAC5000StatusPack()
            status.ParseFromString(pdata)
            # This frame's SoC is the pack scale, not the app-facing one.
            self._copy(status.battery, params, "cmsMaxChgSoc", "bmsChgRemTime", "bmsDsgRemTime")
            self._copy(status.battery, params, ("f32ShowSoc", _KEY_PACK_SOC), "cmsMinDsgSoc")

        elif cmd_id == _CMD_ID_RUNTIME:
            runtime = stream_ac_5000_pb2.StreamAC5000Runtime()
            runtime.ParseFromString(pdata)
            self._decode_runtime(runtime, params)

        elif cmd_id == _CMD_ID_PACK:
            pack = stream_ac_5000_pb2.StreamAC5000Pack()
            pack.ParseFromString(pdata)
            self._copy(
                pack,
                params,
                ("f32ShowSoc", _KEY_PACK_SOC),
                "realSoh",
                "cycleSoh",
                "calendarSoh",
                "accuChgCap",
                "accuDsgCap",
                "accuChgEnergy",
                "accuDsgEnergy",
            )

    def _decode_runtime(self, message: Any, params: dict[str, Any]) -> None:
        if message.HasField("meter"):
            # An empty block means the meter is configured but not responding.
            params[_KEY_METER_LINK] = 1 if message.meter.ByteSize() > 0 else 0
        self._copy(
            message.meter,
            params,
            "meterPhaseAPower",
            "meterPhaseBPower",
            "meterPhaseCPower",
            "meterPhaseAVol",
            "meterPhaseBVol",
            "meterPhaseCVol",
            "meterFreq",
            "meterTotalPower",
        )
        self._copy(
            message.sysConfig,
            params,
            "maxChgPow",
            "maxDsgPow",
            "cmsBattFullEnergy",
            "f32ShowSoc",
            "cmsMaxChgSoc",
            "cmsMinDsgSoc",
            # feedGridModePowLimit deliberately absent: block 10 owns that key,
            # being the structure the app writes and the only unambiguous source.
            "feedGridModePowMax",
        )
        # The BMS SoC runs ~1% low and its max-charge field is a fixed ceiling,
        # so neither may share the app-facing key.
        self._copy(
            message.bmsPack.bms,
            params,
            ("soc", "bmsBattSoc"),
            ("cmsMaxChgSoc", "bmsMaxChgSoc"),
            "cmsBattFullEnergy",
            "bmsDsgRemTime",
            "bmsChgRemTime",
            "minCellVol",
            "maxCellVol",
        )
        self._copy(message.cms, params, "cmsBattFullEnergy", "remainTime")
        # Block 54 owns soc: same scale as block 44 but sent far more often.
        stat = _own_entry(message.statPack.stat, self.device_info.sn)
        if stat is not None:
            self._copy(stat, params, "soc")
        power = _own_entry(message.powerPack.power, self.device_info.sn)
        if power is not None:
            self._decode_unit_power(power, params)
        self._copy(message.deviceCfg, params, "upsEnabled", ("xboostEnabled", _KEY_XBOOST))
        self._copy(message.powerLimits, params, "feedGridModePowLimit", "chgPowLimit")
        self._copy(message.acOut, params, ("enabled", _KEY_AC_OUT))
        self._copy(message, params, ("workMode", _KEY_WORK_MODE))

        # Half-watts on the wire; the CMS copy is emitted more often than the BMS.
        for source, field in ((message.cms, "cmsPowerHalfW"), (message.bmsPack.bms, "bmsPowerHalfW")):
            if source.HasField(field):
                params["bpPower"] = getattr(source, field) / 2
                break

    def _decode_unit_power(self, entry: Any, params: dict[str, Any]) -> None:
        # An entry that arrives without them is a unit moving nothing (the
        # night shape), so they read 0 W rather than holding the last value.
        params[_KEY_BATTERY_NET_POWER] = entry.batteryPower
        params[_KEY_AC_GRID_POWER] = entry.acGridPower
        if self._has_pv_inputs():
            params.update(_pv_values(entry))

    @staticmethod
    def _copy(source: Any, params: dict[str, Any], *fields: str | tuple[str, str]) -> None:
        """Copy set protobuf fields onto parameter names.

        Fields are ``"name"`` or ``("name", "alias")``. The ``HasField`` guard
        matters: incremental uploads carry only what changed, so an absent field
        must not overwrite a good value with a proto3 default.
        """
        for field in fields:
            name, alias = field if isinstance(field, tuple) else (field, field)
            if source.HasField(name):
                params[alias] = getattr(source, name)
