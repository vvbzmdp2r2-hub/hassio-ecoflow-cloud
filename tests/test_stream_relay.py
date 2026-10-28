"""Offline MQTT dispatch regression; synthetic serials, real protobuf/decoder.

No HA server, broker, credentials, or hardware connection is created.
"""
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from test_stream_ac_5000 import PAIR_A, PAIR_B, _device, _frames

from custom_components.ecoflow_cloud.api.ecoflow_mqtt import EcoflowMQTTClient
from custom_components.ecoflow_cloud.devices.internal.proto import stream_ac_5000_pb2, stream_ac_pb2

SNS = ['ES21TESTUNITAAAA', 'ES21TESTUNITBBBB', 'ES21TESTUNITCCCC']


def client_for(devices):
    # Bypass constructor: it connects to MQTT. Only exercise real receive dispatch.
    client = object.__new__(EcoflowMQTTClient)
    client._EcoflowMQTTClient__devices = {d.device_info.sn: d for d in devices}
    client.on_set_reply = Mock()
    client.on_message_processed = Mock()
    return client


def deliver(client, topic, payload):
    client._on_message(None, None, SimpleNamespace(topic=topic, payload=payload))


def packet(serials=SNS, cmd=39):
    runtime = stream_ac_5000_pb2.StreamAC5000Runtime()
    # These unaddressed values belong only to the topic owner, never to peers.
    runtime.sysConfig.f32ShowSoc = 99
    runtime.meter.meterTotalPower = 9000
    for i, sn in enumerate(serials):
        stat = runtime.statPack.stat.add()
        stat.sn = sn
        stat.soc = 20 + i
        power = runtime.powerPack.power.add()
        power.sn = sn
        power.batteryPower = 100 + i
        power.acGridPower = 200 + i
        power.pvPower = 300 + i
        power.pv1Power = 300 + i
    envelope = stream_ac_pb2.StreamACSendHeaderMsg()
    envelope.msg.cmd_func = 254
    envelope.msg.cmd_id = cmd
    envelope.msg.pdata = runtime.SerializeToString()
    return envelope.SerializeToString()


def test_master_frame_updates_all_three_devices():
    devices = [_device(sn) for sn in SNS]
    deliver(client_for(devices), devices[0].device_info.data_topic, packet())
    assert [d.data.params.get('soc') for d in devices] == [20, 21, 22]
    assert [d.data.params.get('pvPower') for d in devices] == [300, 301, 302]
    for d in devices[1:]:
        assert 'f32ShowSoc' not in d.data.params
        assert 'meterTotalPower' not in d.data.params


def test_existing_masked_real_pair_frame_through_dispatch():
    a, b = _device(PAIR_A), _device(PAIR_B)
    # Existing MIT fixtures, no new private capture.
    frame = next(f for f in _frames('es21_pair_pv_masked.json')
                 if b._prepare_data(bytes.fromhex(f['hex']))['params'].get('pvPower', 0) > 0)
    raw = bytes.fromhex(frame['hex'])
    expected = b._prepare_data(raw)['params']['pvPower']
    deliver(client_for([a, b]), a.device_info.data_topic, raw)
    assert b.data.params['pvPower'] == expected


@pytest.mark.parametrize('suffix', ['set_topic', 'set_reply_topic', 'get_topic', 'get_reply_topic'])
def test_no_command_or_reply_relay(suffix):
    a, b = [_device(sn) for sn in SNS[:2]]
    deliver(client_for([a, b]), getattr(a.device_info, suffix), packet())
    assert not b.data.params


def test_unknown_topic_does_not_relay():
    devices = [_device(sn) for sn in SNS]
    deliver(client_for(devices), '/app/device/property/ES21UNKNOWN', packet())
    assert all(not d.data.params for d in devices)


def test_missing_recipient_does_not_refresh_or_invent_values():
    a, b = [_device(sn) for sn in SNS[:2]]
    before = b.data.last_received_time()
    deliver(client_for([a, b]), a.device_info.data_topic, packet([SNS[0]]))
    assert not b.data.params
    assert b.data.last_received_time() == before


def test_master_change_and_duplicate_do_not_sum_values():
    devices = [_device(sn) for sn in SNS]
    c = client_for(devices)
    raw = packet()
    for owner in [0, 0, 2, 1]:
        deliver(c, devices[owner].device_info.data_topic, raw)
        assert [d.data.params.get('pvPower') for d in devices] == [300, 301, 302]


@pytest.mark.parametrize('raw', [b'broken protobuf', packet(cmd=50), packet(cmd=2)])
def test_other_or_malformed_packets_do_not_relay(raw):
    a, b = [_device(sn) for sn in SNS[:2]]
    deliver(client_for([a, b]), a.device_info.data_topic, raw)
    assert not b.data.params


def test_retained_message_does_not_refresh_peer():
    a, b = [_device(sn) for sn in SNS[:2]]
    c = client_for([a, b])
    before = b.data.last_received_time()
    c._on_message(None, None, SimpleNamespace(topic=a.device_info.data_topic, payload=packet(), retain=True))
    assert not b.data.params
    assert b.data.last_received_time() == before


def test_other_entry_cannot_update_recipient():
    a, b = [_device(sn) for sn in SNS[:2]]
    deliver(client_for([b]), a.device_info.data_topic, packet())
    assert not b.data.params


def test_non_stream_source_cannot_relay():
    from custom_components.ecoflow_cloud.devices import BaseDevice
    b = _device(SNS[1])
    assert not b.update_peer_data(packet(), Mock(spec=BaseDevice))
    assert not b.data.params


def test_missing_tagged_soc_keeps_previous_value():
    a, b = [_device(sn) for sn in SNS[:2]]
    c = client_for([a, b])
    deliver(c, a.device_info.data_topic, packet())
    runtime = stream_ac_5000_pb2.StreamAC5000Runtime()
    runtime.statPack.stat.add().sn = SNS[1]  # identity but no reported SoC
    envelope = stream_ac_pb2.StreamACSendHeaderMsg()
    envelope.msg.cmd_func = 254
    envelope.msg.cmd_id = 39
    envelope.msg.pdata = runtime.SerializeToString()
    before = b.data.last_received_time()
    deliver(c, a.device_info.data_topic, envelope.SerializeToString())
    assert b.data.params['soc'] == 21
    assert b.data.last_received_time() == before


def test_ambiguous_duplicate_identity_is_rejected():
    a, b = [_device(sn) for sn in SNS[:2]]
    deliver(client_for([a, b]), a.device_info.data_topic, packet([SNS[1], SNS[1]]))
    assert not b.data.params


def test_wrong_command_family_is_not_relayed():
    a, b = [_device(sn) for sn in SNS[:2]]
    envelope = stream_ac_pb2.StreamACSendHeaderMsg()
    envelope.ParseFromString(packet())
    envelope.msg.cmd_func = 1
    deliver(client_for([a, b]), a.device_info.data_topic, envelope.SerializeToString())
    assert not b.data.params
