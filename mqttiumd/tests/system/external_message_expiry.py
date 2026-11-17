"""Message Expiry clients using only mqttium and Paho public APIs."""
from __future__ import annotations

import asyncio
import hashlib

from external_retained import MqttiumRetained, PahoRetained
from external_sessions import message_row
from external_wildcard import PacketTap as PlainTap, TIMEOUT
from message_expiry_probe import decode_publish, interval
from mqttium.api import Properties
from tcp_probe import ReceiptFailure, require


class PacketTap(PlainTap):
    def publications(self, payload=None):
        values = [decode_publish(frame) for frame in self.server_frames() if frame.kind == 0x30]
        return values if payload is None else [value for value in values if value.payload == payload]


class MqttiumMessageExpiry(MqttiumRetained):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.unacknowledged = {}

    async def publish(self, topic, payload, qos=1, *, retain=False, message_expiry=None):
        expiry = interval(message_expiry)
        props = None if expiry is None else Properties({'message_expiry_interval': expiry})
        receipt = await self.client.publish(topic, payload, qos=qos, retain=retain,
                                            properties=props, timeout=TIMEOUT)
        self.log.record('publication_submitted', qos=qos, retain=retain,
                        message_expiry_interval=expiry, packet_id=receipt.mid if qos else None,
                        client_receipt_id=receipt.mid, payload_sha256=hashlib.sha256(payload).hexdigest())
        try:
            await receipt.wait(timeout=TIMEOUT)
        except Exception as error:
            self.log.record('publication_failed', exception=type(error).__name__,
                            reason_code=getattr(error, 'reason_code', None))
            raise ReceiptFailure('message_expiry_publication_not_completed',
                                 f'{type(error).__name__}: {error}') from error
        require(receipt.is_done() and receipt.exception() is None, 'message_expiry_publication_not_completed')
        self.log.record('publication_completed', qos=qos, retain=retain, message_expiry_interval=expiry,
                        packet_id=receipt.mid if qos else None,
                        completion_scope='local-write-only' if qos == 0 else 'MQTT acknowledgement')
        return receipt.mid

    async def publish_rejected(self, topic, payload, *, reason, retain=True, message_expiry=None):
        """Require the public QoS1 receipt's exact typed MQTT refusal."""
        from mqttium import PublishRejectedError
        expiry = interval(message_expiry)
        props = None if expiry is None else Properties({'message_expiry_interval': expiry})
        receipt = await self.client.publish(topic, payload, qos=1, retain=retain,
                                            properties=props, timeout=TIMEOUT)
        self.log.record('publication_submitted', qos=1, retain=retain, packet_id=receipt.mid,
                        message_expiry_interval=expiry, payload_sha256=hashlib.sha256(payload).hexdigest())
        try:
            await receipt.wait(timeout=TIMEOUT)
        except PublishRejectedError as error:
            require(error.reason_code == reason and receipt.is_done() and
                    isinstance(receipt.exception(), PublishRejectedError),
                    'message_expiry_capacity_wrong_typed_refusal', str(error.reason_code))
            row = {'exception': type(error).__name__, 'reason_code': error.reason_code,
                   'packet_id': receipt.mid, 'qos': 1, 'receipt_done': receipt.is_done()}
            self.log.record('publication_rejected', **row)
            return row
        raise ReceiptFailure('message_expiry_capacity_publication_was_not_rejected')

    async def receive(self, payload=None, qos=None, retain=None, *, acknowledge=True):
        if self.reader_error:
            raise self.reader_error
        message = await asyncio.wait_for(self.incoming.get(), timeout=TIMEOUT)
        expiry = None if message.properties is None else message.properties.get('message_expiry_interval')
        row = {**message_row(message), 'retain': bool(message.retain),
               'message_expiry_present': expiry is not None, 'message_expiry_interval': expiry}
        self.log.record('message', **row)
        require(payload is None or bytes(message.payload) == payload, 'message_expiry_payload_mismatch')
        require(qos is None or row['qos'] == qos, 'message_expiry_qos_mismatch')
        require(retain is None or row['retain'] == retain, 'message_expiry_retain_mismatch')
        if row['qos']:
            self.unacknowledged[message.mid] = message
            if acknowledge:
                await self.acknowledge(row)
        return row

    async def acknowledge(self, row):
        message = self.unacknowledged.pop(row['packet_id'])
        await self.client.ack(message)
        self.log.record('puback_submitted', packet_id=message.mid)


class PahoMessageExpiry(PahoRetained):
    def _message(self, _client, _data, message):
        expiry = getattr(message.properties, 'MessageExpiryInterval', None)
        row = {**message_row(message), 'retain': bool(message.retain),
               'message_expiry_present': expiry is not None, 'message_expiry_interval': expiry}
        self.log.record('message', **row)
        self._put(self.messages, row)

    async def publish(self, topic, payload, qos=1, *, retain=False, message_expiry=None):
        from paho.mqtt.packettypes import PacketTypes
        from paho.mqtt.properties import Properties as PahoProperties
        expiry = interval(message_expiry)
        props = None
        if expiry is not None:
            props = PahoProperties(PacketTypes.PUBLISH)
            props.MessageExpiryInterval = expiry
        info = self.client.publish(topic, payload, qos=qos, retain=retain, properties=props)
        require(info.rc == self.mqtt.MQTT_ERR_SUCCESS, 'message_expiry_publication_submission_refused')
        await asyncio.to_thread(info.wait_for_publish, TIMEOUT)
        require(info.is_published(), 'message_expiry_publication_not_completed')
        mid, reason = await asyncio.to_thread(self._get, self.pubacks)
        require(mid == info.mid and reason < 128, 'message_expiry_publication_not_completed', str(reason))
        self.log.record('publication_completed', qos=qos, retain=retain, message_expiry_interval=expiry,
                        packet_id=mid if qos else None, client_receipt_id=mid,
                        completion_scope='local-write-only' if qos == 0 else 'MQTT acknowledgement')
        return mid

    async def receive(self, payload=None, qos=None, retain=None, *, acknowledge=True):
        row = await asyncio.to_thread(self._get, self.messages)
        require(payload is None or row['payload_sha256'] == hashlib.sha256(payload).hexdigest(),
                'message_expiry_payload_mismatch')
        require(qos is None or row['qos'] == qos, 'message_expiry_qos_mismatch')
        require(retain is None or row['retain'] == retain, 'message_expiry_retain_mismatch')
        if row['qos'] and acknowledge:
            await self.acknowledge(row)
        return row

    async def acknowledge(self, row):
        require(self.client.ack(row['packet_id'], row['qos']) == self.mqtt.MQTT_ERR_SUCCESS,
                'message_expiry_puback_submission_refused')
        self.log.record('puback_submitted', packet_id=row['packet_id'])
