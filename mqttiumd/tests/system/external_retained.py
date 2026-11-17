"""Retained-message reception through public mqttium/Paho APIs only."""
from __future__ import annotations

import asyncio
import hashlib

from external_sessions import message_row
from external_wildcard import MqttiumPeer, PahoPeer, PacketTap as PlainTap, TIMEOUT
from retained_probe import decode_publish
from tcp_probe import require


class PacketTap(PlainTap):
    def publications(self, payload=None):
        values = [decode_publish(frame) for frame in self.server_frames() if frame.kind == 0x30]
        return values if payload is None else [value for value in values if value.payload == payload]


class MqttiumRetained(MqttiumPeer):
    async def publish(self, topic, payload, qos=1, *, retain=True):
        receipt = await self.client.publish(topic, payload, qos=qos, retain=retain, timeout=TIMEOUT)
        self.log.record('publication_submitted', qos=qos, retain=retain,
                        packet_id=receipt.mid if qos else None, client_receipt_id=receipt.mid,
                        payload_sha256=hashlib.sha256(payload).hexdigest())
        await receipt.wait(timeout=TIMEOUT)
        require(receipt.is_done() and receipt.exception() is None, 'retained_publication_not_completed')
        self.log.record('publication_completed', qos=qos, retain=retain,
                        packet_id=receipt.mid if qos else None,
                        completion_scope='local-write-only' if qos == 0 else 'MQTT acknowledgement')
        return receipt.mid

    async def receive(self, payload=None, qos=None, retain=None, *, acknowledge=True):
        if self.reader_error:
            raise self.reader_error
        message = await asyncio.wait_for(self.incoming.get(), timeout=TIMEOUT)
        row = {**message_row(message), 'retain': bool(message.retain)}
        self.log.record('message', **row)
        require(payload is None or bytes(message.payload) == payload, 'retained_payload_mismatch')
        require(qos is None or row['qos'] == qos, 'retained_qos_mismatch')
        require(retain is None or row['retain'] == retain, 'retained_flag_mismatch')
        if row['qos'] and acknowledge:
            await self.client.ack(message)
            self.log.record('puback_submitted', packet_id=message.mid)
        return row


class PahoRetained(PahoPeer):
    def _message(self, _client, _data, message):
        row = {**message_row(message), 'retain': bool(message.retain)}
        self.log.record('message', **row)
        self._put(self.messages, row)

    async def publish(self, topic, payload, qos=1, *, retain=True):
        info = self.client.publish(topic, payload, qos=qos, retain=retain)
        require(info.rc == self.mqtt.MQTT_ERR_SUCCESS, 'paho_retained_publish_submission')
        await asyncio.to_thread(info.wait_for_publish, TIMEOUT)
        require(info.is_published(), 'retained_publication_not_completed')
        mid, reason = await asyncio.to_thread(self._get, self.pubacks)
        require(mid == info.mid and reason < 128, 'retained_publication_not_completed')
        self.log.record('publication_completed', qos=qos, retain=retain,
                        packet_id=mid if qos else None, client_receipt_id=mid,
                        completion_scope='local-write-only' if qos == 0 else 'MQTT acknowledgement')
        return mid

    async def receive(self, payload=None, qos=None, retain=None, *, acknowledge=True):
        row = await asyncio.to_thread(self._get, self.messages)
        require(payload is None or row['payload_sha256'] == hashlib.sha256(payload).hexdigest(),
                'retained_payload_mismatch')
        require(qos is None or row['qos'] == qos, 'retained_qos_mismatch')
        require(retain is None or row['retain'] == retain, 'retained_flag_mismatch')
        if row['qos'] and acknowledge:
            require(self.client.ack(row['packet_id'], row['qos']) == self.mqtt.MQTT_ERR_SUCCESS,
                    'paho_retained_ack_submission')
            self.log.record('puback_submitted', packet_id=row['packet_id'])
        return row
