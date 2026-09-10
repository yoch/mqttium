"""Compare finite callback-backpressure/network-progress behavior; diagnostic only."""
from __future__ import annotations
import asyncio, json
from mqttium.api import AsyncClient
from mqttium.enums import MQTTProtocolVersion, QoS
from mqttium.packets import PublishPacket
from mqttium.types import Message
from tests.support import ScriptedBrokerTransport, transport_factory

async def main() -> None:
    loop=asyncio.get_running_loop(); errors=[]
    old_handler=loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop,c: errors.append(repr(c.get('exception') or c.get('message'))))
    client=AsyncClient('uniform-saturation-diagnostic',message_delivery='callback',protocol=MQTTProtocolVersion.MQTTv311,keepalive=0,max_pending_callbacks=1,delivery_timeout=1.0)
    transport=ScriptedBrokerTransport(protocol=MQTTProtocolVersion.MQTTv311)
    client._transport_factory=transport_factory(transport)
    outcome=loop.create_future(); seen=[]; queue_full_seen=False
    async def callback(message: Message) -> None:
        nonlocal queue_full_seen
        seen.append(message.payload.decode())
        if message.payload != b'0': return
        for _ in range(3): await asyncio.sleep(0)
        queue_full_seen=client._callback_queue.full()
        receipt=client.publish_nowait('uniform/reply',b'reply',qos=1)
        try: await asyncio.wait_for(receipt.wait(),0.25)
        except TimeoutError:
            if not outcome.done(): outcome.set_result(False)
        else:
            if not outcome.done(): outcome.set_result(True)
    client.on_message=callback
    try:
        await client.connect('memory',timeout=1)
        transport.push_rx(b''.join(PublishPacket(topic='uniform/x',payload=str(i).encode(),qos=QoS.AT_LEAST_ONCE,retain=False,dup=False,mid=i+1).encode(MQTTProtocolVersion.MQTTv311) for i in range(3)))
        progressed=await asyncio.wait_for(outcome,1)
        for _ in range(10): await asyncio.sleep(0)
        print(json.dumps({'reply_progressed_with_full_callback_queue':progressed,'queue_full_seen':queue_full_seen,'seen_before_cleanup':seen,'callback_queue_size':client._callback_queue.qsize(),'effect_pending':len(client._effect_pump.pending),'connected':client.is_connected,'loop_errors':errors},sort_keys=True))
    finally:
        try: await asyncio.wait_for(client.disconnect(),2)
        except Exception: pass
        await client._shutdown_callback_worker(drain=False)
        loop.set_exception_handler(old_handler)
asyncio.run(main())
