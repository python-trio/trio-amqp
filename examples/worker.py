#!/usr/bin/env python
"""
    Worker example from the 2nd tutorial
"""

import asyncio
import trio_amqp

import sys

async def callback(channel, body, envelope, properties):
    print(" [x] Received %r" % body)
    await asyncio.sleep(body.count(b'.'))
    print(" [x] Done")
    await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)


async def worker():
    try:
        transport, protocol = await trio_amqp.connect('localhost', 5672)
    except trio_amqp.AmqpClosedConnection:
        print("closed connections")
        return


    channel = await protocol.channel()

    await channel.queue(queue_name='task_queue', durable=True)
    await channel.basic_qos(prefetch_count=1, prefetch_size=0, connection_global=False)
    await channel.basic_consume(callback, queue_name='task_queue')


event_loop = asyncio.get_event_loop()
event_loop.run_until_complete(worker())
event_loop.run_forever()



