#!/usr/bin/env python
"""
    Worker example from the 2nd tutorial
"""

import anyio
import async_amqp


async def callback(channel, body, envelope, properties):
    print(" [x] Received %r" % body)
    await anyio.sleep(body.count(b'.'))
    print(" [x] Done")
    await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)


async def worker():
    async with async_amqp.connect_amqp() as protocol:

        channel = await protocol.channel()

        await channel.queue(queue_name='task_queue', durable=True)
        await channel.basic_qos(
            prefetch_count=1, prefetch_size=0, connection_global=False
        )
        await channel.basic_consume(callback, queue_name='task_queue')
        while True:
            await anyio.sleep(99999)


anyio.run(worker)
