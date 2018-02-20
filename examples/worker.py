#!/usr/bin/env python
"""
    Worker example from the 2nd tutorial
"""

import trio
import trio_amqp


async def callback(channel, body, envelope, properties):
    print(" [x] Received %r" % body)
    await trio.sleep(body.count(b'.'))
    print(" [x] Done")
    await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)


async def worker():
    async with trio_amqp.connect_amqp() as protocol:

        channel = await protocol.channel()

        await channel.queue(queue_name='task_queue', durable=True)
        await channel.basic_qos(
            prefetch_count=1, prefetch_size=0, connection_global=False
        )
        await channel.basic_consume(callback, queue_name='task_queue')
        await trio.sleep_forever()


trio.run(worker)
