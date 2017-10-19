"""
    Hello world `receive.py` example implementation using trio_amqp.
    See the documentation for more informations.
"""

import trio
import trio_amqp


async def callback(channel, body, envelope, properties):
    print(" [x] Received %r" % body)

async def receive():
    protocol = await trio_amqp.connect()
    channel = await protocol.channel()

    await channel.queue_declare(queue_name='hello')

    await channel.basic_consume(callback, queue_name='hello')

    try:
        await trio.sleep_forever()
    finally:
        await protocol.aclose()


trio.run(receive)
