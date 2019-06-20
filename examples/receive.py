"""
    Hello world `receive.py` example implementation using asyncamqp.
    See the documentation for more informations.
"""

import anyio
import asyncamqp


async def callback(channel, body, envelope, properties):
    print(" [x] Received %r" % body)


async def receive():
    try:
        async with asyncamqp.connect_amqp() as protocol:

            channel = await protocol.channel()

            await channel.queue_declare(queue_name='hello')

            await channel.basic_consume(callback, queue_name='hello')

            while True:
                await anyio.sleep(99999)
    except asyncamqp.AmqpClosedConnection:
        print("closed connections")
        return


anyio.run(receive)
