"""
    Hello world `receive.py` example implementation using trio_amqp.
    See the documentation for more informations.
"""

import trio
import trio_amqp


async def callback(channel, body, envelope, properties):
    print(" [x] Received %r" % body)


async def receive():
    try:
        async with trio_amqp.connect_amqp() as protocol:

            channel = await protocol.channel()

            await channel.queue_declare(queue_name='hello')

            await channel.basic_consume(callback, queue_name='hello')

            await trio.sleep_forever()
    except trio_amqp.AmqpClosedConnection:
        print("closed connections")
        return


trio.run(receive)
