"""
    Hello world `send.py` example implementation using trio_amqp.
    See the documentation for more informations.

"""

import trio
import trio_amqp


async def send():
    async with trio_amqp.connect_amqp() as protocol:
        channel = await protocol.channel()

        await channel.queue_declare(queue_name='hello')

        await channel.basic_publish(
            payload='Hello World!', exchange_name='', routing_key='hello'
        )

        print(" [x] Sent 'Hello World!'")


trio.run(send)
