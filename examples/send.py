"""
    Hello world `send.py` example implementation using trio_amqp.
    See the documentation for more informations.

"""

import trio
import trio_amqp


async def send():
    transport, protocol = await trio_amqp.connect()
    channel = await protocol.channel()

    await channel.queue_declare(queue_name='hello')

    await channel.basic_publish(
        payload='Hello World!',
        exchange_name='',
        routing_key='hello'
    )

    print(" [x] Sent 'Hello World!'")
    await protocol.close()
    transport.close()



trio.run(send)
