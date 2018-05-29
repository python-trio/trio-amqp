"""
    Hello world `send.py` example implementation using trio_amqp.
    See the documentation for more informations.

    If there is no queue listening for the routing key, the message will
    get returned.

"""

import sys
import trio
import trio_amqp

async def handle_return(channel, body, envelope, properties):
    print('Got a returned message with routing key: {}.\n'
          'Return code: {}\n'
          'Return message: {}\n'
          'exchange: {}'.format(envelope.routing_key, envelope.reply_code,
                                envelope.reply_text, envelope.exchange_name))


async def get_returns(chan, task_status=trio.TASK_STATUS_IGNORED):
    task_status.started()
    # DO NOT await() between these statements
    async for body, envelope, properties in chan:
        await handle_return(channel, body, envelope, properties)


async def send():
    async with trio_amqp.connect_amqp() as protocol:
        channel = await protocol.channel()
        await protocol.nursery.start(get_returns, channel)

        await channel.queue_declare(queue_name='hello')

        await channel.basic_publish(
            payload='Hello World!',
            exchange_name='',
            routing_key='helo',  # typo on purpose, will cause the return
            mandatory=True,
        )

        print(" [x] Sent 'Hello World!'")


trio.run(send)
