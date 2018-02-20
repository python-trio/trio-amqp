#!/usr/bin/env python
"""
    Rabbitmq.com pub/sub example

    https://www.rabbitmq.com/tutorials/tutorial-three-python.html

"""

import trio
import trio_amqp


async def callback(channel, body, envelope, properties):
    print(" [x] %r" % body)


async def receive_log():
    try:
        async with trio_amqp.connect_amqp() as protocol:

            channel = await protocol.channel()
            exchange_name = 'logs'

            await channel.exchange(
                exchange_name=exchange_name, type_name='fanout'
            )

            # let RabbitMQ generate a random queue name
            result = await channel.queue(queue_name='', exclusive=True)

            queue_name = result['queue']
            await channel.queue_bind(
                exchange_name=exchange_name,
                queue_name=queue_name,
                routing_key=''
            )

            print(' [*] Waiting for logs. To exit press CTRL+C')

            await channel.basic_consume(
                callback, queue_name=queue_name, no_ack=True
            )

            await trio.sleep_forever()

    except trio_amqp.AmqpClosedConnection:
        print("closed connections")
        return


trio.run(receive_log)
