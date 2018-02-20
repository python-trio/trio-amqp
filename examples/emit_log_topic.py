#!/usr/bin/env python
"""
    Rabbitmq.com pub/sub example

    https://www.rabbitmq.com/tutorials/tutorial-five-python.html
"""

import trio
import trio_amqp

import sys


async def exchange_routing_topic():
    try:
        async with trio_amqp.connect_amqp() as protocol:

            channel = await protocol.channel()
            exchange_name = 'topic_logs'
            message = ' '.join(sys.argv[2:]) or 'Hello World!'
            routing_key = sys.argv[1] if len(
                sys.argv
            ) > 1 else 'anonymous.info'

            await channel.exchange(exchange_name, 'topic')

            await channel.publish(
                message, exchange_name=exchange_name, routing_key=routing_key
            )
            print(" [x] Sent %r" % message)

    except trio_amqp.AmqpClosedConnection:
        print("closed connections")
        return


trio.run(exchange_routing_topic)
