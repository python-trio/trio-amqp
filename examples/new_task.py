#!/usr/bin/env python

import anyio
import async_amqp

import sys


async def new_task():
    try:
        async with async_amqp.connect_amqp() as protocol:

            channel = await protocol.channel()

            await channel.queue('task_queue', durable=True)

            message = ' '.join(sys.argv[1:]) or "Hello World!"

            await channel.basic_publish(
                payload=message,
                exchange_name='',
                routing_key='task_queue',
                properties={
                    'delivery_mode': 2,
                },
            )
            print(" [x] Sent %r" % message,)

    except async_amqp.AmqpClosedConnection:
        print("closed connections")
        return


anyio.run(new_task)
