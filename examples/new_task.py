#!/usr/bin/env python

import anyio
import asyncamqp

import sys


async def new_task():
    try:
        async with asyncamqp.connect_amqp() as protocol:

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

    except asyncamqp.AmqpClosedConnection:
        print("closed connections")
        return


anyio.run(new_task)
