#!/usr/bin/env python

import asyncio
import trio_amqp

import sys


async def new_task():
    try:
        transport, protocol = await trio_amqp.connect('localhost', 5672)
    except trio_amqp.AmqpClosedConnection:
        print("closed connections")
        return


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

    await protocol.close()
    transport.close()


asyncio.get_event_loop().run_until_complete(new_task())


