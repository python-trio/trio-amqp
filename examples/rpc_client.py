#!/usr/bin/env python
"""
    RPC client, trio_amqp implementation of RPC examples from RabbitMQ tutorial

"""

import trio
import uuid

import trio_amqp


class FibonacciRpcClient(object):
    def __init__(self):
        self.protocol = None
        self.channel = None
        self.callback_queue = None
        self.waiter = trio.Event()

    async def connect(self, channel):
        """ an `__init__` method can't be a coroutine"""
        result = await channel.queue_declare(
            queue_name='', exclusive=True
        )
        self.callback_queue = result['queue']

        await channel.basic_consume(
            self.on_response,
            no_ack=True,
            queue_name=self.callback_queue,
        )

    async def on_response(self, channel, body, envelope, properties):
        if self.corr_id == properties.correlation_id:
            self.response = body

        self.waiter.set()

    async def call(self, n):
        async with trio_amqp.connect_amqp() as protocol:
            async with protocol.channel() as channel:
                await self.connect(channel)

                self.response = None
                self.corr_id = str(uuid.uuid4())
                await self.channel.basic_publish(
                    payload=str(n),
                    exchange_name='',
                    routing_key='rpc_queue',
                    properties={
                        'reply_to': self.callback_queue,
                        'correlation_id': self.corr_id,
                    },
                )
                await self.waiter.wait()

                return int(self.response)


async def rpc_client():
    fibonacci_rpc = FibonacciRpcClient()
    print(" [x] Requesting fib(30)")
    response = await fibonacci_rpc.call(30)
    print(" [.] Got %r" % response)


trio.run(rpc_client)
