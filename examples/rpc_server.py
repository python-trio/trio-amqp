"""
    RPC server, trio_amqp implementation of RPC examples from RabbitMQ tutorial
"""

import trio
import trio_amqp


def fib(n):
    if n == 0:
        return 0
    elif n == 1:
        return 1
    else:
        return fib(n - 1) + fib(n - 2)


async def on_request(channel, body, envelope, properties):
    n = int(body)

    print(" [.] fib(%s)" % n)
    response = fib(n)

    await channel.basic_publish(
        payload=str(response),
        exchange_name='',
        routing_key=properties.reply_to,
        properties={
            'correlation_id': properties.correlation_id,
        },
    )

    await channel.basic_client_ack(delivery_tag=envelope.delivery_tag)


async def rpc_server():
    async with trio_amqp.connect_amqp() as protocol:

        channel = await protocol.channel()

        await channel.queue_declare(queue_name='rpc_queue')
        await channel.basic_qos(
            prefetch_count=1, prefetch_size=0, connection_global=False
        )
        await channel.basic_consume(on_request, queue_name='rpc_queue')
        print(" [x] Awaiting RPC requests")
        await trio.sleep_forever()


trio.run(rpc_server)
