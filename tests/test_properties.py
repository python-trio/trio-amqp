"""
    Tests for message properties for basic deliver
"""

import anyio
import pytest
import logging

from . import testcase

logger = logging.getLogger(__name__)

server_queue_name = 'server_queue'
client_queue_name = 'client_reply_queue'
exchange_name = 'exchange_name'
server_routing_key = 'reply_test'


class TestReplyOld(testcase.RabbitTestCase):
    """RPC test using classic callbacks"""

    async def _server(
        self,
        amqp,
        server_future,
        exchange_name,
        routing_key,
        *,
        task_status,
    ):
        """Consume messages and reply to them by publishing messages back
        to the client using routing key set to the reply_to property
        """
        async with amqp.new_channel() as channel:
            await channel.queue_declare(server_queue_name, exclusive=False, no_wait=False)
            await channel.exchange_declare(exchange_name, type_name='direct')
            await channel.queue_bind(server_queue_name, exchange_name, routing_key=routing_key)

            async def server_callback(channel, body, envelope, properties):
                logger.debug('Server received message')
                publish_properties = {'correlation_id': properties.correlation_id}
                logger.debug('Replying to %r', properties.reply_to)
                await channel.publish(
                    b'reply message', exchange_name, properties.reply_to, publish_properties
                )
                server_future.test_result = (body, envelope, properties)
                server_future.set()
                logger.debug('Server replied')

            await channel.basic_consume(server_callback, queue_name=server_queue_name)
            logger.debug('Server consuming messages')
            task_status.started()
            await server_future.wait()

    async def _client(
        self,
        amqp,
        client_future,
        exchange_name,
        server_routing_key,
        correlation_id,
        client_routing_key,
        *,
        task_status,
    ):
        """Declare a queue, bind client_routing_key to it, and publish a
        message to the server with the reply_to property set to that
        routing key
        """
        async with amqp.new_channel() as client_channel:
            await client_channel.queue_declare(client_queue_name, exclusive=True, no_wait=False)
            await client_channel.queue_bind(
                client_queue_name, exchange_name, routing_key=client_routing_key
            )

            async def client_callback(channel, body, envelope, properties):
                logger.debug('Client received message')
                client_future.test_result = (body, envelope, properties)
                client_future.set()

            await client_channel.basic_consume(client_callback, queue_name=client_queue_name)
            logger.debug('Client consuming messages')
            task_status.started()

            await client_channel.publish(
                b'client message', exchange_name, server_routing_key, {
                    'correlation_id': correlation_id,
                    'reply_to': client_routing_key
                }
            )
            logger.debug('Client published message')
            await client_future.wait()

    @pytest.mark.trio
    async def test_reply_to(self, amqp):
        server_future = anyio.Event()
        async with anyio.create_task_group() as n:
            await n.start(self._server, amqp, server_future, exchange_name, server_routing_key)

            correlation_id = 'secret correlation id'
            client_routing_key = 'secret_client_key'

            client_future = anyio.Event()
            await n.start(
                self._client, amqp, client_future, exchange_name, server_routing_key,
                correlation_id, client_routing_key
            )

            logger.debug('Waiting for server to receive message')
            await server_future.wait()
            server_body, server_envelope, server_properties = \
                server_future.test_result
            assert server_body == b'client message'
            assert server_properties.correlation_id == correlation_id
            assert server_properties.reply_to == client_routing_key
            assert server_envelope.routing_key == server_routing_key

            logger.debug('Waiting for client to receive message')
            await client_future.wait()
            client_body, client_envelope, client_properties = \
                client_future.test_result
            assert client_body == b'reply message'
            assert client_properties.correlation_id == correlation_id
            assert client_envelope.routing_key == client_routing_key
            n.cancel_scope.cancel()


class TestReplyNew(testcase.RabbitTestCase):
    """RPC test using iteration"""

    async def _server(
        self,
        amqp,
        server_future,
        exchange_name,
        routing_key,
        *,
        task_status,
    ):
        """Consume messages and reply to them by publishing messages back
        to the client using routing key set to the reply_to property
        """
        async with amqp.new_channel() as channel:
            await channel.queue_declare(server_queue_name, exclusive=False, no_wait=False)
            await channel.exchange_declare(exchange_name, type_name='direct')
            await channel.queue_bind(server_queue_name, exchange_name, routing_key=routing_key)

            async with anyio.create_task_group() as n:
                await n.start(self._server_consumer, channel, server_future)
                task_status.started()
                await server_future.wait()
                self._server_scope.cancel()

    async def _server_consumer(self, channel, server_future, *, task_status):
        with anyio.CancelScope() as scope:
            self._server_scope = scope
            async with channel.new_consumer(queue_name=server_queue_name) \
                    as data:
                logger.debug('Server consuming messages')
                task_status.started()
                async for body, envelope, properties in data:

                    logger.debug('Server received message')
                    publish_properties = {'correlation_id': properties.correlation_id}
                    logger.debug('Replying to %r', properties.reply_to)
                    await channel.publish(
                        b'reply message', exchange_name, properties.reply_to, publish_properties
                    )
                    server_future.test_result = (body, envelope, properties)
                    server_future.set()
                    logger.debug('Server replied')

    async def _client(
        self,
        amqp,
        client_future,
        exchange_name,
        server_routing_key,
        correlation_id,
        client_routing_key,
        *,
        task_status,
    ):
        """Declare a queue, bind client_routing_key to it, and publish a
        message to the server with the reply_to property set to that
        routing key
        """
        async with amqp.new_channel() as client_channel:
            await client_channel.queue_declare(client_queue_name, exclusive=True, no_wait=False)
            await client_channel.queue_bind(
                client_queue_name, exchange_name, routing_key=client_routing_key
            )

            async with anyio.create_task_group() as n:
                await n.start(self._client_consumer, client_channel, client_future)
                task_status.started()

                await client_channel.publish(
                    b'client message', exchange_name, server_routing_key, {
                        'correlation_id': correlation_id,
                        'reply_to': client_routing_key
                    }
                )
                logger.debug('Client published message')
                await client_future.wait()
                self._client_scope.cancel()

    async def _client_consumer(self, channel, client_future, *, task_status):
        with anyio.CancelScope() as scope:
            self._client_scope = scope
            async with channel.new_consumer(queue_name=client_queue_name) \
                    as data:
                task_status.started()
                logger.debug('Client consuming messages')

                async for body, envelope, properties in data:
                    logger.debug('Client received message')
                    client_future.test_result = (body, envelope, properties)
                    client_future.set()

    @pytest.mark.trio
    async def test_reply_to(self, amqp):
        server_future = anyio.Event()
        async with anyio.create_task_group() as n:
            await n.start(self._server, amqp, server_future, exchange_name, server_routing_key)

            correlation_id = 'secret correlation id'
            client_routing_key = 'secret_client_key'

            client_future = anyio.Event()
            await n.start(
                self._client, amqp, client_future, exchange_name, server_routing_key,
                correlation_id, client_routing_key
            )

            logger.debug('Waiting for server to receive message')
            await server_future.wait()
            server_body, server_envelope, server_properties = \
                server_future.test_result
            assert server_body == b'client message'
            assert server_properties.correlation_id == correlation_id
            assert server_properties.reply_to == client_routing_key
            assert server_envelope.routing_key == server_routing_key

            logger.debug('Waiting for client to receive message')
            await client_future.wait()
            client_body, client_envelope, client_properties = \
                client_future.test_result
            assert client_body == b'reply message'
            assert client_properties.correlation_id == correlation_id
            assert client_envelope.routing_key == client_routing_key
            n.cancel_scope.cancel()
