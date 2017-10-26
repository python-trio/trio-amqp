"""
    Amqp basic class tests
"""

import trio
import struct
import pytest

from . import testcase
from . import testing
from .. import exceptions
from .. import properties


class TestQos(testcase.RabbitTestCase):

    async def test_basic_qos_default_args(self, amqp):
        result = await self.channel.basic_qos()
        assert result

    async def test_basic_qos(self, amqp):
        result = await self.channel.basic_qos(
            prefetch_size=0,
            prefetch_count=100,
            connection_global=False)

        assert result

    async def test_basic_qos_prefetch_size(self, amqp):
        with pytest.raises(exceptions.ChannelClosed) as cm:
            await self.channel.basic_qos(
                prefetch_size=10,
                prefetch_count=100,
                connection_global=False)

        assert cm.exception.code == 540

    async def test_basic_qos_wrong_values(self, amqp):
        with pytest.raises(struct.error):
            await self.channel.basic_qos(
                prefetch_size=100000,
                prefetch_count=1000000000,
                connection_global=False)


class TestBasicCancel(testcase.RabbitTestCase):

    async def test_basic_cancel(self, amqp):

        async def callback(channel, body, envelope, _properties):
            pass

        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        await self.channel.queue_declare(queue_name)
        await self.channel.exchange_declare(exchange_name, type_name='direct')
        await self.channel.queue_bind(queue_name, exchange_name, routing_key='')
        result = await self.channel.basic_consume(callback, queue_name=queue_name)
        result = await self.channel.basic_cancel(result['consumer_tag'])

        result = await self.channel.publish("payload", exchange_name, routing_key='')

        await trio.sleep(5)

        result = await self.channel.queue_declare(queue_name, passive=True)
        assert result['message_count'] == 1
        assert result['consumer_count'] == 0


    async def test_basic_cancel_unknown_ctag(self, amqp):
        result = await self.channel.basic_cancel("unknown_ctag")
        assert result


class TestBasicGet(testcase.RabbitTestCase):


    async def test_basic_get(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.channel.queue_declare(queue_name)
        await self.channel.exchange_declare(exchange_name, type_name='direct')
        await self.channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)

        await self.channel.publish("payload", exchange_name, routing_key=routing_key)

        result = await self.channel.basic_get(queue_name)
        assert result['routing_key'] == routing_key
        assert not result['redelivered']
        assert 'delivery_tag' in result
        assert result['exchange_name'].split('.')[-1] == exchange_name
        assert result['message'] == b'payload'
        assert isinstance(result['properties'], properties.Properties)

    async def test_basic_get_empty(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''
        await self.channel.queue_declare(queue_name)
        await self.channel.exchange_declare(exchange_name, type_name='direct')
        await self.channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)

        with pytest.raises(exceptions.EmptyQueue):
            await self.channel.basic_get(queue_name)


class TestBasicDelivery(testcase.RabbitTestCase):


    async def publish(self, queue_name, exchange_name, routing_key, payload):
        await self.channel.queue_declare(queue_name, exclusive=False, no_wait=False)
        await self.channel.exchange_declare(exchange_name, type_name='fanout')
        await self.channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)
        await self.channel.publish(payload, exchange_name, queue_name)



    async def test_ack_message(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = trio.Event()

        async def qcallback(channel, body, envelope, _properties):
            qfuture.set()
            self.test_result = envelope

        await self.channel.basic_consume(qcallback, queue_name=queue_name)
        await qfuture.wait()
        envelope = self.test_result

        await self.channel.basic_client_ack(envelope.delivery_tag)

    async def test_basic_nack(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = trio.Event()

        async def qcallback(channel, body, envelope, _properties):
            await self.channel.basic_client_nack(
                envelope.delivery_tag, multiple=True, requeue=False
            )
            qfuture.set()

        await self.channel.basic_consume(qcallback, queue_name=queue_name)
        await qfuture.wait()

    async def test_basic_nack_norequeue(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = trio.Event()

        async def qcallback(channel, body, envelope, _properties):
            await self.channel.basic_client_nack(envelope.delivery_tag, requeue=False)
            qfuture.set()

        await self.channel.basic_consume(qcallback, queue_name=queue_name)
        await qfuture.wait()

    async def test_basic_nack_requeue(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = trio.Event()
        called = False

        async def qcallback(channel, body, envelope, _properties):
            nonlocal called
            if not called:
                called = True
                await self.channel.basic_client_nack(envelope.delivery_tag, requeue=True)
            else:
                await self.channel.basic_client_ack(envelope.delivery_tag)
                qfuture.set()

        await self.channel.basic_consume(qcallback, queue_name=queue_name)
        await qfuture.wait()


    async def test_basic_reject(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''
        await self.publish(
            queue_name, exchange_name, routing_key, "payload"
        )

        qfuture = trio.Event()

        async def qcallback(channel, body, envelope, _properties):
            qfuture.set()
            self.test_result = envelope

        await self.channel.basic_consume(qcallback, queue_name=queue_name)
        await qfuture.wait()
        envelope = self.test_result

        await self.channel.basic_reject(envelope.delivery_tag)
