"""
    Amqp basic class tests
"""

import anyio
import struct
import pytest

from . import testcase
from async_amqp import exceptions
from async_amqp import properties


class TestQos(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_basic_qos_default_args(self, channel):
        result = await channel.basic_qos()
        assert result

    @pytest.mark.trio
    async def test_basic_qos(self, channel):
        result = await channel.basic_qos(
            prefetch_size=0, prefetch_count=100, connection_global=False
        )

        assert result

    @pytest.mark.trio
    async def test_basic_qos_prefetch_size(self):
        self.reset_vhost()
        conn = testcase.connect(virtualhost=self.vhost,)
        async with conn as amqp:
            async with amqp.new_channel() as channel:
                with pytest.raises(exceptions.ChannelClosed) as cm:
                    await channel.basic_qos(
                        prefetch_size=10, prefetch_count=100, connection_global=False
                    )

        assert cm.value.code == 540

    @pytest.mark.trio
    async def test_basic_qos_wrong_values(self):
        self.reset_vhost()
        conn = testcase.connect(virtualhost=self.vhost,)
        async with conn as amqp:
            async with amqp.new_channel() as channel:
                with pytest.raises(TypeError):
                    await channel.basic_qos(
                        prefetch_size=100000, prefetch_count=1000000000, connection_global=False
                    )


class TestBasicCancel(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_basic_cancel(self, channel):
        async def callback(channel, body, envelope, _properties):
            pass

        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        await channel.queue_declare(queue_name)
        await channel.exchange_declare(exchange_name, type_name='direct')
        await channel.queue_bind(queue_name, exchange_name, routing_key='')
        result = await channel.basic_consume(callback, queue_name=queue_name)
        result = await channel.basic_cancel(result['consumer_tag'])

        result = await channel.publish(b"payload", exchange_name, routing_key='')

        await anyio.sleep(1)
        result = await channel.queue_declare(queue_name, passive=True)
        assert result['message_count'] == 1
        assert result['consumer_count'] == 0

    @pytest.mark.trio
    async def test_basic_cancel_unknown_ctag(self, channel):
        result = await channel.basic_cancel("unknown_ctag")
        assert result


class TestBasicGet(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_basic_get(self, channel):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await channel.queue_declare(queue_name)
        await channel.exchange_declare(exchange_name, type_name='direct')
        await channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)

        await channel.publish(b"payload", exchange_name, routing_key=routing_key)

        result = await channel.basic_get(queue_name)
        assert result['routing_key'] == routing_key
        # XXX # assert not result['redelivered']
        assert 'delivery_tag' in result
        assert result['exchange_name'] == channel.protocol.full_name(exchange_name)
        assert result['message'] == b'payload'
        assert isinstance(result['properties'], properties.Properties)

    @pytest.mark.trio
    async def test_basic_get_empty(self, channel):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''
        await channel.queue_declare(queue_name)
        await channel.exchange_declare(exchange_name, type_name='direct')
        await channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)

        with pytest.raises(exceptions.EmptyQueue):
            await channel.basic_get(queue_name)


class TestBasicDelivery(testcase.RabbitTestCase):
    async def publish(self, amqp, queue_name, exchange_name, routing_key, payload):
        async with amqp.new_channel() as channel:
            await channel.queue_declare(queue_name, exclusive=False, no_wait=False)
            await channel.exchange_declare(exchange_name, type_name='fanout')
            await channel.queue_bind(queue_name, exchange_name, routing_key=routing_key)
            await channel.publish(payload, exchange_name, queue_name)

    @pytest.mark.trio
    async def test_ack_message(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.publish(amqp, queue_name, exchange_name, routing_key, b"payload")

        qfuture = anyio.Event()

        async def qcallback(channel, body, envelope, _properties):
            qfuture.set()
            self.test_result = envelope

        async with amqp.new_channel() as channel:
            await channel.basic_consume(qcallback, queue_name=queue_name)
            await qfuture.wait()
            envelope = self.test_result

            await channel.basic_client_ack(envelope.delivery_tag)

    @pytest.mark.trio
    async def test_basic_nack(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.publish(amqp, queue_name, exchange_name, routing_key, b"payload")

        qfuture = anyio.Event()

        async with amqp.new_channel() as channel:

            async def qcallback(channel, body, envelope, _properties):
                await channel.basic_client_nack(
                    envelope.delivery_tag, multiple=True, requeue=False
                )
                qfuture.set()

            await channel.basic_consume(qcallback, queue_name=queue_name)
            await qfuture.wait()

    @pytest.mark.trio
    async def test_basic_nack_norequeue(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.publish(amqp, queue_name, exchange_name, routing_key, b"payload")

        qfuture = anyio.Event()

        async with amqp.new_channel() as channel:

            async def qcallback(channel, body, envelope, _properties):
                await channel.basic_client_nack(envelope.delivery_tag, requeue=False)
                qfuture.set()

            await channel.basic_consume(qcallback, queue_name=queue_name)
            await qfuture.wait()

    @pytest.mark.trio
    async def test_basic_nack_requeue(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''

        await self.publish(amqp, queue_name, exchange_name, routing_key, b"payload")

        qfuture = anyio.Event()
        called = False

        async with amqp.new_channel() as channel:

            async def qcallback(channel, body, envelope, _properties):
                nonlocal called
                if not called:
                    called = True
                    await channel.basic_client_nack(envelope.delivery_tag, requeue=True)
                else:
                    await channel.basic_client_ack(envelope.delivery_tag)
                    qfuture.set()

            await channel.basic_consume(qcallback, queue_name=queue_name)
            await qfuture.wait()

    @pytest.mark.trio
    async def test_basic_reject(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'
        routing_key = ''
        await self.publish(amqp, queue_name, exchange_name, routing_key, b"payload")

        qfuture = anyio.Event()

        async def qcallback(channel, body, envelope, _properties):
            qfuture.set()
            self.test_result = envelope

        async with amqp.new_channel() as channel:
            await channel.basic_consume(qcallback, queue_name=queue_name)
            await qfuture.wait()
            envelope = self.test_result

            await channel.basic_reject(envelope.delivery_tag)
