"""
    Amqp queue class tests
"""

import trio
import pytest
from functools import partial

from . import testcase
from .. import exceptions


class TestQueueDeclare(testcase.RabbitTestCase):

    def setUp(self):
        super().setUp()
        self.consume_future = trio.Event()

    async def callback(self, body, envelope, properties):
        self.consume_future.set()
        self.consume_result = (body, envelope, properties)

    async def get_callback_result(self):
        await self.consume_future.wait()
        result = self.consume_result
        self.consume_future = trio.Event()
        return result

    async def test_queue_declare_no_name(self, amqp):
        result = await self.channel.queue_declare()
        assert result['queue'] is not None

    async def test_queue_declare(self, amqp):
        queue_name = 'queue_name'
        result = await self.channel.queue_declare('queue_name')
        assert result['message_count'] == 0
        assert result['consumer_count'] == 0
        assert result['queue'].split('.')[-1] == queue_name
        assert result

    async def test_queue_declare_passive(self, amqp):
        queue_name = 'queue_name'
        await self.channel.queue_declare('queue_name')
        result = await self.channel.queue_declare(queue_name, passive=True)
        assert result['message_count'] == 0
        assert result['consumer_count'] == 0
        assert result['queue'].split('.')[-1] == queue_name

    async def test_queue_declare_passive_nonexistant_queue(self, amqp):
        queue_name = 'queue_name'
        with pytest.raises(exceptions.ChannelClosed) as cm:
            await self.channel.queue_declare(queue_name, passive=True)

        assert cm.value.code == 404

    async def test_wrong_parameter_queue(self, amqp):
        queue_name = 'queue_name'
        await self.channel.queue_declare(queue_name, exclusive=False, auto_delete=False)

        with pytest.raises(exceptions.ChannelClosed) as cm:
            await self.channel.queue_declare(queue_name,
                passive=False, exclusive=True, auto_delete=True)

        assert cm.value.code == 406

    async def test_multiple_channel_same_queue(self, amqp):
        queue_name = 'queue_name'

        channel1 = await self.amqp.channel()
        channel2 = await self.amqp.channel()

        result = await channel1.queue_declare(queue_name, passive=False)
        assert result['message_count'] == 0
        assert result['consumer_count'] == 0
        assert result['queue'].split('.')[-1] == queue_name

        result = await channel2.queue_declare(queue_name, passive=False)
        assert result['message_count'] == 0
        assert result['consumer_count'] == 0
        assert result['queue'].split('.')[-1] == queue_name

    async def _test_queue_declare(self, queue_name, exclusive=False, durable=False, auto_delete=False):
        # declare queue
        result = await self.channel.queue_declare(
            queue_name, no_wait=False, exclusive=exclusive, durable=durable,
            auto_delete=auto_delete)

        # assert returned results has the good arguments
        # in test the channel declared queues with prefixed names, to get the full name of the
        # declared queue we have to use self.full_name function
        assert self.full_name(queue_name) == result['queue']

        queues = self.list_queues()
        queue = queues[queue_name]

        # assert queue has been declared with the good arguments
        assert queue_name == queue['name']
        assert 0 == queue.get('consumers', 0)
        assert 0 == queue.get('messages_ready', 0)
        assert auto_delete == queue['auto_delete']
        assert durable == queue['durable']

        # delete queue
        await self.safe_queue_delete(queue_name)

    async def test_durable_and_auto_deleted(self, amqp):
        await self._test_queue_declare('q', exclusive=False, durable=True, auto_delete=True)

    async def test_durable_and_not_auto_deleted(self, amqp):
        await self._test_queue_declare('q', exclusive=False, durable=True, auto_delete=False)

    async def test_not_durable_and_auto_deleted(self, amqp):
        await self._test_queue_declare('q', exclusive=False, durable=False, auto_delete=True)

    async def test_not_durable_and_not_auto_deleted(self, amqp):
        await self._test_queue_declare('q', exclusive=False, durable=False, auto_delete=False)

    async def test_exclusive(self, amqp):
        # create an exclusive queue
        await self.channel.queue_declare("q", exclusive=True)
        # consume from it
        await self.channel.basic_consume(self.callback, queue_name="q", no_wait=False)

        # create another amqp connection
        async with self.create_amqp() as protocol:
            channel = await self.create_channel(amqp=protocol)
            # assert that this connection cannot connect to the queue
            with pytest.raises(exceptions.ChannelClosed):
                await channel.basic_consume(self.callback, queue_name="q", no_wait=False)
            # channels are auto deleted by test case

    async def test_not_exclusive(self, amqp):
        # create a non-exclusive queue
        await self.channel.queue_declare('q', exclusive=False)
        # consume it
        await self.channel.basic_consume(self.callback, queue_name='q', no_wait=False)
        # create an other amqp connection
        async with self.create_amqp() as protocol:
            channel = await self.create_channel(amqp=protocol)
            # assert that this connection can connect to the queue
            await channel.basic_consume(self.callback, queue_name='q', no_wait=False)


class TestQueueDelete(testcase.RabbitTestCase):


    async def test_delete_queue(self, amqp):
        queue_name = 'queue_name'
        await self.channel.queue_declare(queue_name)
        result = await self.channel.queue_delete(queue_name)
        assert result

    async def test_delete_inexistant_queue(self, amqp):
        queue_name = 'queue_name'
        if self.server_version() < (3, 3, 5):
            with pytest.raises(exceptions.ChannelClosed) as cm:
                result = await self.channel.queue_delete(queue_name)

            assert cm.value.code == 404

        else:
            result = await self.channel.queue_delete(queue_name)
            assert result

class TestQueueBind(testcase.RabbitTestCase):


    async def test_bind_queue(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'

        await self.channel.queue_declare(queue_name)
        await self.channel.exchange_declare(exchange_name, type_name='direct')

        result = await self.channel.queue_bind(queue_name, exchange_name, routing_key='')
        assert result

    async def test_bind_unexistant_exchange(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'


        await self.channel.queue_declare(queue_name)
        with pytest.raises(exceptions.ChannelClosed) as cm:
            await self.channel.queue_bind(queue_name, exchange_name, routing_key='')
        assert cm.value.code == 404

    async def test_bind_unexistant_queue(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'


        await self.channel.exchange_declare(exchange_name, type_name='direct')

        with pytest.raises(exceptions.ChannelClosed) as cm:
            await self.channel.queue_bind(queue_name, exchange_name, routing_key='')
        assert cm.value.code == 404

    async def test_unbind_queue(self, amqp):
        queue_name = 'queue_name'
        exchange_name = 'exchange_name'

        await self.channel.queue_declare(queue_name)
        await self.channel.exchange_declare(exchange_name, type_name='direct')

        await self.channel.queue_bind(queue_name, exchange_name, routing_key='')

        result = await self.channel.queue_unbind(queue_name, exchange_name, routing_key='')
        assert result


class TestQueuePurge(testcase.RabbitTestCase):


    async def test_purge_queue(self, amqp):
        queue_name = 'queue_name'

        await self.channel.queue_declare(queue_name)
        result = await self.channel.queue_purge(queue_name)
        assert result['message_count'] == 0

    async def test_purge_queue_inexistant_queue(self, amqp):
        queue_name = 'queue_name'

        with pytest.raises(exceptions.ChannelClosed) as cm:
            await self.channel.queue_purge(queue_name)
        assert cm.value.code == 404
