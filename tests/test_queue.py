"""
    Amqp queue class tests
"""

import anyio
import pytest

from . import testcase
from async_amqp import exceptions


class TestQueueDeclare(testcase.RabbitTestCase):
    def setUp(self):
        super().setUp()
        self.consume_future = anyio.Event()

    async def callback(self, body, envelope, properties):
        await self.consume_future.set()
        self.consume_result = (body, envelope, properties)

    async def get_callback_result(self):
        await self.consume_future.wait()
        result = self.consume_result
        self.consume_future = anyio.Event()
        return result

    @pytest.mark.trio
    async def test_queue_declare_no_name(self, channel):
        result = await channel.queue_declare()
        assert result['queue'] is not None

    @pytest.mark.trio
    async def test_queue_declare(self, channel):
        queue_name = 'q15'
        result = await channel.queue_declare(queue_name)
        assert result['message_count'] == 0, result
        assert result['consumer_count'] == 0, result
        assert channel.protocol.local_name(result['queue']) == queue_name, result
        assert result

    @pytest.mark.trio
    async def test_queue_declare_passive(self, channel):
        queue_name = 'q16'
        await channel.queue_declare(queue_name)
        result = await channel.queue_declare(queue_name, passive=True)
        assert result['message_count'] == 0, result
        assert result['consumer_count'] == 0, result
        assert channel.protocol.local_name(result['queue']) == queue_name, result

    @pytest.mark.trio
    async def test_queue_declare_custom_x_message_ttl_32_bits(self, channel):
        queue_name = 'queue_name'
        # 2147483648 == 10000000000000000000000000000000
        # in binary, meaning it is 32 bit long
        x_message_ttl = 2147483648
        result = await channel.queue_declare(queue_name, arguments={
            'x-message-ttl': x_message_ttl
        })
        assert result['message_count'] == 0
        assert result['consumer_count'] == 0
        assert result['queue'].endswith(queue_name)
        assert result

    @pytest.mark.trio
    async def test_queue_declare_passive_nonexistant_queue(self, channel):
        queue_name = 'q17'
        with pytest.raises(exceptions.ChannelClosed) as cm:
            await channel.queue_declare(queue_name, passive=True)

        assert cm.value.code == 404

    @pytest.mark.trio
    async def test_wrong_parameter_queue(self, channel):
        queue_name = 'q18'
        await channel.queue_declare(queue_name, exclusive=False, auto_delete=False)

        with pytest.raises(exceptions.ChannelClosed) as cm:
            await channel.queue_declare(
                queue_name, passive=False, exclusive=True, auto_delete=True
            )

        assert cm.value.code in (405,406)

    @pytest.mark.trio
    async def test_multiple_channel_same_queue(self, amqp):
        queue_name = 'q19'

        async with amqp.new_channel() as channel1:
            async with amqp.new_channel() as channel2:

                result = await channel1.queue_declare(queue_name, passive=False)
                assert result['message_count'] == 0, result
                assert result['consumer_count'] == 0, result
                assert amqp.local_name(result['queue']) == queue_name, result

                result = await channel2.queue_declare(queue_name, passive=False)
                assert result['message_count'] == 0, result
                assert result['consumer_count'] == 0, result
                assert amqp.local_name(result['queue']) == queue_name, result

    async def _test_queue_declare(
        self, amqp, queue_name, exclusive=False, durable=False, auto_delete=False
    ):
        # declare queue
        async with amqp.new_channel() as channel:
            result = await channel.queue_declare(
                queue_name,
                no_wait=False,
                exclusive=exclusive,
                durable=durable,
                auto_delete=auto_delete
            )

            # assert that the returned results matches
            assert amqp.full_name(queue_name) == result['queue']

            queues = await self.list_queues(channel.protocol)
            queue = queues[queue_name]

            # assert queue has been declared with correct arguments
            assert queue_name == queue['name']
            assert 0 == queue.get('consumers', 0), queue
            assert 0 == queue.get('messages_ready', 0), queue
            assert auto_delete == queue['auto_delete'], queue
            assert durable == queue['durable'], queue

            # delete queue
            await self.safe_queue_delete(queue_name, channel)

    @pytest.mark.trio
    async def test_durable_and_auto_deleted(self, amqp):
        await self._test_queue_declare(amqp, 'q1', exclusive=False, durable=True, auto_delete=True)

    @pytest.mark.trio
    async def test_durable_and_not_auto_deleted(self, amqp):
        await self._test_queue_declare(
            amqp, 'q2', exclusive=False, durable=True, auto_delete=False
        )

    @pytest.mark.trio
    async def test_not_durable_and_auto_deleted(self, amqp):
        await self._test_queue_declare(
            amqp, 'q3', exclusive=False, durable=False, auto_delete=True
        )

    @pytest.mark.trio
    async def test_not_durable_and_not_auto_deleted(self, amqp):
        await self._test_queue_declare(
            amqp, 'q4', exclusive=False, durable=False, auto_delete=False
        )

    @pytest.mark.trio
    async def test_exclusive(self, amqp):
        async with amqp.new_channel() as channel:
            # create an exclusive queue
            await channel.queue_declare("q5", exclusive=True)
            # consume from it
            await channel.basic_consume(self.callback, queue_name="q5", no_wait=False)

            # create another amqp connection
            async with self.create_amqp() as amqp:
                async with amqp.new_channel() as channel:
                    # assert that this connection cannot connect to the queue
                    with pytest.raises(exceptions.ChannelClosed):
                        await channel.basic_consume(self.callback, queue_name="q5", no_wait=False)
                        # channels are auto deleted by test case

    @pytest.mark.trio
    async def test_not_exclusive(self, amqp):
        async with amqp.new_channel() as channel:
            # create a non-exclusive queue
            await channel.queue_declare('q6', exclusive=False)
            # consume it
            await channel.basic_consume(self.callback, queue_name='q6', no_wait=False)
            # create an other amqp connection
            async with self.create_amqp(test_seq=amqp.test_seq) as amqp2:
                async with amqp2.new_channel() as channel:
                    # assert that this connection can connect to the queue
                    await channel.basic_consume(self.callback, queue_name='q6', no_wait=False)


class TestQueueDelete(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_delete_queue(self, channel):
        queue_name = 'q7'
        await channel.queue_declare(queue_name)
        result = await channel.queue_delete(queue_name)
        assert result

    @pytest.mark.trio
    async def test_delete_inexistant_queue(self, amqp):
        queue_name = 'q8'
        async with amqp.new_channel() as channel:
            if self.server_version(amqp) < (3, 3, 5):
                with pytest.raises(exceptions.ChannelClosed) as cm:
                    result = await channel.queue_delete(queue_name)

                assert cm.value.code == 404

            else:
                result = await channel.queue_delete(queue_name)
                assert result


class TestQueueBind(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_bind_queue(self, channel):
        queue_name = 'q9'
        exchange_name = 'exchange_name'

        await channel.queue_declare(queue_name)
        await channel.exchange_declare(exchange_name, type_name='direct')

        result = await channel.queue_bind(queue_name, exchange_name, routing_key='')
        assert result

    @pytest.mark.trio
    async def test_bind_unexistant_exchange(self, channel):
        queue_name = 'q10'
        exchange_name = 'exchange_name'

        await channel.queue_declare(queue_name)
        with pytest.raises(exceptions.ChannelClosed) as cm:
            await channel.queue_bind(queue_name, exchange_name, routing_key='')
        assert cm.value.code == 404

    @pytest.mark.trio
    async def test_bind_unexistant_queue(self, channel):
        queue_name = 'q11'
        exchange_name = 'exchange_name'

        await channel.exchange_declare(exchange_name, type_name='direct')

        with pytest.raises(exceptions.ChannelClosed) as cm:
            await channel.queue_bind(queue_name, exchange_name, routing_key='')
        assert cm.value.code == 404

    @pytest.mark.trio
    async def test_unbind_queue(self, channel):
        queue_name = 'q12'
        exchange_name = 'exchange_name'

        await channel.queue_declare(queue_name)
        await channel.exchange_declare(exchange_name, type_name='direct')

        await channel.queue_bind(queue_name, exchange_name, routing_key='')

        result = await channel.queue_unbind(queue_name, exchange_name, routing_key='')
        assert result


class TestQueuePurge(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_purge_queue(self, channel):
        queue_name = 'q13'

        await channel.queue_declare(queue_name)
        result = await channel.queue_purge(queue_name)
        assert result['message_count'] == 0

    @pytest.mark.trio
    async def test_purge_queue_inexistant_queue(self, channel):
        queue_name = 'q14'

        with pytest.raises(exceptions.ChannelClosed) as cm:
            await channel.queue_purge(queue_name)
        assert cm.value.code == 404
