"""
    Amqp exchange class tests
"""

import pytest

from . import testcase
from async_amqp import exceptions


class TestExchangeDeclare(testcase.RabbitTestCase):

    _multiprocess_can_split_ = True

    @pytest.mark.trio
    async def test_exchange_direct_declare(self, channel):
        result = await channel.exchange_declare('exchange_name', type_name='direct')
        assert result

    @pytest.mark.trio
    async def test_exchange_fanout_declare(self, channel):
        result = await channel.exchange_declare('exchange_name', type_name='fanout')
        assert result

    @pytest.mark.trio
    async def test_exchange_topic_declare(self, channel):
        result = await channel.exchange_declare('exchange_name', type_name='topic')
        assert result

    @pytest.mark.trio
    async def test_exchange_headers_declare(self, channel):
        result = await channel.exchange_declare('exchange_name', type_name='headers')
        assert result

    @pytest.mark.trio
    async def test_exchange_declare_wrong_types(self, channel):
        result = await channel.exchange_declare(
            'exchange_name', type_name='headers', auto_delete=True, durable=True
        )
        assert result

        with pytest.raises(exceptions.ChannelClosed):
            result = await channel.exchange_declare(
                'exchange_name', type_name='fanout', auto_delete=False, durable=False
            )

    @pytest.mark.trio
    async def test_exchange_declare_passive(self, channel):
        result = await channel.exchange_declare(
            'exchange_name', type_name='headers', auto_delete=True, durable=True
        )
        assert result
        result = await channel.exchange_declare(
            'exchange_name', type_name='headers', auto_delete=True, durable=True, passive=True
        )
        assert result

        result = await channel.exchange_declare(
            'exchange_name', type_name='headers', auto_delete=False, durable=False, passive=True
        )
        assert result

    @pytest.mark.trio
    async def test_exchange_declare_passive_does_not_exists(self, channel):
        with pytest.raises(exceptions.ChannelClosed) as cm:
            await channel.exchange_declare(
                'non_existant_exchange',
                type_name='headers',
                auto_delete=False,
                durable=False,
                passive=True
            )
        assert cm.value.code == 404

    @pytest.mark.trio
    async def test_exchange_declare_unknown_type(self, channel):
        with pytest.raises(exceptions.ChannelClosed):
            await channel.exchange_declare(
                'non_existant_exchange',
                type_name='unknown_type',
                auto_delete=False,
                durable=False,
                passive=True
            )


class TestExchangeDelete(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_delete(self, channel):
        exchange_name = 'exchange_name'
        await channel.exchange_declare(exchange_name, type_name='direct')
        result = await channel.exchange_delete(exchange_name)
        assert result
        with pytest.raises(exceptions.ChannelClosed) as cm:
            await channel.exchange_declare(exchange_name, type_name='direct', passive=True)

        assert cm.value.code == 404

    @pytest.mark.trio
    async def test_double_delete(self, channel):
        exchange_name = 'exchange_name'
        await channel.exchange_declare(exchange_name, type_name='direct')
        result = await channel.exchange_delete(exchange_name)
        assert result
        if self.server_version(channel.protocol) < (3, 3, 5):
            with pytest.raises(exceptions.ChannelClosed) as cm:
                await channel.exchange_delete(exchange_name)

            assert cm.value.code == 404

        else:
            # weird result from rabbitmq 3.3.5
            result = await channel.exchange_delete(exchange_name)
            assert result


class TestExchangeBind(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_exchange_bind(self, channel):
        await channel.exchange_declare('exchange_destination', type_name='direct')
        await channel.exchange_declare('exchange_source', type_name='direct')

        result = await channel.exchange_bind(
            'exchange_destination', 'exchange_source', routing_key=''
        )

        assert result

    @pytest.mark.trio
    async def test_inexistant_exchange_bind(self, channel):
        with pytest.raises(exceptions.ChannelClosed) as cm:
            await channel.exchange_bind('exchange_destination', 'exchange_source', routing_key='')

        assert cm.value.code == 404


class TestExchangeUnbind(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_exchange_unbind(self, channel):
        ex_source = 'exchange_source'
        ex_destination = 'exchange_destination'
        await channel.exchange_declare(ex_destination, type_name='direct')
        await channel.exchange_declare(ex_source, type_name='direct')

        await channel.exchange_bind(ex_destination, ex_source, routing_key='')

        await channel.exchange_unbind(ex_destination, ex_source, routing_key='')

    @pytest.mark.trio
    async def test_exchange_unbind_reversed(self, channel):
        ex_source = 'exchange_source'
        ex_destination = 'exchange_destination'
        await channel.exchange_declare(ex_destination, type_name='direct')
        await channel.exchange_declare(ex_source, type_name='direct')

        await channel.exchange_bind(ex_destination, ex_source, routing_key='')

        if self.server_version(channel.protocol) < (3, 3, 5):
            with pytest.raises(exceptions.ChannelClosed) as cm:
                result = await channel.exchange_unbind(ex_source, ex_destination, routing_key='')

            assert cm.value.code == 404

        else:
            # weird result from rabbitmq 3.3.5
            result = await channel.exchange_unbind(ex_source, ex_destination, routing_key='')
            assert result
