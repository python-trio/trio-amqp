"""
    Amqp exchange class tests
"""

import unittest

from . import testcase
from . import testing
from .. import exceptions


class TestExchangeDeclare(testcase.RabbitTestCase):

    _multiprocess_can_split_ = True

    async def test_exchange_direct_declare(self, amqp):
        result = await self.channel.exchange_declare(
            'exchange_name', type_name='direct')
        assert result

    async def test_exchange_fanout_declare(self, amqp):
        result = await self.channel.exchange_declare(
            'exchange_name', type_name='fanout')
        assert result

    async def test_exchange_topic_declare(self, amqp):
        result = await self.channel.exchange_declare(
            'exchange_name', type_name='topic')
        assert result

    async def test_exchange_headers_declare(self, amqp):
        result = await self.channel.exchange_declare(
            'exchange_name', type_name='headers')
        assert result

    async def test_exchange_declare_wrong_types(self, amqp):
        result = await self.channel.exchange_declare(
            'exchange_name', type_name='headers',
            auto_delete=True, durable=True)
        assert result

        with pytest.raises(exceptions.ChannelClosed):
            result = await self.channel.exchange_declare(
                'exchange_name', type_name='fanout',
                auto_delete=False, durable=False)

    async def test_exchange_declare_passive(self, amqp):
        result = await self.channel.exchange_declare(
            'exchange_name', type_name='headers',
            auto_delete=True, durable=True)
        assert result
        result = await self.channel.exchange_declare(
            'exchange_name', type_name='headers',
            auto_delete=True, durable=True, passive=True)
        assert result

        result = await self.channel.exchange_declare(
            'exchange_name', type_name='headers',
            auto_delete=False, durable=False, passive=True)
        assert result


    async def test_exchange_declare_passive_does_not_exists(self, amqp):
        with pytest.raises(exceptions.ChannelClosed) as cm:
            await self.channel.exchange_declare(
                'non_existant_exchange',
                type_name='headers',
                auto_delete=False, durable=False, passive=True)
        assert cm.exception.code == 404

    async def test_exchange_declare_unknown_type(self, amqp):
        with pytest.raises(exceptions.ChannelClosed):
            await self.channel.exchange_declare(
                'non_existant_exchange',
                type_name='unknown_type',
                auto_delete=False, durable=False, passive=True)


class TestExchangeDelete(testcase.RabbitTestCase):

    async def test_delete(self, amqp):
        exchange_name = 'exchange_name'
        await self.channel.exchange_declare(exchange_name, type_name='direct')
        result = await self.channel.exchange_delete(exchange_name)
        assert result
        with pytest.raises(exceptions.ChannelClosed) as cm:
            await self.channel.exchange_declare(
                exchange_name, type_name='direct', passive=True
            )

        assert cm.exception.code == 404


    async def test_double_delete(self, amqp):
        exchange_name = 'exchange_name'
        await self.channel.exchange_declare(exchange_name, type_name='direct')
        result = await self.channel.exchange_delete(exchange_name)
        assert result
        if self.server_version() < (3, 3, 5):
            with pytest.raises(exceptions.ChannelClosed) as cm:
                await self.channel.exchange_delete(exchange_name)

            assert cm.exception.code == 404

        else:
            # weird result from rabbitmq 3.3.5
            result = await self.channel.exchange_delete(exchange_name)
            assert result

class TestExchangeBind(testcase.RabbitTestCase):

    async def test_exchange_bind(self, amqp):
        await self.channel.exchange_declare('exchange_destination', type_name='direct')
        await self.channel.exchange_declare('exchange_source', type_name='direct')

        result = await self.channel.exchange_bind(
            'exchange_destination', 'exchange_source', routing_key='')

        assert result

    async def test_inexistant_exchange_bind(self, amqp):
        with pytest.raises(exceptions.ChannelClosed) as cm:
            await self.channel.exchange_bind(
                'exchange_destination', 'exchange_source', routing_key='')

        assert cm.exception.code == 404


class TestExchangeUnbind(testcase.RabbitTestCase):


    async def test_exchange_unbind(self, amqp):
        ex_source = 'exchange_source'
        ex_destination = 'exchange_destination'
        await self.channel.exchange_declare(ex_destination, type_name='direct')
        await self.channel.exchange_declare(ex_source, type_name='direct')

        await self.channel.exchange_bind(
            ex_destination, ex_source, routing_key='')

        await self.channel.exchange_unbind(
            ex_destination, ex_source, routing_key='')

    async def test_exchange_unbind_reversed(self, amqp):
        ex_source = 'exchange_source'
        ex_destination = 'exchange_destination'
        await self.channel.exchange_declare(ex_destination, type_name='direct')
        await self.channel.exchange_declare(ex_source, type_name='direct')

        await self.channel.exchange_bind(
            ex_destination, ex_source, routing_key='')

        if self.server_version() < (3, 3, 5):
            with pytest.raises(exceptions.ChannelClosed) as cm:
                result = await self.channel.exchange_unbind(
                    ex_source, ex_destination, routing_key='')

            assert cm.exception.code == 404

        else:
            # weird result from rabbitmq 3.3.5
            result = await self.channel.exchange_unbind(ex_source, ex_destination, routing_key='')
            assert result
