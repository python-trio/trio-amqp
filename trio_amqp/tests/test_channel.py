"""
    Tests the "Channel" amqp class implementation
"""

import os
import unittest

from . import testcase
from . import testing
from .. import exceptions

IMPLEMENT_CHANNEL_FLOW = os.environ.get('IMPLEMENT_CHANNEL_FLOW', False)

class TestChannel(testcase.RabbitTestCase):

    _multiprocess_can_split_ = True

    async def test_open(self, amqp):
        channel = await self.amqp.channel()
        assert channel.channel_id != 0
        assert channel.is_open 

    async def test_close(self, amqp):
        channel = await self.amqp.channel()
        result = await channel.close()
        assert result is True
        assert not channel.is_open

    async def test_server_initiated_close(self, amqp):
        channel = await self.amqp.channel()
        try:
            await channel.basic_get(queue_name='non-existant')
        except exceptions.ChannelClosed as e:
            assert e.code != 404
        assert not channel.is_open
        channel = await self.amqp.channel()

    async def test_alreadyclosed_channel(self, amqp):
        channel = await self.amqp.channel()
        result = await channel.close()
        assert result is True

        with pytest.raises(exceptions.ChannelClosed):
            result = await channel.close()

    async def test_multiple_open(self, amqp):
        channel1 = await self.amqp.channel()
        channel2 = await self.amqp.channel()
        assert channel1.channel_id != channel2.channel_id

    async def test_channel_active_flow(self, amqp):
        channel = await self.amqp.channel()
        result = await channel.flow(active=True)
        assert result['active']

    @unittest.skipIf(IMPLEMENT_CHANNEL_FLOW is False, "active=false is not implemented in RabbitMQ")
    async def test_channel_inactive_flow(self, amqp):
        channel = await self.amqp.channel()
        result = await channel.flow(active=False)
        assert not result['active']
        result = await channel.flow(active=True)

    async def test_channel_active_flow_twice(self, amqp):
        channel = await self.amqp.channel()
        result = await channel.flow(active=True)
        assert result['active']
        result = await channel.flow(active=True)

    @unittest.skipIf(IMPLEMENT_CHANNEL_FLOW is False, "active=false is not implemented in RabbitMQ")
    async def test_channel_active_inactive_flow(self, amqp):
        channel = await self.amqp.channel()
        result = await channel.flow(active=True)
        assert result['active']
        result = await channel.flow(active=False)
        assert not result['active']


class TestChannelId(testcase.RabbitTestCase):

    async def test_channel_id_release_close(self, amqp):
        channels_count_start = self.amqp.channels_ids_count
        channel = await self.amqp.channel()
        assert self.amqp.channels_ids_count == channels_count_start + 1
        result = await channel.close()
        assert result == True
        assert not channel.is_open
        assert self.amqp.channels_ids_count == channels_count_start
