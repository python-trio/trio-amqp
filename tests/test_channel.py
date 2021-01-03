"""
    Tests the "Channel" amqp class implementation
"""

import os
import pytest

from . import testcase
from async_amqp import exceptions

IMPLEMENT_CHANNEL_FLOW = os.environ.get('IMPLEMENT_CHANNEL_FLOW', False)


class TestChannel(testcase.RabbitTestCase):

    _multiprocess_can_split_ = True

    @pytest.mark.trio
    async def test_open(self, amqp):
        async with amqp.new_channel() as channel:
            assert channel.channel_id != 0
            assert channel.is_open

    @pytest.mark.trio
    async def test_close(self, amqp):
        async with amqp.new_channel() as channel:
            assert channel.is_open
        assert not channel.is_open

    @pytest.mark.trio
    async def test_server_initiated_close(self, channel):
        try:
            await channel.basic_get(queue_name='non-existant')
        except exceptions.ChannelClosed as e:
            assert e.code == 404
        assert not channel.is_open

    @pytest.mark.trio
    async def test_alreadyclosed_channel(self, channel):
        result = await channel.close()
        assert result is True

        with pytest.raises(exceptions.ChannelClosed):
            result = await channel.close()

    @pytest.mark.trio
    async def test_multiple_open(self, amqp):
        async with amqp.new_channel() as channel1:
            async with amqp.new_channel() as channel2:
                assert channel1.channel_id != channel2.channel_id

    @pytest.mark.trio
    async def test_channel_active_flow(self, channel):
        result = await channel.flow(active=True)
        assert result['active']

    @pytest.mark.skipif(
        IMPLEMENT_CHANNEL_FLOW is False, reason="active=false is not implemented in RabbitMQ"
    )
    @pytest.mark.trio
    async def test_channel_inactive_flow(self, channel):
        result = await channel.flow(active=False)
        assert not result['active']
        result = await channel.flow(active=True)

    @pytest.mark.trio
    async def test_channel_active_flow_twice(self, channel):
        result = await channel.flow(active=True)
        assert result['active']
        result = await channel.flow(active=True)

    @pytest.mark.skipif(
        IMPLEMENT_CHANNEL_FLOW is False, reason="active=false is not implemented in RabbitMQ"
    )
    @pytest.mark.trio
    async def test_channel_active_inactive_flow(self, channel):
        result = await channel.flow(active=True)
        assert result['active']
        result = await channel.flow(active=False)
        assert not result['active']


class TestChannelId(testcase.RabbitTestCase):
    @pytest.mark.trio
    async def test_channel_id_release_close(self, amqp):
        channels_count_start = amqp.channels_ids_count
        async with amqp.new_channel() as channel:
            assert amqp.channels_ids_count == channels_count_start + 1
        assert not channel.is_open
        assert amqp.channels_ids_count == channels_count_start
