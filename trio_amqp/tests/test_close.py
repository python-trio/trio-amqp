import trio
import pytest

from . import testcase
from . import testing
from .. import exceptions


class TestClose(testcase.RabbitTestCase):

    def setUp(self):
        super().setUp()
        self.consume_future = trio.Event()

    async def callback(self, body, envelope, properties):
        self.consume_result = (body, envelope, properties)

    async def get_callback_result(self):
        await self.consume_future.wait()
        self.consume_future = trio.Event()
        return self.consume_result

    async def test_close(self, amqp):
        channel = await self.create_channel()
        assert channel.is_open
        await channel.close()
        assert not channel.is_open

    async def test_multiple_close(self, amqp):
        channel = await self.create_channel()
        await channel.close()
        assert not channel.is_open
        with pytest.raises(exceptions.ChannelClosed):
            await channel.close()

    async def test_cannot_publish_after_close(self, amqp):
        channel = self.channel
        await channel.close()
        with pytest.raises(exceptions.ChannelClosed):
            await self.channel.publish("coucou", "my_e", "")

    async def test_cannot_declare_queue_after_close(self, amqp):
        channel = self.channel
        await channel.close()
        with pytest.raises(exceptions.ChannelClosed):
            await self.channel.queue_declare("qq")

    async def test_cannot_consume_after_close(self, amqp):
        channel = self.channel
        await self.channel.queue_declare("q")
        await channel.close()
        with pytest.raises(exceptions.ChannelClosed):
            await channel.basic_consume(self.callback)
