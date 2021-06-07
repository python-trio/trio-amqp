import anyio
import pytest

from . import testcase
from async_amqp import exceptions


class TestClose(testcase.RabbitTestCase):
    def setUp(self):
        super().setUp()
        self.consume_future = anyio.Event()

    @pytest.mark.trio
    async def callback(self, body, envelope, properties):
        self.consume_result = (body, envelope, properties)

    @pytest.mark.trio
    async def get_callback_result(self):
        await self.consume_future.wait()
        self.consume_future = anyio.Event()
        return self.consume_result

    @pytest.mark.trio
    async def test_close(self, channel):
        assert channel.is_open
        await channel.close()
        assert not channel.is_open

    @pytest.mark.trio
    async def test_multiple_close(self, channel):
        await channel.close()
        assert not channel.is_open
        with pytest.raises(exceptions.ChannelClosed):
            await channel.close()

    @pytest.mark.trio
    async def test_cannot_publish_after_close(self, channel):
        await channel.close()
        with pytest.raises(exceptions.ChannelClosed):
            await channel.publish(b"coucou", "my_e", "")

    @pytest.mark.trio
    async def test_cannot_declare_queue_after_close(self, channel):
        await channel.close()
        with pytest.raises(exceptions.ChannelClosed):
            await channel.queue_declare("qq")

    @pytest.mark.trio
    async def test_cannot_consume_after_close(self, channel):
        await channel.queue_declare("q")
        await channel.close()
        with pytest.raises(exceptions.ChannelClosed):
            await channel.basic_consume(self.callback)
