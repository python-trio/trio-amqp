import pytest
import anyio
from async_amqp.protocol import OPEN, CLOSED

from . import testcase


@pytest.mark.skip(reason="epoll ignores closing")
class TestConnectionLost(testcase.RabbitTestCase):

    _multiprocess_can_split_ = True

    @pytest.mark.trio
    async def test_connection_lost(self, amqp):
        async with amqp.new_channel() as channel:
            assert amqp.state == OPEN
            assert channel.is_open
            # os.close(amqp._stream.socket.fileno()) # does not work w/ epoll
            # this should have the same effect as the tcp connection being lost
            await amqp._stream.aclose()

            with anyio.fail_after(1):
                await amqp.connection_closed.wait()
            assert amqp.state == CLOSED
            assert not channel.is_open
