import unittest

from trio_amqp.protocol import OPEN, CLOSED
from trio_amqp.exceptions import AmqpClosedConnection

from . import testcase
from . import testing


class TestClose(testcase.RabbitTestCase):

    async def test_close(self, amqp):
        assert amqp.state == OPEN
        # grab a ref here because py36 sets _stream_reader to None in
        # StreamReaderProtocol.connection_lost()
        transport = amqp._stream_reader._transport
        await amqp.close()
        assert amqp.state == CLOSED
        if hasattr(transport, 'is_closing'):
            assert transport.is_closing()
        else:
            # TODO: remove with python <3.4.4 support
            assert transport._closing
        # make sure those 2 tasks/futures are properly set as finished
        await amqp.stop_now
        await amqp.worker

    async def test_multiple_close(self, amqp):
        await amqp.close()
        assert amqp.state == CLOSED
        with pytest.raises(AmqpClosedConnection):
            await amqp.close()
